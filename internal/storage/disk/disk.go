package disk

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"sync"

	"example.com/dfs/internal/storage"
)

// Engine stores one object per file in a confined directory.
type Engine struct {
	mu   sync.Mutex
	root *os.Root
	dir  *os.File
}

var _ storage.Engine = (*Engine)(nil)

// Open creates or opens a content directory. Its owner coordinates access
// with metadata and holds the filesystem's exclusive backing-directory lock.
func Open(directory string) (*Engine, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	dir, err := root.Open(".")
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return &Engine{root: root, dir: dir}, nil
}

func validKey(key string) bool {
	if len(key) != 32 {
		return false
	}
	for _, c := range key {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Create allocates a durable empty object and returns it open for reading/writing.
func (d *Engine) Create() (string, storage.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.root == nil {
		return "", nil, os.ErrClosed
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", nil, err
	}
	key := hex.EncodeToString(random[:])
	file, err := d.root.OpenFile(key, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return "", nil, err
	}
	if err := errors.Join(file.Sync(), d.dir.Sync()); err != nil {
		return "", nil, errors.Join(err, file.Close(), d.root.Remove(key))
	}
	return key, &diskFile{File: file, access: storage.ReadWrite}, nil
}

// Open opens an existing object with the requested access; it never creates.
func (d *Engine) Open(key string, access storage.Access) (storage.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.root == nil {
		return nil, os.ErrClosed
	}
	if !validKey(key) || access > storage.ReadWrite {
		return nil, fs.ErrInvalid
	}
	flags := os.O_RDONLY
	switch access {
	case storage.WriteOnly:
		flags = os.O_WRONLY
	case storage.ReadWrite:
		flags = os.O_RDWR
	}
	info, err := d.root.Lstat(key)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fs.ErrInvalid
	}
	file, err := d.root.OpenFile(key, flags, 0)
	if err != nil {
		return nil, err
	}
	return &diskFile{File: file, access: access}, nil
}

// Remove durably deletes an object. Repeated deletion succeeds.
func (d *Engine) Remove(key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.root == nil {
		return os.ErrClosed
	}
	if !validKey(key) {
		return fs.ErrInvalid
	}
	if err := d.root.Remove(key); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return d.dir.Sync()
}

// Keys lists objects for startup recovery; foreign files are rejected.
func (d *Engine) Keys() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.root == nil {
		return nil, os.ErrClosed
	}
	dir, err := d.root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, readErr := dir.ReadDir(-1)
	if err := errors.Join(readErr, dir.Close()); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !validKey(entry.Name()) || !entry.Type().IsRegular() {
			return nil, fs.ErrInvalid
		}
		keys = append(keys, entry.Name())
	}
	return keys, nil
}

// Sync persists changes to the object directory.
func (d *Engine) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.root == nil {
		return os.ErrClosed
	}
	return d.dir.Sync()
}

// Close releases the directory; already open objects must be closed by callers.
func (d *Engine) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.root == nil {
		return nil
	}
	err := errors.Join(d.dir.Close(), d.root.Close())
	d.root, d.dir = nil, nil
	return err
}

type diskFile struct {
	*os.File
	access storage.Access
}

func (f *diskFile) ReadAt(p []byte, off int64) (int, error) {
	if f.access == storage.WriteOnly {
		return 0, fs.ErrPermission
	}
	if off < 0 {
		return 0, fs.ErrInvalid
	}
	return f.File.ReadAt(p, off)
}

func (f *diskFile) WriteAt(p []byte, off int64) (int, error) {
	if f.access == storage.ReadOnly {
		return 0, fs.ErrPermission
	}
	if off < 0 {
		return 0, fs.ErrInvalid
	}
	return f.File.WriteAt(p, off)
}

func (f *diskFile) Truncate(size int64) error {
	if f.access == storage.ReadOnly {
		return fs.ErrPermission
	}
	if size < 0 {
		return fs.ErrInvalid
	}
	return f.File.Truncate(size)
}

func (f *diskFile) Size() (uint64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}
