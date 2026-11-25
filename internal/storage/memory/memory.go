// Package memory provides volatile content storage for tests and experiments.
// Sync is a no-op: objects live only for the lifetime of this adapter instance.
package memory

import (
	"fmt"
	"io"
	"io/fs"
	"math"
	"sort"
	"sync"

	"example.com/dfs/internal/storage"
)

const blockSize int64 = 4096

type object struct {
	size   int64
	blocks map[int64][]byte
}

// Engine keeps sparse content objects in memory. Removing a key leaves existing
// handles usable. Close stops new engine operations; callers close their handles.
type Engine struct {
	mu      sync.Mutex
	objects map[string]*object
	next    uint64
	closed  bool
}

var _ storage.Engine = (*Engine)(nil)

// New creates a fresh, empty content namespace without disk or OS dependencies.
func New() *Engine { return &Engine{objects: make(map[string]*object)} }

func (e *Engine) Create() (string, storage.File, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return "", nil, fs.ErrClosed
	}
	e.next++
	key := fmt.Sprintf("mem-%d", e.next)
	obj := &object{blocks: make(map[int64][]byte)}
	e.objects[key] = obj
	return key, &file{engine: e, object: obj, access: storage.ReadWrite}, nil
}

func (e *Engine) Open(key string, access storage.Access) (storage.File, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fs.ErrClosed
	}
	if access > storage.ReadWrite {
		return nil, fs.ErrInvalid
	}
	obj := e.objects[key]
	if obj == nil {
		return nil, fs.ErrNotExist
	}
	return &file{engine: e, object: obj, access: access}, nil
}

func (e *Engine) Remove(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fs.ErrClosed
	}
	delete(e.objects, key)
	return nil
}

func (e *Engine) Keys() ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fs.ErrClosed
	}
	keys := make([]string, 0, len(e.objects))
	for key := range e.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (e *Engine) Sync() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fs.ErrClosed
	}
	return nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed, e.objects = true, nil
	return nil
}

type file struct {
	engine *Engine
	object *object
	access storage.Access
	closed bool
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	if f.closed {
		return 0, fs.ErrClosed
	}
	if f.access == storage.WriteOnly {
		return 0, fs.ErrPermission
	}
	if off < 0 {
		return 0, fs.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= f.object.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), f.object.size-off))
	clear(p[:n])
	for done := 0; done < n; {
		index, within := off/blockSize, off%blockSize
		count := min(n-done, int(blockSize-within))
		if block := f.object.blocks[index]; block != nil {
			copy(p[done:done+count], block[within:within+int64(count)])
		}
		done, off = done+count, off+int64(count)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *file) WriteAt(p []byte, off int64) (int, error) {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	if f.closed {
		return 0, fs.ErrClosed
	}
	if f.access == storage.ReadOnly {
		return 0, fs.ErrPermission
	}
	if off < 0 || int64(len(p)) > math.MaxInt64-off {
		return 0, fs.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}
	for done := 0; done < len(p); {
		index, within := off/blockSize, off%blockSize
		count := min(len(p)-done, int(blockSize-within))
		block := f.object.blocks[index]
		if block == nil {
			block = make([]byte, blockSize)
			f.object.blocks[index] = block
		}
		copy(block[within:within+int64(count)], p[done:done+count])
		done, off = done+count, off+int64(count)
	}
	f.object.size = max(f.object.size, off)
	return len(p), nil
}

func (f *file) Size() (uint64, error) {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	if f.closed {
		return 0, fs.ErrClosed
	}
	return uint64(f.object.size), nil
}

func (f *file) Truncate(size int64) error {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	if f.closed {
		return fs.ErrClosed
	}
	if f.access == storage.ReadOnly {
		return fs.ErrPermission
	}
	if size < 0 {
		return fs.ErrInvalid
	}
	if size < f.object.size {
		for index, block := range f.object.blocks {
			if index > size/blockSize || index == size/blockSize && size%blockSize == 0 {
				delete(f.object.blocks, index)
			} else if index == size/blockSize {
				clear(block[size%blockSize:])
			}
		}
	}
	f.object.size = size
	return nil
}

func (f *file) Sync() error {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	if f.closed {
		return fs.ErrClosed
	}
	return nil
}

func (f *file) Close() error {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true
	return nil
}
