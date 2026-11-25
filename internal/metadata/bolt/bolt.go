package bolt

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"strings"
	"time"

	"example.com/dfs/internal/metadata"

	bbolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

var (
	inodesBucket  = []byte("inodes")
	entriesBucket = []byte("entries")
	formatBucket  = []byte("format")
)

// Engine stores namespace records in an embedded transactional database.
type Engine struct{ db *bbolt.DB }

var _ metadata.Engine = (*Engine)(nil)

// Open opens or initializes a metadata database. Default bbolt commits sync
// to disk; durability must not be disabled while using a separate content store.
func Open(filename string) (*Engine, error) {
	db, err := bbolt.Open(filename, 0o600, &bbolt.Options{Timeout: 100 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		if format := tx.Bucket(formatBucket); format != nil {
			if string(format.Get([]byte("version"))) != "1" || tx.Bucket(inodesBucket) == nil || tx.Bucket(entriesBucket) == nil {
				return fmt.Errorf("unsupported or corrupt metadata format")
			}
			_, err := get(tx, metadata.Root)
			return err
		}
		for _, name := range [][]byte{formatBucket, inodesBucket, entriesBucket} {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		if err := tx.Bucket(formatBucket).Put([]byte("version"), []byte("1")); err != nil {
			return err
		}
		if err := tx.Bucket(inodesBucket).SetSequence(metadata.Root); err != nil {
			return err
		}
		now := time.Now().UTC()
		return put(tx, metadata.Inode{ID: metadata.Root, Parent: metadata.Root, Mode: os.ModeDir | 0o755, Nlink: 2,
			UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Atime: now, Mtime: now, Ctime: now, Btime: now})
	})
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &Engine{db: db}, nil
}

func idKey(id uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, id)
}

func entryKey(parent uint64, name string) []byte {
	return append(idKey(parent), name...)
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return fs.ErrInvalid
	}
	if len(name) > 255 {
		return metadata.ErrNameTooLong
	}
	return nil
}

func get(tx *bbolt.Tx, id uint64) (metadata.Inode, error) {
	value := tx.Bucket(inodesBucket).Get(idKey(id))
	if value == nil {
		return metadata.Inode{}, fs.ErrNotExist
	}
	var inode metadata.Inode
	if err := json.Unmarshal(value, &inode); err != nil {
		return metadata.Inode{}, fmt.Errorf("decode inode %d: %w", id, err)
	}
	if inode.ID != id {
		return metadata.Inode{}, fmt.Errorf("invalid inode identity %d", id)
	}
	return inode, nil
}

func put(tx *bbolt.Tx, inode metadata.Inode) error {
	value, err := json.Marshal(inode)
	if err != nil {
		return err
	}
	return tx.Bucket(inodesBucket).Put(idKey(inode.ID), value)
}

func directory(tx *bbolt.Tx, id uint64) (metadata.Inode, error) {
	inode, err := get(tx, id)
	if err != nil {
		return metadata.Inode{}, err
	}
	if inode.Nlink == 0 {
		return metadata.Inode{}, fs.ErrNotExist
	}
	if !inode.Mode.IsDir() {
		return metadata.Inode{}, metadata.ErrNotDirectory
	}
	return inode, nil
}

func lookup(tx *bbolt.Tx, parent uint64, name string) (metadata.Inode, error) {
	value := tx.Bucket(entriesBucket).Get(entryKey(parent, name))
	if value == nil {
		return metadata.Inode{}, fs.ErrNotExist
	}
	if len(value) != 8 {
		return metadata.Inode{}, fmt.Errorf("invalid directory entry")
	}
	return get(tx, binary.BigEndian.Uint64(value))
}

func empty(tx *bbolt.Tx, id uint64) bool {
	prefix := idKey(id)
	key, _ := tx.Bucket(entriesBucket).Cursor().Seek(prefix)
	return !bytes.HasPrefix(key, prefix)
}

func touch(tx *bbolt.Tx, id uint64, links int) error {
	inode, err := get(tx, id)
	if err != nil {
		return err
	}
	inode.Nlink = uint32(int64(inode.Nlink) + int64(links))
	inode.Mtime, inode.Ctime = time.Now().UTC(), time.Now().UTC()
	return put(tx, inode)
}

// Get returns attributes even for an inode retained after unlink.
func (b *Engine) Get(id uint64) (inode metadata.Inode, err error) {
	err = b.db.View(func(tx *bbolt.Tx) error {
		inode, err = get(tx, id)
		return err
	})
	return inode, adapterError(err)
}

// Lookup resolves a directory entry.
func (b *Engine) Lookup(parent uint64, name string) (inode metadata.Inode, err error) {
	if err := validName(name); err != nil {
		return metadata.Inode{}, err
	}
	err = b.db.View(func(tx *bbolt.Tx) error {
		if _, err := directory(tx, parent); err != nil {
			return err
		}
		inode, err = lookup(tx, parent, name)
		return err
	})
	return inode, adapterError(err)
}

// ReadDir returns a sorted snapshot that does not include dot entries.
func (b *Engine) ReadDir(id uint64) (entries []metadata.Entry, err error) {
	err = b.db.View(func(tx *bbolt.Tx) error {
		if _, err := directory(tx, id); err != nil {
			return err
		}
		prefix := idKey(id)
		cursor := tx.Bucket(entriesBucket).Cursor()
		for key, value := cursor.Seek(prefix); bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			if len(value) != 8 {
				return fmt.Errorf("invalid directory entry")
			}
			inode, err := get(tx, binary.BigEndian.Uint64(value))
			if err != nil {
				return err
			}
			entries = append(entries, metadata.Entry{Name: string(key[8:]), Inode: inode})
		}
		return nil
	})
	return entries, adapterError(err)
}

// SetAttributes atomically changes file attributes without changing identity.
func (b *Engine) SetAttributes(id uint64, attrs metadata.Attributes) (inode metadata.Inode, err error) {
	err = b.db.Update(func(tx *bbolt.Tx) error {
		inode, err = get(tx, id)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if attrs.Size != nil {
			if inode.Mode.IsDir() {
				return metadata.ErrIsDirectory
			}
			if *attrs.Size > math.MaxInt64 {
				return metadata.ErrTooLarge
			}
			inode.Size, inode.Mtime = *attrs.Size, now
		}
		if attrs.Mode != nil {
			inode.Mode = inode.Mode.Type() | attrs.Mode.Perm()
		}
		if attrs.Atime != nil {
			inode.Atime = attrs.Atime.UTC()
		}
		if attrs.Mtime != nil {
			inode.Mtime = attrs.Mtime.UTC()
		}
		inode.Ctime = now
		return put(tx, inode)
	})
	return inode, adapterError(err)
}

// List returns all inode records, including tombstones, for startup recovery.
func (b *Engine) List() (inodes []metadata.Inode, err error) {
	err = b.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(inodesBucket).ForEach(func(key, _ []byte) error {
			if len(key) != 8 {
				return fmt.Errorf("invalid inode key")
			}
			inode, err := get(tx, binary.BigEndian.Uint64(key))
			if err == nil {
				inodes = append(inodes, inode)
			}
			return err
		})
	})
	return inodes, adapterError(err)
}

// Sync is a barrier: every completed write transaction has already been synced.
func (b *Engine) Sync() error { return adapterError(b.db.View(func(*bbolt.Tx) error { return nil })) }

// Close releases the database and its exclusive file lock.
func (b *Engine) Close() error { return b.db.Close() }

// adapterError keeps database lifecycle errors out of the engine contract.
func adapterError(err error) error {
	if errors.Is(err, bolterrors.ErrDatabaseNotOpen) {
		return fs.ErrClosed
	}
	return err
}
