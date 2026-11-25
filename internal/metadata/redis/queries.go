package redis

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"example.com/dfs/internal/metadata"
)

// Get returns an inode, including a tombstone retained for open handles.
func (e *Engine) Get(id uint64) (inode metadata.Inode, err error) {
	err = e.run(false, func(tx *transaction) error { inode, err = get(tx, id); return err })
	return inode, err
}

// Lookup resolves a name within a directory.
func (e *Engine) Lookup(parent uint64, name string) (inode metadata.Inode, err error) {
	if err := validName(name); err != nil {
		return metadata.Inode{}, err
	}
	err = e.run(false, func(tx *transaction) error {
		if _, err := directory(tx, parent); err != nil {
			return err
		}
		inode, err = lookup(tx, parent, name)
		return err
	})
	return inode, err
}

// ReadDir returns one revision-consistent, sorted directory snapshot.
func (e *Engine) ReadDir(id uint64) (entries []metadata.Entry, err error) {
	err = e.run(false, func(tx *transaction) error {
		entries = nil
		if _, err := directory(tx, id); err != nil {
			return err
		}
		children, err := tx.entries(id)
		if err != nil {
			return err
		}
		for name, child := range children {
			inode, err := get(tx, child)
			if err != nil {
				return err
			}
			entries = append(entries, metadata.Entry{Name: name, Inode: inode})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		return nil
	})
	return entries, err
}

// SetAttributes atomically updates attributes without changing inode identity.
func (e *Engine) SetAttributes(id uint64, attrs metadata.Attributes) (inode metadata.Inode, err error) {
	err = e.run(true, func(tx *transaction) error {
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
	return inode, err
}

// List returns all inode records for startup recovery, including tombstones.
func (e *Engine) List() (inodes []metadata.Inode, err error) {
	err = e.run(false, func(tx *transaction) error {
		inodes = nil
		records, err := tx.client.HGetAll(tx.ctx, e.inodesKey()).Result()
		if err != nil {
			return err
		}
		for key, value := range records {
			id, err := strconv.ParseUint(key, 10, 64)
			if err != nil {
				return err
			}
			var inode metadata.Inode
			if err := json.Unmarshal([]byte(value), &inode); err != nil {
				return err
			}
			if inode.ID != id {
				return fmt.Errorf("invalid inode identity %d", id)
			}
			inodes = append(inodes, inode)
		}
		sort.Slice(inodes, func(i, j int) bool { return inodes[i].ID < inodes[j].ID })
		return nil
	})
	return inodes, err
}
