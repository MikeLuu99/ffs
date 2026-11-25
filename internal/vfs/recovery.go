package vfs

import (
	"errors"
	"fmt"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"
)

// recover checks live content before reclaiming objects left by interrupted
// creates/unlinks. Content is synced before metadata updates; a crash in between
// may leave a newer size in the content store, which is reconciled here.
func (f *FS) recover() error {
	inodes, err := f.meta.List()
	if err != nil {
		return err
	}
	keys, err := f.store.Keys()
	if err != nil {
		return fmt.Errorf("list content objects: %w", err)
	}
	live := make(map[string]bool)
	for _, inode := range inodes {
		if inode.Nlink == 0 || inode.Mode.IsDir() {
			continue
		}
		if live[inode.Object] {
			return fmt.Errorf("content object shared by multiple inodes: %s", inode.Object)
		}
		file, err := f.store.Open(inode.Object, storage.ReadOnly)
		if err != nil {
			return fmt.Errorf("open content for inode %d: %w", inode.ID, err)
		}
		size, sizeErr := file.Size()
		if err := errors.Join(sizeErr, file.Close()); err != nil {
			return err
		}
		live[inode.Object] = true
		if size != inode.Size {
			if _, err := f.meta.SetAttributes(inode.ID, metadata.Attributes{Size: &size}); err != nil {
				return err
			}
		}
	}
	for _, inode := range inodes {
		if inode.Nlink == 0 {
			if live[inode.Object] {
				return fmt.Errorf("tombstone references live content: %s", inode.Object)
			}
			if err := f.reclaim(inode); err != nil {
				return err
			}
		}
	}
	for _, key := range keys {
		if !live[key] {
			if err := f.store.Remove(key); err != nil {
				return err
			}
		}
	}
	return nil
}
