package bolt

import (
	"errors"
	"io/fs"
	"os"
	"time"

	"example.com/dfs/internal/metadata"

	bbolt "go.etcd.io/bbolt"
)

// Create atomically allocates an inode and links it into its parent directory.
// File content must already exist durably before its key is passed here.
func (b *Engine) Create(parent uint64, name string, mode os.FileMode, uid, gid uint32, object string) (inode metadata.Inode, err error) {
	if err := validName(name); err != nil {
		return metadata.Inode{}, err
	}
	if mode.Type() != 0 && mode.Type() != os.ModeDir || mode.IsDir() && object != "" || !mode.IsDir() && object == "" {
		return metadata.Inode{}, fs.ErrInvalid
	}
	err = b.db.Update(func(tx *bbolt.Tx) error {
		if _, err := directory(tx, parent); err != nil {
			return err
		}
		entries := tx.Bucket(entriesBucket)
		key := entryKey(parent, name)
		if entries.Get(key) != nil {
			return fs.ErrExist
		}
		id, err := tx.Bucket(inodesBucket).NextSequence()
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		inode = metadata.Inode{ID: id, Parent: parent, Object: object, Mode: mode.Type() | mode.Perm(), Nlink: 1,
			UID: uid, GID: gid, Atime: now, Mtime: now, Ctime: now, Btime: now}
		links := 0
		if mode.IsDir() {
			inode.Nlink, links = 2, 1
		}
		if err := put(tx, inode); err != nil {
			return err
		}
		if err := entries.Put(key, idKey(id)); err != nil {
			return err
		}
		return touch(tx, parent, links)
	})
	return inode, adapterError(err)
}

// Rename atomically moves an entry and returns any replaced inode as a tombstone.
func (b *Engine) Rename(oldParent uint64, oldName string, newParent uint64, newName string) (replaced *metadata.Inode, err error) {
	if err := validName(oldName); err != nil {
		return nil, err
	}
	if err := validName(newName); err != nil {
		return nil, err
	}
	err = b.db.Update(func(tx *bbolt.Tx) error {
		if _, err := directory(tx, oldParent); err != nil {
			return err
		}
		if _, err := directory(tx, newParent); err != nil {
			return err
		}
		source, err := lookup(tx, oldParent, oldName)
		if err != nil {
			return err
		}
		if oldParent == newParent && oldName == newName {
			return nil
		}
		if source.Mode.IsDir() {
			// A directory cannot be moved into itself or any of its descendants.
			for parent := newParent; ; {
				if parent == source.ID {
					return fs.ErrInvalid
				}
				if parent == metadata.Root {
					break
				}
				inode, err := directory(tx, parent)
				if err != nil {
					return err
				}
				parent = inode.Parent
			}
		}
		target, err := lookup(tx, newParent, newName)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err == nil {
			if source.Mode.IsDir() && !target.Mode.IsDir() {
				return metadata.ErrNotDirectory
			}
			if !source.Mode.IsDir() && target.Mode.IsDir() {
				return metadata.ErrIsDirectory
			}
			if target.Mode.IsDir() && !empty(tx, target.ID) {
				return metadata.ErrNotEmpty
			}
			target.Nlink, target.Ctime = 0, time.Now().UTC()
			if err := put(tx, target); err != nil {
				return err
			}
			replaced = &target
			if target.Mode.IsDir() {
				if err := touch(tx, newParent, -1); err != nil {
					return err
				}
			}
		}
		entries := tx.Bucket(entriesBucket)
		if err := entries.Delete(entryKey(oldParent, oldName)); err != nil {
			return err
		}
		if err := entries.Put(entryKey(newParent, newName), idKey(source.ID)); err != nil {
			return err
		}
		source.Parent, source.Ctime = newParent, time.Now().UTC()
		if err := put(tx, source); err != nil {
			return err
		}
		links := 0
		if source.Mode.IsDir() && oldParent != newParent {
			links = 1
		}
		if err := touch(tx, oldParent, -links); err != nil {
			return err
		}
		return touch(tx, newParent, links)
	})
	if err != nil {
		return nil, adapterError(err)
	}
	return replaced, nil
}

// Remove unlinks a file or empty directory and retains its inode as a tombstone.
func (b *Engine) Remove(parent uint64, name string, isDirectory bool) (inode metadata.Inode, err error) {
	if err := validName(name); err != nil {
		return metadata.Inode{}, err
	}
	err = b.db.Update(func(tx *bbolt.Tx) error {
		if _, err := directory(tx, parent); err != nil {
			return err
		}
		inode, err = lookup(tx, parent, name)
		if err != nil {
			return err
		}
		if isDirectory && !inode.Mode.IsDir() {
			return metadata.ErrNotDirectory
		}
		if !isDirectory && inode.Mode.IsDir() {
			return metadata.ErrIsDirectory
		}
		links := 0
		if isDirectory {
			if !empty(tx, inode.ID) {
				return metadata.ErrNotEmpty
			}
			links = -1
		}
		if err := tx.Bucket(entriesBucket).Delete(entryKey(parent, name)); err != nil {
			return err
		}
		inode.Nlink, inode.Ctime = 0, time.Now().UTC()
		if err := put(tx, inode); err != nil {
			return err
		}
		return touch(tx, parent, links)
	})
	return inode, adapterError(err)
}

// Delete reclaims a tombstone after its final open handle has closed.
func (b *Engine) Delete(id uint64) error {
	return adapterError(b.db.Update(func(tx *bbolt.Tx) error {
		inode, err := get(tx, id)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if inode.Nlink != 0 || id == metadata.Root {
			return metadata.ErrBusy
		}
		return tx.Bucket(inodesBucket).Delete(idKey(id))
	}))
}
