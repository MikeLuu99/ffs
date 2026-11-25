// Package vfs coordinates filesystem operations across metadata and content
// engines and exposes them through FUSE callbacks.
package vfs

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"os"
	"sync"
	"syscall"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type handle struct {
	inode     uint64
	file      storage.File
	flags     int
	dirty     bool
	entries   []fuseutil.Dirent
	directory bool
}

// FS serializes client operations while coordinating the two durable engines.
type FS struct {
	fuseutil.NotImplementedFileSystem
	mu         sync.Mutex
	meta       metadata.Engine
	store      storage.Engine
	release    func() error
	handles    map[fuseops.HandleID]*handle
	nextHandle fuseops.HandleID
}

var _ fuseutil.FileSystem = (*FS)(nil)

// Close releases handles and engines, then its optional lease. It is idempotent.
func (f *FS) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.meta == nil {
		return nil
	}
	var err error
	for _, h := range f.handles {
		if h.file != nil {
			err = errors.Join(err, f.flushHandle(h), h.file.Close())
		}
	}
	f.handles = nil
	inodes, listErr := f.meta.List()
	err = errors.Join(err, listErr)
	if listErr == nil {
		for _, inode := range inodes {
			if inode.Nlink == 0 {
				err = errors.Join(err, f.reclaim(inode))
			}
		}
	}
	err = errors.Join(err, f.meta.Sync(), f.store.Sync(), f.store.Close(), f.meta.Close())
	if f.release != nil {
		err = errors.Join(err, f.release())
	}
	f.meta, f.store, f.release = nil, nil, nil
	return err
}

// Destroy releases resources when the FUSE connection ends.
func (f *FS) Destroy() {
	if err := f.Close(); err != nil {
		log.Printf("close filesystem: %v", err)
	}
}

func systemError(err error) error {
	if err == nil {
		return nil
	}
	var number syscall.Errno
	if errors.As(err, &number) {
		return number
	}
	if errors.Is(err, os.ErrClosed) {
		return syscall.EBADF
	}
	if errors.Is(err, fs.ErrInvalid) {
		return syscall.EINVAL
	}
	for _, mapping := range []struct {
		err  error
		code syscall.Errno
	}{
		{fs.ErrNotExist, syscall.ENOENT},
		{fs.ErrExist, syscall.EEXIST},
		{fs.ErrPermission, syscall.EACCES},
		{errors.ErrUnsupported, syscall.ENOTSUP},
		{metadata.ErrNotDirectory, syscall.ENOTDIR},
		{metadata.ErrIsDirectory, syscall.EISDIR},
		{metadata.ErrNotEmpty, syscall.ENOTEMPTY},
		{metadata.ErrBusy, syscall.EBUSY},
		{metadata.ErrNameTooLong, syscall.ENAMETOOLONG},
		{metadata.ErrTooLarge, syscall.EFBIG},
	} {
		if errors.Is(err, mapping.err) {
			return mapping.code
		}
	}
	return syscall.EIO
}

func (f *FS) get(id fuseops.InodeID) (metadata.Inode, error) {
	if f.meta == nil {
		return metadata.Inode{}, syscall.EBADF
	}
	inode, err := f.meta.Get(uint64(id))
	return inode, systemError(err)
}

func inodeAttributes(inode metadata.Inode) fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Size: inode.Size, Mode: inode.Mode, Nlink: inode.Nlink,
		Uid: inode.UID, Gid: inode.GID, Atime: inode.Atime,
		Mtime: inode.Mtime, Ctime: inode.Ctime, Crtime: inode.Btime,
	}
}

func childEntry(inode metadata.Inode) fuseops.ChildInodeEntry {
	return fuseops.ChildInodeEntry{Child: fuseops.InodeID(inode.ID), Attributes: inodeAttributes(inode)}
}

// LookUpInode resolves a name entirely through the metadata engine.
func (f *FS) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.meta == nil {
		return syscall.EBADF
	}
	inode, err := f.meta.Lookup(uint64(op.Parent), op.Name)
	if err != nil {
		return systemError(err)
	}
	op.Entry = childEntry(inode)
	return nil
}

// GetInodeAttributes reports metadata, including for unlinked open files.
func (f *FS) GetInodeAttributes(_ context.Context, op *fuseops.GetInodeAttributesOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	inode, err := f.get(op.Inode)
	if err != nil {
		return err
	}
	op.Attributes = inodeAttributes(inode)
	return nil
}

// StatFS reports capacity when the selected content adapter supports it.
func (f *FS) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.meta == nil {
		return syscall.EBADF
	}
	reporter, ok := f.store.(storage.CapacityReporter)
	if !ok {
		return syscall.ENOSYS
	}
	stats, err := reporter.Capacity()
	if errors.Is(err, errors.ErrUnsupported) {
		return syscall.ENOSYS
	}
	if err != nil {
		return systemError(err)
	}
	op.BlockSize, op.IoSize = stats.BlockSize, stats.IOSize
	op.Blocks, op.BlocksFree, op.BlocksAvailable = stats.Blocks, stats.BlocksFree, stats.BlocksAvailable
	op.Inodes, op.InodesFree = stats.Inodes, stats.InodesFree
	return nil
}

// MkDir creates a metadata directory; it does not create a content object.
func (f *FS) MkDir(_ context.Context, op *fuseops.MkDirOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	parent, err := f.get(op.Parent)
	if err != nil {
		return err
	}
	inode, err := f.meta.Create(uint64(op.Parent), op.Name, os.ModeDir|op.Mode.Perm(), op.OpContext.Uid, parent.GID, "")
	if err != nil {
		return systemError(err)
	}
	op.Entry = childEntry(inode)
	return nil
}

// Rename commits a namespace transaction without moving any file content.
func (f *FS) Rename(_ context.Context, op *fuseops.RenameOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.meta == nil {
		return syscall.EBADF
	}
	replaced, err := f.meta.Rename(uint64(op.OldParent), op.OldName, uint64(op.NewParent), op.NewName)
	if err != nil {
		return systemError(err)
	}
	if replaced != nil {
		f.reclaimLater(*replaced)
	}
	return nil
}

// reclaim deletes content before forgetting its tombstone, so recovery can retry.
func (f *FS) reclaim(inode metadata.Inode) error {
	if inode.Nlink != 0 {
		return nil
	}
	for _, h := range f.handles {
		if h.inode == inode.ID {
			return nil
		}
	}
	if inode.Object != "" {
		if err := f.store.Remove(inode.Object); err != nil {
			return err
		}
	}
	return f.meta.Delete(inode.ID)
}

func (f *FS) reclaimLater(inode metadata.Inode) {
	if err := f.reclaim(inode); err != nil {
		// The namespace transaction already committed. Retain its tombstone so
		// startup recovery can retry cleanup without undoing a successful unlink.
		log.Printf("defer cleanup of inode %d: %v", inode.ID, err)
	}
}

func (f *FS) remove(parent fuseops.InodeID, name string, directory bool) error {
	if f.meta == nil {
		return syscall.EBADF
	}
	inode, err := f.meta.Remove(uint64(parent), name, directory)
	if err != nil {
		return systemError(err)
	}
	f.reclaimLater(inode)
	return nil
}

// Unlink removes a name while retaining content needed by open handles.
func (f *FS) Unlink(_ context.Context, op *fuseops.UnlinkOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remove(op.Parent, op.Name, false)
}

// RmDir removes an empty metadata directory.
func (f *FS) RmDir(_ context.Context, op *fuseops.RmDirOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remove(op.Parent, op.Name, true)
}

// ForgetInode accepts kernel cache-reference drops; inode IDs are never reused.
func (*FS) ForgetInode(context.Context, *fuseops.ForgetInodeOp) error { return nil }

// BatchForget accepts batched kernel cache-reference drops.
func (*FS) BatchForget(context.Context, *fuseops.BatchForgetOp) error { return nil }
