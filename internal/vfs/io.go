package vfs

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"syscall"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"

	"github.com/jacobsa/fuse/fuseops"
)

func (f *FS) addHandle(h *handle) fuseops.HandleID {
	id := f.nextHandle
	f.nextHandle++
	f.handles[id] = h
	return id
}

func (f *FS) fileHandle(id fuseops.HandleID, inode fuseops.InodeID) (*handle, error) {
	h := f.handles[id]
	if h == nil || h.file == nil || h.inode != uint64(inode) || h.directory {
		return nil, syscall.EBADF
	}
	return h, nil
}

// CreateFile creates durable content before linking it into the namespace.
func (f *FS) CreateFile(_ context.Context, op *fuseops.CreateFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	parent, err := f.get(op.Parent)
	if err != nil {
		return err
	}
	flags := int(op.OpenFlags)
	if flags&3 == 3 {
		return syscall.EINVAL
	}
	key, file, err := f.store.Create()
	if err != nil {
		return systemError(err)
	}
	inode, err := f.meta.Create(uint64(op.Parent), op.Name, op.Mode.Perm(), op.OpContext.Uid, parent.GID, key)
	if err != nil {
		return systemError(errors.Join(err, file.Close(), f.store.Remove(key)))
	}
	op.Entry = childEntry(inode)
	op.Handle = f.addHandle(&handle{file: file, inode: inode.ID, flags: flags})
	return nil
}

// OpenFile opens content by its metadata key, independently of its current name.
func (f *FS) OpenFile(_ context.Context, op *fuseops.OpenFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	inode, err := f.get(op.Inode)
	if err != nil {
		return err
	}
	if inode.Nlink == 0 {
		return syscall.ENOENT
	}
	if inode.Mode.IsDir() {
		return syscall.EISDIR
	}
	flags := int(op.OpenFlags)
	if flags&3 == 3 {
		return syscall.EINVAL
	}
	if flags&os.O_TRUNC != 0 && flags&3 == os.O_RDONLY {
		return syscall.EACCES
	}
	access := storage.ReadOnly
	if flags&os.O_RDWR != 0 {
		access = storage.ReadWrite
	} else if flags&os.O_WRONLY != 0 {
		access = storage.WriteOnly
	}
	file, err := f.store.Open(inode.Object, access)
	if err != nil {
		return systemError(err)
	}
	if flags&os.O_TRUNC != 0 {
		if err := file.Truncate(0); err != nil {
			return systemError(errors.Join(err, file.Close()))
		}
		if err := f.commitContent(inode.ID, file, metadata.Attributes{}); err != nil {
			return systemError(errors.Join(err, file.Close()))
		}
	}
	op.Handle = f.addHandle(&handle{file: file, inode: inode.ID, flags: flags})
	return nil
}

// ReadFile reads from an open content object at the supplied offset.
func (f *FS) ReadFile(_ context.Context, op *fuseops.ReadFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, err := f.fileHandle(op.Handle, op.Inode)
	if err != nil {
		return err
	}
	if h.flags&3 == os.O_WRONLY {
		return syscall.EBADF
	}
	if op.Offset < 0 || op.Size < 0 || op.Size > 1<<20 {
		return syscall.EINVAL
	}
	dst := op.Dst
	if int64(len(dst)) < op.Size {
		dst = make([]byte, op.Size)
		op.Data = [][]byte{dst}
	}
	op.BytesRead, err = h.file.ReadAt(dst[:op.Size], op.Offset)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return systemError(err)
}

// commitContent makes bytes durable before committing size/timestamp metadata.
func (f *FS) commitContent(id uint64, file storage.File, attrs metadata.Attributes) error {
	if err := file.Sync(); err != nil {
		return err
	}
	size, err := file.Size()
	if err != nil {
		return err
	}
	attrs.Size = &size
	_, err = f.meta.SetAttributes(id, attrs)
	return err
}

func (f *FS) flushHandle(h *handle) error {
	if !h.dirty {
		return h.file.Sync()
	}
	if err := f.commitContent(h.inode, h.file, metadata.Attributes{}); err != nil {
		return err
	}
	h.dirty = false
	return nil
}

// WriteFile serializes append and commits content before updating metadata.
func (f *FS) WriteFile(_ context.Context, op *fuseops.WriteFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, err := f.fileHandle(op.Handle, op.Inode)
	if err != nil {
		return err
	}
	if h.flags&3 == os.O_RDONLY {
		return syscall.EBADF
	}
	if op.Offset < 0 {
		return syscall.EINVAL
	}
	if len(op.Data) == 0 {
		return nil
	}
	offset := op.Offset
	if h.flags&os.O_APPEND != 0 {
		size, err := h.file.Size()
		if err != nil {
			return systemError(err)
		}
		offset = int64(size)
	}
	if int64(len(op.Data)) > math.MaxInt64-offset {
		return syscall.EFBIG
	}
	n, writeErr := h.file.WriteAt(op.Data, offset)
	if writeErr == nil && n != len(op.Data) {
		writeErr = io.ErrShortWrite
	}
	if n > 0 {
		h.dirty = true
		writeErr = errors.Join(writeErr, f.flushHandle(h))
	}
	return systemError(writeErr)
}

// SyncFile is a durability barrier for content and committed metadata. Directory
// handles sync namespace metadata and the object directory without a data file.
func (f *FS) SyncFile(_ context.Context, op *fuseops.SyncFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.handles[op.Handle]
	if h == nil || h.inode != uint64(op.Inode) {
		return syscall.EBADF
	}
	if h.file != nil {
		if err := f.flushHandle(h); err != nil {
			return systemError(err)
		}
	}
	return systemError(errors.Join(f.meta.Sync(), f.store.Sync()))
}

// FlushFile syncs on close while retaining the handle until release.
func (f *FS) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) error {
	return f.SyncFile(ctx, &fuseops.SyncFileOp{Inode: op.Inode, Handle: op.Handle})
}

// ReleaseFileHandle closes a descriptor and reclaims unlinked content if unused.
func (f *FS) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.handles[op.Handle]
	if h == nil || h.directory {
		return syscall.EBADF
	}
	err := errors.Join(f.flushHandle(h), h.file.Close())
	delete(f.handles, op.Handle)
	inode, getErr := f.meta.Get(h.inode)
	if getErr == nil {
		f.reclaimLater(inode)
	}
	return systemError(errors.Join(err, getErr))
}

// SetInodeAttributes supports truncate, permission changes, and timestamps.
// Ownership changes are not yet supported.
func (f *FS) SetInodeAttributes(_ context.Context, op *fuseops.SetInodeAttributesOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	inode, err := f.get(op.Inode)
	if err != nil {
		return err
	}
	if op.Uid != nil || op.Gid != nil {
		return syscall.ENOTSUP
	}
	var h *handle
	if op.Handle != nil {
		h, err = f.fileHandle(*op.Handle, op.Inode)
		if err != nil {
			return err
		}
	} else if inode.Nlink == 0 {
		return syscall.ENOENT
	}
	attrs := metadata.Attributes{Size: op.Size, Mode: op.Mode, Atime: op.Atime, Mtime: op.Mtime}
	if op.Size != nil {
		if inode.Mode.IsDir() {
			return syscall.EISDIR
		}
		if *op.Size > math.MaxInt64 {
			return syscall.EFBIG
		}
		var file storage.File
		if h != nil {
			if h.flags&3 == os.O_RDONLY {
				return syscall.EBADF
			}
			file = h.file
		} else {
			file, err = f.store.Open(inode.Object, storage.ReadWrite)
			if err != nil {
				return systemError(err)
			}
		}
		err = file.Truncate(int64(*op.Size))
		if err == nil {
			if h != nil {
				h.dirty = true
			}
			err = f.commitContent(inode.ID, file, attrs)
			if h != nil && err == nil {
				h.dirty = false
			}
		}
		if h == nil {
			err = errors.Join(err, file.Close())
		}
		if err != nil {
			return systemError(err)
		}
	} else if op.Mode != nil || op.Atime != nil || op.Mtime != nil {
		if _, err := f.meta.SetAttributes(inode.ID, attrs); err != nil {
			return systemError(err)
		}
	}
	inode, err = f.meta.Get(inode.ID)
	op.Attributes = inodeAttributes(inode)
	return systemError(err)
}
