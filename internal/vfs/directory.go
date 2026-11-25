package vfs

import (
	"context"
	"syscall"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// OpenDir takes a metadata snapshot for stable paginated reads.
func (f *FS) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	inode, err := f.get(op.Inode)
	if err != nil {
		return err
	}
	entries, err := f.meta.ReadDir(inode.ID)
	if err != nil {
		return systemError(err)
	}
	listing := []fuseutil.Dirent{
		{Inode: op.Inode, Name: ".", Type: fuseutil.DT_Directory, Offset: 1},
		{Inode: fuseops.InodeID(inode.Parent), Name: "..", Type: fuseutil.DT_Directory, Offset: 2},
	}
	for _, entry := range entries {
		kind := fuseutil.DT_File
		if entry.Inode.Mode.IsDir() {
			kind = fuseutil.DT_Directory
		}
		listing = append(listing, fuseutil.Dirent{Inode: fuseops.InodeID(entry.Inode.ID), Name: entry.Name,
			Type: kind, Offset: fuseops.DirOffset(len(listing) + 1)})
	}
	op.Handle = f.addHandle(&handle{inode: inode.ID, entries: listing, directory: true})
	return nil
}

// ReadDir serves a page from an opened directory snapshot.
func (f *FS) ReadDir(_ context.Context, op *fuseops.ReadDirOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.handles[op.Handle]
	if h == nil || h.inode != uint64(op.Inode) || !h.directory {
		return syscall.EBADF
	}
	if uint64(op.Offset) > uint64(len(h.entries)) {
		return syscall.EINVAL
	}
	for _, entry := range h.entries[op.Offset:] {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], entry)
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
	return nil
}

// ReleaseDirHandle discards a directory listing snapshot.
func (f *FS) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.handles[op.Handle]
	if h == nil || !h.directory {
		return syscall.EBADF
	}
	delete(f.handles, op.Handle)
	inode, err := f.meta.Get(h.inode)
	if err == nil {
		f.reclaimLater(inode)
	}
	return systemError(err)
}
