package localfs

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"sort"
	"syscall"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"golang.org/x/sys/unix"
)

func (f *FS) addHandle(h *handle) fuseops.HandleID {
	id := f.nextHandle
	f.nextHandle++
	f.handles[id] = h
	return id
}

func (f *FS) fileHandle(id fuseops.HandleID, inode fuseops.InodeID) (*handle, error) {
	h := f.handles[id]
	if h == nil || h.file == nil || h.node.id != inode || h.node.info.IsDir() {
		return nil, syscall.EBADF
	}
	return h, nil
}

// CreateFile creates and opens a new regular file exclusively.
func (f *FS) CreateFile(_ context.Context, op *fuseops.CreateFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, name, err := f.child(op.Parent, op.Name)
	if err != nil {
		return err
	}
	flags := int(op.OpenFlags) & (os.O_WRONLY | os.O_RDWR | os.O_SYNC)
	file, err := f.root.OpenFile(name, flags|os.O_CREATE|os.O_EXCL, op.Mode.Perm())
	if err != nil {
		return systemError(err)
	}
	info, err := file.Stat()
	if err != nil {
		return systemError(errors.Join(err, file.Close()))
	}
	n := f.remember(dir, op.Name, info)
	op.Entry = fuseops.ChildInodeEntry{Child: n.id, Attributes: fileAttributes(info)}
	op.Handle = f.addHandle(&handle{file: file, node: n, append: int(op.OpenFlags)&os.O_APPEND != 0})
	return nil
}

// OpenFile opens a regular file with the requested access mode.
func (f *FS) OpenFile(_ context.Context, op *fuseops.OpenFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.nodes[op.Inode]
	if n == nil || n.unlinked {
		return syscall.ENOENT
	}
	if n.info.IsDir() {
		return syscall.EISDIR
	}
	flags := int(op.OpenFlags) & (os.O_WRONLY | os.O_RDWR | os.O_TRUNC | os.O_SYNC)
	file, err := f.root.OpenFile(n.path(), flags, 0)
	if err != nil {
		return systemError(err)
	}
	op.Handle = f.addHandle(&handle{file: file, node: n, append: int(op.OpenFlags)&os.O_APPEND != 0})
	return nil
}

// ReadFile reads from an open descriptor at the supplied offset.
func (f *FS) ReadFile(_ context.Context, op *fuseops.ReadFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, err := f.fileHandle(op.Handle, op.Inode)
	if err != nil {
		return err
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

// WriteFile writes at an offset, or appends atomically when opened with O_APPEND.
func (f *FS) WriteFile(_ context.Context, op *fuseops.WriteFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, err := f.fileHandle(op.Handle, op.Inode)
	if err != nil {
		return err
	}
	if op.Offset < 0 {
		return syscall.EINVAL
	}
	offset := op.Offset
	if h.append {
		info, err := h.file.Stat()
		if err != nil {
			return systemError(err)
		}
		offset = info.Size()
	}
	n, err := h.file.WriteAt(op.Data, offset)
	if err == nil && n != len(op.Data) {
		err = io.ErrShortWrite
	}
	return systemError(err)
}

// SyncFile makes an open file's contents and metadata durable on local storage.
func (f *FS) SyncFile(_ context.Context, op *fuseops.SyncFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.handles[op.Handle]
	if h == nil || h.file == nil || h.node.id != op.Inode {
		return syscall.EBADF
	}
	return systemError(h.file.Sync())
}

// FlushFile also syncs on close and leaves the handle alive until release.
func (f *FS) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) error {
	return f.SyncFile(ctx, &fuseops.SyncFileOp{Inode: op.Inode, Handle: op.Handle})
}

// ReleaseFileHandle closes a descriptor after its final user releases it.
func (f *FS) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.handles[op.Handle]
	if h == nil || h.node.info.IsDir() {
		return syscall.EBADF
	}
	delete(f.handles, op.Handle)
	return systemError(h.file.Close())
}

// SetInodeAttributes supports truncate, permission changes, and timestamps.
// Ownership changes are outside the initial single-user scope.
func (f *FS) SetInodeAttributes(_ context.Context, op *fuseops.SetInodeAttributesOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.nodes[op.Inode]
	if n == nil {
		return syscall.ENOENT
	}
	if op.Uid != nil || op.Gid != nil {
		return syscall.ENOTSUP
	}
	var file *os.File
	if op.Handle != nil {
		h, err := f.fileHandle(*op.Handle, op.Inode)
		if err != nil {
			return err
		}
		file = h.file
	} else if n.unlinked {
		return syscall.ENOENT
	}
	if op.Size != nil {
		if n.info.IsDir() {
			return syscall.EISDIR
		}
		if *op.Size > math.MaxInt64 {
			return syscall.EFBIG
		}
		if file != nil {
			if err := file.Truncate(int64(*op.Size)); err != nil {
				return systemError(err)
			}
		} else {
			temporary, err := f.root.OpenFile(n.path(), os.O_WRONLY, 0)
			if err != nil {
				return systemError(err)
			}
			if err := errors.Join(temporary.Truncate(int64(*op.Size)), temporary.Close()); err != nil {
				return systemError(err)
			}
		}
	}
	if op.Mode != nil {
		var err error
		if file != nil {
			err = file.Chmod(*op.Mode)
		} else {
			err = f.root.Chmod(n.path(), *op.Mode)
		}
		if err != nil {
			return systemError(err)
		}
	}
	if op.Atime != nil || op.Mtime != nil {
		attrs, err := f.attributes(n)
		if err != nil {
			return err
		}
		atime, mtime := attrs.Atime, attrs.Mtime
		if op.Atime != nil {
			atime = *op.Atime
		}
		if op.Mtime != nil {
			mtime = *op.Mtime
		}
		if file != nil {
			times := []unix.Timeval{unix.NsecToTimeval(atime.UnixNano()), unix.NsecToTimeval(mtime.UnixNano())}
			err = unix.Futimes(int(file.Fd()), times)
		} else {
			err = f.root.Chtimes(n.path(), atime, mtime)
		}
		if err != nil {
			return systemError(err)
		}
	}
	var err error
	op.Attributes, err = f.attributes(n)
	return err
}

// OpenDir takes a stable directory listing for this handle's paginated reads.
func (f *FS) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.directory(op.Inode)
	if err != nil {
		return err
	}
	dir, err := f.root.Open(n.path())
	if err != nil {
		return systemError(err)
	}
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return systemError(errors.Join(err, dir.Close()))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	parent := n
	if n.parent != nil {
		parent = n.parent
	}
	listing := []fuseutil.Dirent{
		{Inode: n.id, Name: ".", Type: fuseutil.DT_Directory, Offset: 1},
		{Inode: parent.id, Name: "..", Type: fuseutil.DT_Directory, Offset: 2},
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return systemError(errors.Join(err, dir.Close()))
		}
		kind := fuseutil.DT_File
		if info.IsDir() {
			kind = fuseutil.DT_Directory
		} else if !info.Mode().IsRegular() {
			return systemError(errors.Join(syscall.ENOTSUP, dir.Close()))
		}
		child := f.remember(n, entry.Name(), info)
		listing = append(listing, fuseutil.Dirent{Inode: child.id, Name: entry.Name(), Type: kind, Offset: fuseops.DirOffset(len(listing) + 1)})
	}
	op.Handle = f.addHandle(&handle{file: dir, node: n, entries: listing})
	return nil
}

// ReadDir serves a page from an opened directory snapshot.
func (f *FS) ReadDir(_ context.Context, op *fuseops.ReadDirOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.handles[op.Handle]
	if h == nil || h.node.id != op.Inode || !h.node.info.IsDir() {
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
	if h == nil || !h.node.info.IsDir() {
		return syscall.EBADF
	}
	delete(f.handles, op.Handle)
	return systemError(h.file.Close())
}
