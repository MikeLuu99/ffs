// Package localfs implements a small disk-backed FUSE filesystem.
package localfs

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type node struct {
	id       fuseops.InodeID
	name     string
	parent   *node
	children map[string]*node
	info     os.FileInfo
	unlinked bool
}

func (n *node) path() string {
	if n.parent == nil {
		return "."
	}
	return path.Join(n.parent.path(), n.name)
}

type handle struct {
	file    *os.File
	node    *node
	append  bool
	entries []fuseutil.Dirent
}

// FS serves one backing directory. Access is serialized for this first version.
// Files must be changed through this instance while it is open.
type FS struct {
	fuseutil.NotImplementedFileSystem
	mu         sync.Mutex
	root       *os.Root
	lock       *os.File
	nodes      map[fuseops.InodeID]*node
	handles    map[fuseops.HandleID]*handle
	nextInode  fuseops.InodeID
	nextHandle fuseops.HandleID
}

var _ fuseutil.FileSystem = (*FS)(nil)

// Open creates or opens a persistent backing directory and locks it exclusively.
func Open(directory string) (*FS, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	lock, err := root.Open(".")
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(err, lock.Close(), root.Close())
	}
	info, err := lock.Stat()
	if err != nil {
		return nil, errors.Join(err, lock.Close(), root.Close())
	}
	first := &node{id: fuseops.RootInodeID, info: info, children: make(map[string]*node)}
	return &FS{
		root: root, lock: lock,
		nodes:   map[fuseops.InodeID]*node{first.id: first},
		handles: make(map[fuseops.HandleID]*handle), nextInode: first.id + 1, nextHandle: 1,
	}, nil
}

// Close releases open handles and the backing-directory lock. It is idempotent.
func (f *FS) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.root == nil {
		return nil
	}
	var err error
	for _, h := range f.handles {
		if h.file != nil {
			err = errors.Join(err, h.file.Close())
		}
	}
	err = errors.Join(err, f.lock.Close(), f.root.Close())
	f.root, f.nodes, f.handles = nil, nil, nil
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
	return syscall.EIO
}

func (f *FS) directory(id fuseops.InodeID) (*node, error) {
	if f.root == nil {
		return nil, syscall.EBADF
	}
	n := f.nodes[id]
	if n == nil || n.unlinked {
		return nil, syscall.ENOENT
	}
	if !n.info.IsDir() {
		return nil, syscall.ENOTDIR
	}
	return n, nil
}

func (f *FS) child(parent fuseops.InodeID, name string) (*node, string, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return nil, "", syscall.EINVAL
	}
	dir, err := f.directory(parent)
	if err != nil {
		return nil, "", err
	}
	return dir, path.Join(dir.path(), name), nil
}

func (f *FS) remember(parent *node, name string, info os.FileInfo) *node {
	if n := parent.children[name]; n != nil {
		n.info = info
		return n
	}
	n := &node{id: f.nextInode, name: name, parent: parent, info: info, children: make(map[string]*node)}
	f.nextInode++
	f.nodes[n.id], parent.children[name] = n, n
	return n
}

func (f *FS) attributes(n *node) (fuseops.InodeAttributes, error) {
	if n == nil {
		return fuseops.InodeAttributes{}, syscall.ENOENT
	}
	for _, h := range f.handles {
		if h.node == n && h.file != nil {
			info, err := h.file.Stat()
			if err != nil {
				return fuseops.InodeAttributes{}, systemError(err)
			}
			n.info = info
			return fileAttributes(info), nil
		}
	}
	if !n.unlinked {
		info, err := f.root.Stat(n.path())
		if err != nil {
			return fuseops.InodeAttributes{}, systemError(err)
		}
		n.info = info
	}
	attrs := fileAttributes(n.info)
	if n.unlinked {
		attrs.Nlink = 0
	}
	return attrs, nil
}

// LookUpInode resolves a child name within a directory.
func (f *FS) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, name, err := f.child(op.Parent, op.Name)
	if err != nil {
		return err
	}
	info, err := f.root.Lstat(name)
	if err != nil {
		return systemError(err)
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return syscall.ENOTSUP
	}
	n := f.remember(dir, op.Name, info)
	op.Entry = fuseops.ChildInodeEntry{Child: n.id, Attributes: fileAttributes(info)}
	return nil
}

// GetInodeAttributes reports current metadata, including for unlinked open files.
func (f *FS) GetInodeAttributes(_ context.Context, op *fuseops.GetInodeAttributesOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	op.Attributes, err = f.attributes(f.nodes[op.Inode])
	return err
}

// StatFS reports the backing filesystem's capacity.
func (f *FS) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.root == nil {
		return syscall.EBADF
	}
	return filesystemStats(int(f.lock.Fd()), op)
}

// MkDir creates a directory in the backing store.
func (f *FS) MkDir(_ context.Context, op *fuseops.MkDirOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, name, err := f.child(op.Parent, op.Name)
	if err != nil {
		return err
	}
	if err := f.root.Mkdir(name, op.Mode.Perm()); err != nil {
		return systemError(err)
	}
	info, err := f.root.Stat(name)
	if err != nil {
		return systemError(err)
	}
	n := f.remember(dir, op.Name, info)
	op.Entry = fuseops.ChildInodeEntry{Child: n.id, Attributes: fileAttributes(info)}
	return nil
}

// Rename atomically moves an entry and preserves its in-memory inode identity.
func (f *FS) Rename(_ context.Context, op *fuseops.RenameOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	oldDir, oldPath, err := f.child(op.OldParent, op.OldName)
	if err != nil {
		return err
	}
	newDir, newPath, err := f.child(op.NewParent, op.NewName)
	if err != nil {
		return err
	}
	if err := f.root.Rename(oldPath, newPath); err != nil {
		return systemError(err)
	}
	if oldPath == newPath {
		return nil
	}
	if target := newDir.children[op.NewName]; target != nil {
		target.unlinked = true
		delete(newDir.children, op.NewName)
	}
	if source := oldDir.children[op.OldName]; source != nil {
		delete(oldDir.children, op.OldName)
		source.parent, source.name = newDir, op.NewName
		newDir.children[op.NewName] = source
	}
	return nil
}

func (f *FS) remove(parent fuseops.InodeID, name string, directory bool) error {
	dir, filePath, err := f.child(parent, name)
	if err != nil {
		return err
	}
	info, err := f.root.Lstat(filePath)
	if err != nil {
		return systemError(err)
	}
	if directory && !info.IsDir() {
		return syscall.ENOTDIR
	}
	if !directory && info.IsDir() {
		return syscall.EISDIR
	}
	if err := f.root.Remove(filePath); err != nil {
		return systemError(err)
	}
	if n := dir.children[name]; n != nil {
		n.unlinked = true
		delete(dir.children, name)
	}
	return nil
}

// Unlink removes a file name; open handles remain usable.
func (f *FS) Unlink(_ context.Context, op *fuseops.UnlinkOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remove(op.Parent, op.Name, false)
}

// RmDir removes an empty directory.
func (f *FS) RmDir(_ context.Context, op *fuseops.RmDirOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remove(op.Parent, op.Name, true)
}

// ForgetInode accepts cache-reference drops. Inode metadata is retained until close.
func (*FS) ForgetInode(context.Context, *fuseops.ForgetInodeOp) error { return nil }

// BatchForget accepts batched cache-reference drops.
func (*FS) BatchForget(context.Context, *fuseops.BatchForgetOp) error { return nil }
