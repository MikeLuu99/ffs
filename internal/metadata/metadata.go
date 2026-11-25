// Package metadata owns the persistent namespace and file attributes. It has no
// dependency on FUSE or on the implementation that stores file contents.
package metadata

import (
	"os"
	"time"
)

// Root is the permanent root directory inode.
const Root uint64 = 1

// Inode identifies a file independently of its directory entry and content key.
// An unlinked inode has Nlink == 0 and is kept until its open handles are gone.
type Inode struct {
	ID     uint64
	Parent uint64
	Object string
	Mode   os.FileMode
	Size   uint64
	Nlink  uint32
	UID    uint32
	GID    uint32
	Atime  time.Time
	Mtime  time.Time
	Ctime  time.Time
	Btime  time.Time
}

// Entry is a directory name and its inode snapshot.
type Entry struct {
	Name  string
	Inode Inode
}

// Attributes contains optional changes; omitted attributes are preserved.
type Attributes struct {
	Size  *uint64
	Mode  *os.FileMode
	Atime *time.Time
	Mtime *time.Time
}

// Engine makes each namespace mutation atomic and durable. It leaves content
// deletion to the caller, which knows whether an unlinked file is still open.
// Adapters use io/fs errors and the namespace errors defined in this package;
// provider-specific errors must be translated at the adapter boundary.
type Engine interface {
	Get(id uint64) (Inode, error)
	Lookup(parent uint64, name string) (Inode, error)
	ReadDir(id uint64) ([]Entry, error)
	Create(parent uint64, name string, mode os.FileMode, uid, gid uint32, object string) (Inode, error)
	SetAttributes(id uint64, attrs Attributes) (Inode, error)
	Rename(oldParent uint64, oldName string, newParent uint64, newName string) (*Inode, error)
	Remove(parent uint64, name string, directory bool) (Inode, error)
	Delete(id uint64) error
	List() ([]Inode, error)
	Sync() error
	Close() error
}
