// Package storage stores file contents under opaque keys, independently of names
// and directory structure. Callers must Sync a file before committing its size.
package storage

import "io"

// Access describes portable content access without operating-system open flags.
type Access uint8

const (
	ReadOnly Access = iota
	WriteOnly
	ReadWrite
)

// File is an open content object. Its lifetime is independent of its name.
type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Size() (uint64, error)
	Truncate(size int64) error
	Sync() error
}

// Engine provides content storage without knowing about filesystem metadata.
// Keys are opaque to callers. Remove and Close are idempotent. Methods must be
// safe for concurrent use; open files remain caller-owned after engine Close.
// Sync follows the provider's persistence model (memory storage is volatile).
type Engine interface {
	Create() (string, File, error)
	Open(key string, access Access) (File, error)
	Remove(key string) error
	Keys() ([]string, error)
	Sync() error
	Close() error
}

// Capacity is optional capacity information reported by a storage adapter.
type Capacity struct {
	BlockSize       uint32
	IOSize          uint32
	Blocks          uint64
	BlocksFree      uint64
	BlocksAvailable uint64
	Inodes          uint64
	InodesFree      uint64
}

// CapacityReporter is an optional capability. Providers without meaningful
// capacity information can implement Engine without implementing this interface.
type CapacityReporter interface {
	Capacity() (Capacity, error)
}
