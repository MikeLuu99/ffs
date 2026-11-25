package vfs

import (
	"errors"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"

	"github.com/jacobsa/fuse/fuseops"
)

// Config supplies adapters and an optional resource-release callback. A caller
// may decorate either engine without changing filesystem operations.
type Config struct {
	Metadata metadata.Engine
	Storage  storage.Engine
	// Release runs after both engines close, for example to release a lease.
	Release func() error
}

// New recovers an exclusively owned metadata/content pair. Storage must be a
// dedicated namespace for this metadata engine; recovery removes unreferenced
// objects. Swapping providers does not migrate their existing data.
//
// Ownership of the engines and Release transfers only on success. Close closes
// both engines and calls Release once. On error the caller retains ownership.
func New(config Config) (*FS, error) {
	if config.Metadata == nil || config.Storage == nil {
		return nil, errors.New("metadata and storage engines are required")
	}
	root, err := config.Metadata.Get(metadata.Root)
	if err != nil {
		return nil, err
	}
	if root.ID != metadata.Root || !root.Mode.IsDir() || root.Nlink == 0 {
		return nil, errors.New("metadata engine has no valid root directory")
	}
	files := &FS{
		meta: config.Metadata, store: config.Storage, release: config.Release,
		handles: make(map[fuseops.HandleID]*handle), nextHandle: 1,
	}
	if err := files.recover(); err != nil {
		return nil, err
	}
	return files, nil
}
