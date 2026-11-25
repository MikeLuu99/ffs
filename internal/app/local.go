// Package app composes engine adapters and the mount for the local deployment.
package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"example.com/dfs/internal/metadata/bolt"
	"example.com/dfs/internal/storage/disk"
	"example.com/dfs/internal/vfs"
)

// Open opens both persistent engines and exclusively locks their backing directory.
// Unformatted directories must be empty; legacy backing files are never imported
// or deleted implicitly.
func Open(directory string) (*vfs.FS, error) {
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
	fail := func(err error) (*vfs.FS, error) { return nil, errors.Join(err, lock.Close(), root.Close()) }
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(err)
	}
	entries, err := lock.ReadDir(-1)
	if err != nil {
		return fail(err)
	}
	formatted := false
	for _, entry := range entries {
		if entry.Name() == "metadata.db" && entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return fail(err)
			}
			if info.Size() == 0 && len(entries) > 1 {
				return fail(errors.New("empty metadata.db alongside existing entries; refusing to initialize over existing content"))
			}
			formatted = true
			continue
		}
		if entry.Name() == "objects" && entry.IsDir() {
			continue
		}
		return fail(fmt.Errorf("unrecognized backing entry %q; choose an empty -data directory (existing files are untouched)", entry.Name()))
	}
	if !formatted && len(entries) != 0 {
		return fail(errors.New("metadata.db is missing; choose an empty -data directory (existing files are untouched)"))
	}
	meta, err := bolt.Open(filepath.Join(directory, "metadata.db"))
	if err != nil {
		return fail(fmt.Errorf("open metadata: %w", err))
	}
	store, err := disk.Open(filepath.Join(directory, "objects"))
	if err != nil {
		return fail(errors.Join(err, meta.Close()))
	}
	if err := lock.Sync(); err != nil {
		return fail(errors.Join(err, store.Close(), meta.Close()))
	}
	files, err := vfs.New(vfs.Config{
		Metadata: meta,
		Storage:  store,
		Release:  func() error { return errors.Join(lock.Close(), root.Close()) },
	})
	if err != nil {
		// Failed construction leaves ownership with the caller. Do not reclaim
		// anything after a failed consistency check.
		return fail(errors.Join(err, store.Close(), meta.Close()))
	}
	return files, nil
}
