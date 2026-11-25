package vfs

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/metadata/bolt"
	"example.com/dfs/internal/storage"
	"example.com/dfs/internal/storage/disk"

	"github.com/jacobsa/fuse/fuseops"
)

type failMetadata struct {
	metadata.Engine
	fail bool
}

func (m *failMetadata) SetAttributes(id uint64, attrs metadata.Attributes) (metadata.Inode, error) {
	if m.fail {
		return metadata.Inode{}, syscall.EIO
	}
	return m.Engine.SetAttributes(id, attrs)
}

type failSync struct {
	storage.File
	fail bool
}

func (f *failSync) Sync() error {
	if f.fail {
		return syscall.EIO
	}
	return f.File.Sync()
}

func TestFailedCommitCanBeRetriedByFsync(t *testing.T) {
	for _, stage := range []string{"content sync", "metadata commit"} {
		t.Run(stage, func(t *testing.T) {
			directory := t.TempDir()
			metaAdapter, err := bolt.Open(filepath.Join(directory, "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			store, err := disk.Open(filepath.Join(directory, "objects"))
			if err != nil {
				_ = metaAdapter.Close()
				t.Fatal(err)
			}
			files, err := New(Config{Metadata: metaAdapter, Storage: store})
			if err != nil {
				_ = store.Close()
				_ = metaAdapter.Close()
				t.Fatal(err)
			}
			defer func() {
				if err := files.Close(); err != nil {
					t.Error(err)
				}
			}()
			ctx := context.Background()
			created := fuseops.CreateFileOp{Parent: fuseops.RootInodeID, Name: "file", Mode: 0o600, OpenFlags: syscall.O_RDWR}
			if err := files.CreateFile(ctx, &created); err != nil {
				t.Fatal(err)
			}
			h := files.handles[created.Handle]
			meta := &failMetadata{Engine: files.meta, fail: stage == "metadata commit"}
			file := &failSync{File: h.file, fail: stage == "content sync"}
			files.meta, h.file = meta, file
			err = files.WriteFile(ctx, &fuseops.WriteFileOp{Inode: created.Entry.Child, Handle: created.Handle, Data: []byte("durable")})
			meta.fail, file.fail = false, false
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("failed write = %v", err)
			}
			inode, err := files.meta.Get(uint64(created.Entry.Child))
			if err != nil || inode.Size != 0 {
				t.Fatalf("failed commit changed size: %+v, %v", inode, err)
			}
			if err := files.SyncFile(ctx, &fuseops.SyncFileOp{Inode: created.Entry.Child, Handle: created.Handle}); err != nil {
				t.Fatal(err)
			}
			inode, err = files.meta.Get(uint64(created.Entry.Child))
			if err != nil || inode.Size != 7 {
				t.Fatalf("retry did not commit size: %+v, %v", inode, err)
			}
		})
	}
}
