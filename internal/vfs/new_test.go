package vfs_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/metadata/bolt"
	"example.com/dfs/internal/storage"
	"example.com/dfs/internal/storage/memory"
	"example.com/dfs/internal/vfs"

	"github.com/jacobsa/fuse/fuseops"
)

type observedMetadata struct {
	metadata.Engine
	closed int
	lookup error
}

func (m *observedMetadata) Close() error {
	m.closed++
	return m.Engine.Close()
}

func (m *observedMetadata) Lookup(parent uint64, name string) (metadata.Inode, error) {
	if m.lookup != nil {
		return metadata.Inode{}, m.lookup
	}
	return m.Engine.Lookup(parent, name)
}

type observedStorage struct {
	storage.Engine
	closed   int
	closeErr error
}

func (s *observedStorage) Close() error {
	s.closed++
	return errors.Join(s.Engine.Close(), s.closeErr)
}

func adapterPair(t *testing.T) (*observedMetadata, *observedStorage) {
	t.Helper()
	meta, err := bolt.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = meta.Close() })
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	return &observedMetadata{Engine: meta}, &observedStorage{Engine: store}
}

func TestComposedAdaptersAndOwnership(t *testing.T) {
	meta, store := adapterPair(t)
	released := 0
	files, err := vfs.New(vfs.Config{
		Metadata: meta, Storage: store,
		Release: func() error {
			released++
			if meta.closed != 1 || store.closed != 1 {
				t.Error("lease released before the engines closed")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	ctx := context.Background()
	created := fuseops.CreateFileOp{Parent: fuseops.RootInodeID, Name: "composed", Mode: 0o600, OpenFlags: syscall.O_RDWR}
	if err := files.CreateFile(ctx, &created); err != nil {
		t.Fatal(err)
	}
	if err := files.WriteFile(ctx, &fuseops.WriteFileOp{Inode: created.Entry.Child, Handle: created.Handle, Data: []byte("adapter")}); err != nil {
		t.Fatal(err)
	}
	op := fuseops.ReadFileOp{Inode: created.Entry.Child, Handle: created.Handle, Size: 7, Dst: make([]byte, 7)}
	if err := files.ReadFile(ctx, &op); err != nil || string(op.Dst[:op.BytesRead]) != "adapter" {
		t.Fatalf("injected adapter read = %q, %v", op.Dst, err)
	}
	if err := files.StatFS(ctx, &fuseops.StatFSOp{}); !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("optional capacity = %v", err)
	}
	closeErr := errors.New("adapter close failure")
	store.closeErr = closeErr
	if err := files.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v", err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	if got := []int{meta.closed, store.closed, released}; !reflect.DeepEqual(got, []int{1, 1, 1}) {
		t.Fatalf("cleanup counts = %v", got)
	}
}

func TestFailedConstructionRetainsCallerOwnership(t *testing.T) {
	meta, store := adapterPair(t)
	if _, err := meta.Create(metadata.Root, "missing", 0o600, 0, 0, "missing-object"); err != nil {
		t.Fatal(err)
	}
	released := false
	if _, err := vfs.New(vfs.Config{Metadata: meta, Storage: store, Release: func() error { released = true; return nil }}); err == nil {
		t.Fatal("construction succeeded with missing content")
	}
	if meta.closed != 0 || store.closed != 0 || released {
		t.Fatal("failed construction took ownership")
	}
	if _, err := meta.Get(metadata.Root); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Keys(); err != nil {
		t.Fatal(err)
	}
	if _, err := vfs.New(vfs.Config{Metadata: meta}); err == nil {
		t.Fatal("accepted a missing storage adapter")
	}
	if _, err := vfs.New(vfs.Config{Storage: store}); err == nil {
		t.Fatal("accepted a missing metadata adapter")
	}
}

func TestProviderErrorsAreTranslatedAtFuseBoundary(t *testing.T) {
	meta, store := adapterPair(t)
	files, err := vfs.New(vfs.Config{Metadata: meta, Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	for _, test := range []struct {
		err  error
		want syscall.Errno
	}{
		{fs.ErrNotExist, syscall.ENOENT},
		{fs.ErrExist, syscall.EEXIST},
		{fs.ErrInvalid, syscall.EINVAL},
		{fs.ErrPermission, syscall.EACCES},
		{fs.ErrClosed, syscall.EBADF},
		{metadata.ErrNotDirectory, syscall.ENOTDIR},
		{metadata.ErrIsDirectory, syscall.EISDIR},
		{metadata.ErrNotEmpty, syscall.ENOTEMPTY},
		{metadata.ErrBusy, syscall.EBUSY},
		{metadata.ErrNameTooLong, syscall.ENAMETOOLONG},
		{metadata.ErrTooLarge, syscall.EFBIG},
		{errors.ErrUnsupported, syscall.ENOTSUP},
	} {
		meta.lookup = fmt.Errorf("provider response: %w", test.err)
		if err := files.LookUpInode(context.Background(), &fuseops.LookUpInodeOp{Parent: fuseops.RootInodeID, Name: "file"}); !errors.Is(err, test.want) {
			t.Errorf("provider error %v became %v, want %v", test.err, err, test.want)
		}
	}
}
