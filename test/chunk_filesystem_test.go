package integration_test

import (
	"context"
	"errors"
	"io/fs"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"example.com/dfs/internal/app"
	"example.com/dfs/internal/chunk"
	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/metadata/redis"
	"example.com/dfs/internal/metadata/redistest"
	"example.com/dfs/internal/storage/s3"
	"example.com/dfs/test/s3test"

	"github.com/jacobsa/fuse/fuseops"
	goredis "github.com/redis/go-redis/v9"
)

func TestS3FilesystemRestartAndFailure(t *testing.T) {
	rc := redistest.Config(t)
	server := s3test.New()
	config := app.Config{RedisURL: rc.URL, Namespace: rc.Namespace, Storage: "s3", S3: server.Config("ignored")}
	data := t.TempDir()
	files, err := app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	file := create(t, files, fuseops.RootInodeID, "before")
	write(t, files, file, "persistent", 0)
	server.FailWrites(true)
	if err := files.WriteFile(context.Background(), &fuseops.WriteFileOp{Inode: file.Entry.Child, Handle: file.Handle, Data: []byte("destroyed")}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("failed upload = %v", err)
	}
	server.FailWrites(false)
	if got := read(t, files, file.Entry.Child, file.Handle); got != "persistent" {
		t.Fatalf("failed upload changed data: %q", got)
	}
	if err := files.Rename(context.Background(), &fuseops.RenameOp{OldParent: fuseops.RootInodeID, OldName: "before", NewParent: fuseops.RootInodeID, NewName: "after"}); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	id := lookup(t, files, fuseops.RootInodeID, "after")
	if id != file.Entry.Child {
		t.Fatal("inode changed after reopen")
	}
	opened := fuseops.OpenFileOp{Inode: id, OpenFlags: syscall.O_RDWR}
	if err := files.OpenFile(context.Background(), &opened); err != nil {
		t.Fatal(err)
	}
	if got := read(t, files, id, opened.Handle); got != "persistent" {
		t.Fatalf("reopened bytes = %q", got)
	}
	if err := files.Unlink(context.Background(), &fuseops.UnlinkOp{Parent: fuseops.RootInodeID, Name: "after"}); err != nil {
		t.Fatal(err)
	}
	if err := files.WriteFile(context.Background(), &fuseops.WriteFileOp{Inode: id, Handle: opened.Handle, Offset: 10, Data: []byte("!")}); err != nil {
		t.Fatal(err)
	}
	if got := read(t, files, id, opened.Handle); got != "persistent!" {
		t.Fatalf("open unlinked data = %q", got)
	}
	if err := files.ReleaseFileHandle(context.Background(), &fuseops.ReleaseFileHandleOp{Handle: opened.Handle}); err != nil {
		t.Fatal(err)
	}
	objects, _ := s3.New(server.Config("volumes/" + rc.Namespace))
	if keys, err := objects.List(context.Background()); err != nil || len(keys) != 0 {
		t.Fatalf("unlinked blocks remain = %v, %v", keys, err)
	}
}

func TestLayoutAndInodeSizeCommitTogether(t *testing.T) {
	meta, err := redis.Open(redistest.Config(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = meta.Close() })
	objects, _ := s3.New(s3test.New().Config("volume"))
	store, err := chunk.New(objects, meta)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	key, file, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	inode, err := meta.Create(metadata.Root, "atomic", 0o600, 0, 0, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("abc"), 0); err != nil {
		t.Fatal(err)
	}
	if got, err := meta.Get(inode.ID); err != nil || got.Size != 3 {
		t.Fatalf("inode not committed with layout: %+v, %v", got, err)
	}
	if err := file.Truncate(1); err != nil {
		t.Fatal(err)
	}
	if got, err := meta.Get(inode.ID); err != nil || got.Size != 1 {
		t.Fatalf("truncate not atomic: %+v, %v", got, err)
	}
}

func TestS3RecoveryPreservesBlocksOnMetadataLoss(t *testing.T) {
	rc := redistest.Config(t)
	server := s3test.New()
	config := app.Config{RedisURL: rc.URL, Namespace: rc.Namespace, Storage: "s3", S3: server.Config("ignored")}
	data := t.TempDir()
	files, err := app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	file := create(t, files, fuseops.RootInodeID, "keep")
	write(t, files, file, "preserve", 0)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	objects, _ := s3.New(server.Config("volumes/" + rc.Namespace))
	before, err := objects.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	changed := config
	changed.S3.Bucket = "elsewhere"
	if reopened, err := app.Open(data, changed); err == nil {
		_ = reopened.Close()
		t.Fatal("bucket change accepted")
	}
	opts, err := goredis.ParseURL(rc.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := goredis.NewClient(opts)
	defer func() { _ = client.Close() }()
	if err := client.Del(context.Background(), "dfs:{"+rc.Namespace+"}:state").Err(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := app.Open(data, config); err == nil {
		_ = reopened.Close()
		t.Fatal("missing metadata reformatted")
	} else if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	after, err := objects.List(context.Background())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("metadata loss swept blocks: %v -> %v, %v", before, after, err)
	}
}

func TestS3MissingLiveBlockStopsOrphanCleanup(t *testing.T) {
	rc := redistest.Config(t)
	server := s3test.New()
	config := app.Config{RedisURL: rc.URL, Namespace: rc.Namespace, Storage: "s3", S3: server.Config("ignored")}
	data := t.TempDir()
	files, err := app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	file := create(t, files, fuseops.RootInodeID, "missing")
	write(t, files, file, "data", 0)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	rc.Create = false
	meta, err := redis.Open(rc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = meta.Close() }()
	inode, err := meta.Get(uint64(file.Entry.Child))
	if err != nil {
		t.Fatal(err)
	}
	layout, err := meta.GetLayout(inode.Object)
	if err != nil {
		t.Fatal(err)
	}
	objects, _ := s3.New(server.Config("volumes/" + rc.Namespace))
	store, _ := chunk.New(objects, meta)
	defer func() { _ = store.Close() }()
	orphan, content, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := content.WriteAt([]byte("orphan"), 0); err != nil {
		t.Fatal(err)
	}
	if err := content.Close(); err != nil {
		t.Fatal(err)
	}
	if err := objects.Delete(context.Background(), layout.Blocks[0]); err != nil {
		t.Fatal(err)
	}
	if reopened, err := app.Open(data, config); err == nil {
		_ = reopened.Close()
		t.Fatal("missing block accepted")
	} else if !strings.Contains(err.Error(), "verify content") {
		t.Fatal(err)
	}
	if _, err := meta.GetLayout(orphan); err != nil {
		t.Fatalf("failed startup deleted orphan layout: %v", err)
	}
}
