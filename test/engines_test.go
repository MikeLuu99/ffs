package integration_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"example.com/dfs/internal/app"
	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/metadata/bolt"
	"example.com/dfs/internal/storage"
	"example.com/dfs/internal/storage/disk"

	"github.com/jacobsa/fuse/fuseops"
)

func objectNames(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(directory, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestContentAndNamespaceLifecycle(t *testing.T) {
	ctx := context.Background()
	data := t.TempDir()
	files := openFS(t, data)
	file := create(t, files, fuseops.RootInodeID, "original")
	write(t, files, file, "content", 0)
	keys := objectNames(t, data)
	if len(keys) != 1 || keys[0] == "original" {
		t.Fatalf("content objects = %v", keys)
	}
	if _, err := os.Stat(filepath.Join(data, "original")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mounted name leaked into backing directory: %v", err)
	}
	if err := files.Rename(ctx, &fuseops.RenameOp{OldParent: fuseops.RootInodeID, OldName: "original", NewParent: fuseops.RootInodeID, NewName: "renamed"}); err != nil {
		t.Fatal(err)
	}
	if after := objectNames(t, data); len(after) != 1 || after[0] != keys[0] {
		t.Fatalf("rename moved content: %v -> %v", keys, after)
	}
	duplicate := fuseops.CreateFileOp{Parent: fuseops.RootInodeID, Name: "renamed", Mode: 0o600}
	if err := files.CreateFile(ctx, &duplicate); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("duplicate create = %v", err)
	}
	if after := objectNames(t, data); len(after) != 1 {
		t.Fatalf("failed create leaked content: %v", after)
	}
	if err := files.Unlink(ctx, &fuseops.UnlinkOp{Parent: fuseops.RootInodeID, Name: "renamed"}); err != nil {
		t.Fatal(err)
	}
	write(t, files, file, "!", 7)
	if got := read(t, files, file.Entry.Child, file.Handle); got != "content!" {
		t.Fatalf("unlinked file = %q", got)
	}
	if keys := objectNames(t, data); len(keys) != 1 {
		t.Fatalf("open content was reclaimed: %v", keys)
	}
	if err := files.ReleaseFileHandle(ctx, &fuseops.ReleaseFileHandleOp{Handle: file.Handle}); err != nil {
		t.Fatal(err)
	}
	if keys := objectNames(t, data); len(keys) != 0 {
		t.Fatalf("closed unlinked content was not reclaimed: %v", keys)
	}
}

func TestFilesystemCrashRecovery(t *testing.T) {
	if data := os.Getenv("DFS_CRASH_FIXTURE"); data != "" {
		crashFixture(t, data)
		return
	}
	data := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFilesystemCrashRecovery$")
	command.Env = append(os.Environ(), "DFS_CRASH_FIXTURE="+data)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("crash fixture: %v\n%s", err, output)
	}
	files := openFS(t, data)
	id := lookup(t, files, fuseops.RootInodeID, "live")
	if uint64(id) != metadata.Root+1 {
		t.Fatalf("persistent inode identity = %d", id)
	}
	op := fuseops.OpenFileOp{Inode: id}
	if err := files.OpenFile(context.Background(), &op); err != nil {
		t.Fatal(err)
	}
	if got := read(t, files, id, op.Handle); got != "beforeafter" {
		t.Fatalf("recovered content = %q", got)
	}
	attrs := fuseops.GetInodeAttributesOp{Inode: id}
	if err := files.GetInodeAttributes(context.Background(), &attrs); err != nil || attrs.Attributes.Size != 11 {
		t.Fatalf("recovered size = %d, %v", attrs.Attributes.Size, err)
	}
	if keys := objectNames(t, data); len(keys) != 1 {
		t.Fatalf("abandoned content survived recovery: %v", keys)
	}
	if err := files.LookUpInode(context.Background(), &fuseops.LookUpInodeOp{Parent: fuseops.RootInodeID, Name: "doomed"}); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("unlinked name survived recovery: %v", err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	meta, err := bolt.Open(filepath.Join(data, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = meta.Close() }()
	if inodes, err := meta.List(); err != nil || len(inodes) != 2 {
		t.Fatalf("tombstone survived recovery: %v, %v", inodes, err)
	}
}

func crashFixture(t *testing.T, data string) {
	t.Helper()
	files, err := app.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	file := create(t, files, fuseops.RootInodeID, "live")
	write(t, files, file, "before", 0)
	liveKey := objectNames(t, data)[0]
	doomed := create(t, files, fuseops.RootInodeID, "doomed")
	write(t, files, doomed, "deleted but open", 0)
	if err := files.Unlink(context.Background(), &fuseops.UnlinkOp{Parent: fuseops.RootInodeID, Name: "doomed"}); err != nil {
		t.Fatal(err)
	}
	disk, err := disk.Open(filepath.Join(data, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash between content allocation and namespace creation.
	if _, _, err := disk.Create(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash between content sync and its size metadata transaction.
	content, err := disk.Open(liveKey, storage.ReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := content.WriteAt([]byte("after"), 6); err != nil {
		t.Fatal(err)
	}
	if err := content.Sync(); err != nil {
		t.Fatal(err)
	}
	// Intentionally bypass cleanup with outstanding handles and a tombstone.
	os.Exit(0)
}

func TestMissingContentStopsRecoveryWithoutSweeping(t *testing.T) {
	data := t.TempDir()
	files := openFS(t, data)
	create(t, files, fuseops.RootInodeID, "missing")
	key := objectNames(t, data)[0]
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	disk, err := disk.Open(filepath.Join(data, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = disk.Close() }()
	if err := disk.Remove(key); err != nil {
		t.Fatal(err)
	}
	orphan, content, err := disk.Create()
	if err != nil {
		t.Fatal(err)
	}
	if err := content.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := app.Open(data); err == nil {
		_ = reopened.Close()
		t.Fatal("opened a namespace with missing content")
	}
	if keys := objectNames(t, data); len(keys) != 1 || keys[0] != orphan {
		t.Fatalf("failed recovery deleted content: %v", keys)
	}
	// Failed startup must also release the metadata database lock.
	meta, err := bolt.Open(filepath.Join(data, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyBackingFilesArePreserved(t *testing.T) {
	data := t.TempDir()
	filename := filepath.Join(data, "keep.txt")
	if err := os.WriteFile(filename, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if files, err := app.Open(data); err == nil {
		_ = files.Close()
		t.Fatal("opened an unformatted directory with existing files")
	} else if !strings.Contains(err.Error(), "empty -data directory") {
		t.Fatalf("legacy directory error = %v", err)
	}
	if got, err := os.ReadFile(filename); err != nil || string(got) != "keep" {
		t.Fatalf("existing content = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(data, "metadata.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed initialization created metadata: %v", err)
	}
}

func TestEmptyMetadataDoesNotReformatExistingObjects(t *testing.T) {
	data := t.TempDir()
	disk, err := disk.Open(filepath.Join(data, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	key, content, err := disk.Create()
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(content.Close(), disk.Close()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "metadata.db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if files, err := app.Open(data); err == nil {
		_ = files.Close()
		t.Fatal("empty metadata was initialized over existing content")
	}
	if keys := objectNames(t, data); len(keys) != 1 || keys[0] != key {
		t.Fatalf("existing object was removed: %v", keys)
	}
}
