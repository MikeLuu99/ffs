package integration_test

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"example.com/dfs/internal/app"
	"example.com/dfs/internal/vfs"

	"github.com/jacobsa/fuse/fuseops"
)

func openFS(t *testing.T, directory string) *vfs.FS {
	t.Helper()
	files, err := app.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	})
	return files
}

func create(t *testing.T, files *vfs.FS, parent fuseops.InodeID, name string) fuseops.CreateFileOp {
	t.Helper()
	op := fuseops.CreateFileOp{Parent: parent, Name: name, Mode: 0o600, OpenFlags: syscall.O_RDWR}
	if err := files.CreateFile(context.Background(), &op); err != nil {
		t.Fatal(err)
	}
	return op
}

func lookup(t *testing.T, files *vfs.FS, parent fuseops.InodeID, name string) fuseops.InodeID {
	t.Helper()
	op := fuseops.LookUpInodeOp{Parent: parent, Name: name}
	if err := files.LookUpInode(context.Background(), &op); err != nil {
		t.Fatal(err)
	}
	return op.Entry.Child
}

func write(t *testing.T, files *vfs.FS, file fuseops.CreateFileOp, value string, offset int64) {
	t.Helper()
	if err := files.WriteFile(context.Background(), &fuseops.WriteFileOp{Inode: file.Entry.Child, Handle: file.Handle, Data: []byte(value), Offset: offset}); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, files *vfs.FS, inode fuseops.InodeID, handle fuseops.HandleID) string {
	t.Helper()
	op := fuseops.ReadFileOp{Inode: inode, Handle: handle, Dst: make([]byte, 1024), Size: 1024}
	if err := files.ReadFile(context.Background(), &op); err != nil {
		t.Fatal(err)
	}
	return string(op.Dst[:op.BytesRead])
}

func TestRestartAndDirectoryRename(t *testing.T) {
	ctx := context.Background()
	data := t.TempDir()
	files := openFS(t, data)
	stats := fuseops.StatFSOp{}
	if err := files.StatFS(ctx, &stats); err != nil || stats.BlockSize == 0 {
		t.Fatalf("filesystem capacity = %+v, %v", stats, err)
	}
	dir := fuseops.MkDirOp{Parent: fuseops.RootInodeID, Name: "docs", Mode: 0o700}
	if err := files.MkDir(ctx, &dir); err != nil {
		t.Fatal(err)
	}
	file := create(t, files, dir.Entry.Child, "hello.txt")
	write(t, files, file, "hello", 0)
	write(t, files, file, "a", 1)
	if got := read(t, files, file.Entry.Child, file.Handle); got != "hallo" {
		t.Fatalf("read = %q", got)
	}
	if err := files.SyncFile(ctx, &fuseops.SyncFileOp{Inode: file.Entry.Child, Handle: file.Handle}); err != nil {
		t.Fatal(err)
	}
	if err := files.Rename(ctx, &fuseops.RenameOp{OldParent: fuseops.RootInodeID, OldName: "docs", NewParent: fuseops.RootInodeID, NewName: "notes"}); err != nil {
		t.Fatal(err)
	}
	if id := lookup(t, files, dir.Entry.Child, "hello.txt"); id != file.Entry.Child {
		t.Fatal("rename changed the file identity")
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files = openFS(t, data)
	directory := lookup(t, files, fuseops.RootInodeID, "notes")
	id := lookup(t, files, directory, "hello.txt")
	if directory != dir.Entry.Child || id != file.Entry.Child {
		t.Fatal("inode identity changed after restart")
	}
	opened := fuseops.OpenFileOp{Inode: id}
	if err := files.OpenFile(ctx, &opened); err != nil {
		t.Fatal(err)
	}
	if got := read(t, files, id, opened.Handle); got != "hallo" {
		t.Fatalf("after restart = %q", got)
	}
	if err := files.FlushFile(ctx, &fuseops.FlushFileOp{Inode: id, Handle: opened.Handle}); err != nil {
		t.Fatal(err)
	}
	if err := files.WriteFile(ctx, &fuseops.WriteFileOp{Inode: id, Handle: opened.Handle, Data: []byte("x")}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only write = %v", err)
	}
}

func TestOpenFilesSurviveUnlinkAndReplacement(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "unlink", true: "rename-overwrite"}[replace], func(t *testing.T) {
			ctx := context.Background()
			files := openFS(t, t.TempDir())
			old := create(t, files, fuseops.RootInodeID, "target")
			write(t, files, old, "old", 0)
			var fresh fuseops.CreateFileOp
			if replace {
				fresh = create(t, files, fuseops.RootInodeID, "source")
				write(t, files, fresh, "new", 0)
				if err := files.Rename(ctx, &fuseops.RenameOp{OldParent: fuseops.RootInodeID, OldName: "source", NewParent: fuseops.RootInodeID, NewName: "target"}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := files.Unlink(ctx, &fuseops.UnlinkOp{Parent: fuseops.RootInodeID, Name: "target"}); err != nil {
					t.Fatal(err)
				}
				fresh = create(t, files, fuseops.RootInodeID, "target")
				write(t, files, fresh, "new", 0)
			}
			if got := lookup(t, files, fuseops.RootInodeID, "target"); got != fresh.Entry.Child || got == old.Entry.Child {
				t.Fatal("replacement did not establish a new inode")
			}
			if got := read(t, files, old.Entry.Child, old.Handle); got != "old" {
				t.Fatalf("old open descriptor read %q", got)
			}
			attrs := fuseops.GetInodeAttributesOp{Inode: old.Entry.Child}
			if err := files.GetInodeAttributes(ctx, &attrs); err != nil || attrs.Attributes.Nlink != 0 {
				t.Fatalf("unlinked attributes = %+v, %v", attrs.Attributes, err)
			}
		})
	}
}

func TestTruncatePermissionsAndTimestamps(t *testing.T) {
	ctx := context.Background()
	files := openFS(t, t.TempDir())
	file := create(t, files, fuseops.RootInodeID, "file")
	write(t, files, file, "abcdef", 0)
	size, mode, mtime := uint64(3), os.FileMode(0o640), time.Unix(1_700_000_000, 0)
	op := fuseops.SetInodeAttributesOp{Inode: file.Entry.Child, Handle: &file.Handle, Size: &size, Mode: &mode, Mtime: &mtime}
	if err := files.SetInodeAttributes(ctx, &op); err != nil {
		t.Fatal(err)
	}
	if op.Attributes.Size != size || op.Attributes.Mode.Perm() != mode || !op.Attributes.Mtime.Equal(mtime) {
		t.Fatalf("unexpected attributes: %+v", op.Attributes)
	}
	if got := read(t, files, file.Entry.Child, file.Handle); got != "abc" {
		t.Fatalf("truncate read = %q", got)
	}
	if err := files.ReleaseFileHandle(ctx, &fuseops.ReleaseFileHandleOp{Handle: file.Handle}); err != nil {
		t.Fatal(err)
	}
	if err := files.ReadFile(ctx, &fuseops.ReadFileOp{Inode: file.Entry.Child, Handle: file.Handle}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("released handle read = %v", err)
	}
}

func TestDirectoryPaginationAndErrors(t *testing.T) {
	ctx := context.Background()
	files := openFS(t, t.TempDir())
	create(t, files, fuseops.RootInodeID, "b")
	create(t, files, fuseops.RootInodeID, "a")
	dir := fuseops.OpenDirOp{Inode: fuseops.RootInodeID}
	if err := files.OpenDir(ctx, &dir); err != nil {
		t.Fatal(err)
	}
	create(t, files, fuseops.RootInodeID, "later")
	var names []string
	for offset := fuseops.DirOffset(0); ; offset++ {
		op := fuseops.ReadDirOp{Inode: dir.Inode, Handle: dir.Handle, Offset: offset, Dst: make([]byte, 32)}
		if err := files.ReadDir(ctx, &op); err != nil {
			t.Fatal(err)
		}
		if op.BytesRead == 0 {
			break
		}
		length := binary.LittleEndian.Uint32(op.Dst[16:20])
		names = append(names, string(op.Dst[24:24+length]))
	}
	if strings.Join(names, ",") != ".,..,a,b" {
		t.Fatalf("directory snapshot = %v", names)
	}
	if err := files.SyncFile(ctx, &fuseops.SyncFileOp{Inode: dir.Inode, Handle: dir.Handle}); err != nil {
		t.Fatal(err)
	}
	if err := files.ReleaseDirHandle(ctx, &fuseops.ReleaseDirHandleOp{Handle: dir.Handle}); err != nil {
		t.Fatal(err)
	}
	if err := files.RmDir(ctx, &fuseops.RmDirOp{Parent: fuseops.RootInodeID, Name: "a"}); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("rmdir regular file = %v", err)
	}
	for _, name := range []string{"..", "../outside", "/absolute", "", "bad\x00name"} {
		op := fuseops.CreateFileOp{Parent: fuseops.RootInodeID, Name: name}
		if err := files.CreateFile(ctx, &op); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("create %q = %v", name, err)
		}
	}
}

func TestConcurrentAppend(t *testing.T) {
	ctx := context.Background()
	files := openFS(t, t.TempDir())
	file := create(t, files, fuseops.RootInodeID, "append")
	var group sync.WaitGroup
	for range 8 {
		op := fuseops.OpenFileOp{Inode: file.Entry.Child, OpenFlags: syscall.O_RDWR | syscall.O_APPEND}
		if err := files.OpenFile(ctx, &op); err != nil {
			t.Fatal(err)
		}
		group.Add(1)
		go func() {
			defer group.Done()
			for range 8 {
				if err := files.WriteFile(ctx, &fuseops.WriteFileOp{Inode: op.Inode, Handle: op.Handle, Data: []byte("x")}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	group.Wait()
	if got := read(t, files, file.Entry.Child, file.Handle); got != strings.Repeat("x", 64) {
		t.Fatalf("append lost writes: length=%d", len(got))
	}
}

func TestBackingDirectoryLockAndNamespaceIsolation(t *testing.T) {
	data := t.TempDir()
	files := openFS(t, data)
	if second, err := app.Open(data); err == nil {
		if err := second.Close(); err != nil {
			t.Error(err)
		}
		t.Fatal("second filesystem acquired the same backing directory")
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(data, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := files.LookUpInode(context.Background(), &fuseops.LookUpInodeOp{Parent: fuseops.RootInodeID, Name: "escape"}); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("backing file leaked into metadata namespace: %v", err)
	}
}
