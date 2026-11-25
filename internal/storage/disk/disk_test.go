package disk_test

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"example.com/dfs/internal/storage"
	diskadapter "example.com/dfs/internal/storage/disk"
)

func TestDiskPersistenceAndSparseWrites(t *testing.T) {
	directory := t.TempDir()
	disk, err := diskadapter.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	key, file, err := disk.Create()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("tail"), 4); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(file.Sync(), file.Close(), disk.Close()); err != nil {
		t.Fatal(err)
	}
	disk, err = diskadapter.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	file, err = disk.Open(key, storage.ReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	buffer := make([]byte, 16)
	n, err := file.ReadAt(buffer, 0)
	if !errors.Is(err, io.EOF) || string(buffer[:n]) != "\x00\x00\x00\x00tail" {
		t.Fatalf("read = %q, %v", buffer[:n], err)
	}
	if err := file.Truncate(2); err != nil {
		t.Fatal(err)
	}
	if size, err := file.Size(); err != nil || size != 2 {
		t.Fatalf("size = %d, %v", size, err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if keys, err := disk.Keys(); err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("keys = %v, %v", keys, err)
	}
	if err := disk.Remove(key); err != nil {
		t.Fatal(err)
	}
	// Deleting the object name does not invalidate an existing descriptor.
	if _, err := file.WriteAt([]byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if err := disk.Remove(key); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.Open(key, storage.ReadOnly); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open removed object = %v", err)
	}
}

func TestDiskRejectsInvalidKeysAndSymlinks(t *testing.T) {
	directory := t.TempDir()
	disk, err := diskadapter.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = disk.Close() }()
	for _, key := range []string{"", "../outside", "/etc/passwd", "................................"} {
		if _, err := disk.Open(key, storage.ReadOnly); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("open %q = %v", key, err)
		}
		if err := disk.Remove(key); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("remove %q = %v", key, err)
		}
	}
	key := "0123456789abcdef0123456789abcdef"
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(directory, key)); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.Open(key, storage.ReadOnly); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("symlink open = %v", err)
	}
	if _, err := disk.Keys(); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("symlink listing = %v", err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := disk.Create(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("create after close = %v", err)
	}
}
