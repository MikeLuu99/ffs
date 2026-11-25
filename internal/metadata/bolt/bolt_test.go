package bolt_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/metadata/bolt"
)

func openMetadata(t *testing.T, filename string) *bolt.Engine {
	t.Helper()
	engine, err := bolt.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	})
	return engine
}

func createEntry(t *testing.T, engine metadata.Engine, parent uint64, name string, directory bool) metadata.Inode {
	t.Helper()
	mode, key := os.FileMode(0o640), "content-"+name
	if directory {
		mode, key = os.ModeDir|0o750, ""
	}
	inode, err := engine.Create(parent, name, mode, 123, 456, key)
	if err != nil {
		t.Fatal(err)
	}
	return inode
}

func TestMetadataPersistenceAndIdentity(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "metadata.db")
	engine := openMetadata(t, filename)
	dir := createEntry(t, engine, metadata.Root, "docs", true)
	file := createEntry(t, engine, dir.ID, "file", false)
	size, mode, mtime := uint64(4096), os.FileMode(0o600), time.Unix(1_700_000_000, 123)
	if _, err := engine.SetAttributes(file.ID, metadata.Attributes{Size: &size, Mode: &mode, Mtime: &mtime}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Rename(metadata.Root, "docs", metadata.Root, "notes"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine = openMetadata(t, filename)
	reopened, err := engine.Lookup(metadata.Root, "notes")
	if err != nil || reopened.ID != dir.ID {
		t.Fatalf("directory identity = %+v, %v", reopened, err)
	}
	got, err := engine.Lookup(dir.ID, "file")
	if err != nil || got.ID != file.ID || got.Object != file.Object || got.Size != size || got.Mode != mode || !got.Mtime.Equal(mtime) || got.UID != 123 || got.GID != 456 {
		t.Fatalf("persisted attributes = %+v, %v", got, err)
	}
	newFile := createEntry(t, engine, metadata.Root, "new", false)
	if newFile.ID <= file.ID {
		t.Fatal("inode sequence was reused after restart")
	}
	if err := engine.Delete(file.ID); !errors.Is(err, metadata.ErrBusy) {
		t.Fatalf("delete linked inode = %v", err)
	}
	if err := engine.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestClosedDatabaseUsesContractError(t *testing.T) {
	engine := openMetadata(t, filepath.Join(t.TempDir(), "metadata.db"))
	file := createEntry(t, engine, metadata.Root, "file", false)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"get":            func() error { _, err := engine.Get(file.ID); return err },
		"lookup":         func() error { _, err := engine.Lookup(metadata.Root, "file"); return err },
		"list directory": func() error { _, err := engine.ReadDir(metadata.Root); return err },
		"create":         func() error { _, err := engine.Create(metadata.Root, "new", 0o600, 0, 0, "key"); return err },
		"attributes":     func() error { _, err := engine.SetAttributes(file.ID, metadata.Attributes{}); return err },
		"rename":         func() error { _, err := engine.Rename(metadata.Root, "file", metadata.Root, "new"); return err },
		"remove":         func() error { _, err := engine.Remove(metadata.Root, "file", false); return err },
		"delete":         func() error { return engine.Delete(file.ID) },
		"list":           func() error { _, err := engine.List(); return err },
		"sync":           engine.Sync,
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("closed adapter error = %v", err)
			}
		})
	}
}
