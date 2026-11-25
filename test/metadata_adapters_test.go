package integration_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/metadata/bolt"
	"example.com/dfs/internal/metadata/redis"
	"example.com/dfs/internal/metadata/redistest"
)

// Each metadata adapter must satisfy these namespace and atomicity contracts.
func TestMetadataAdapters(t *testing.T) {
	for name, open := range map[string]func(*testing.T) metadata.Engine{
		"redis": func(t *testing.T) metadata.Engine {
			engine, err := redis.Open(redistest.Config(t))
			if err != nil {
				t.Fatal(err)
			}
			return engine
		},
		"bolt": func(t *testing.T) metadata.Engine {
			engine, err := bolt.Open(filepath.Join(t.TempDir(), "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			return engine
		},
	} {
		t.Run(name, func(t *testing.T) {
			for scenario, check := range map[string]func(*testing.T, metadata.Engine){
				"rename atomicity":                 checkMetadataRename,
				"concurrent create and validation": checkMetadataCreate,
			} {
				t.Run(scenario, func(t *testing.T) {
					engine := open(t)
					t.Cleanup(func() {
						if err := engine.Close(); err != nil {
							t.Error(err)
						}
					})
					check(t, engine)
				})
			}
		})
	}
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

func checkMetadataRename(t *testing.T, engine metadata.Engine) {
	parent := createEntry(t, engine, metadata.Root, "parent", true)
	child := createEntry(t, engine, parent.ID, "child", true)
	if _, err := engine.Rename(metadata.Root, "parent", child.ID, "loop"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("rename into descendant = %v", err)
	}
	if inode, err := engine.Lookup(metadata.Root, "parent"); err != nil || inode.ID != parent.ID {
		t.Fatalf("failed rename changed namespace: %+v, %v", inode, err)
	}
	source := createEntry(t, engine, metadata.Root, "source", false)
	target := createEntry(t, engine, metadata.Root, "target", false)
	if _, err := engine.Rename(metadata.Root, "source", metadata.Root, "parent"); !errors.Is(err, metadata.ErrIsDirectory) {
		t.Fatalf("file over directory = %v", err)
	}
	replaced, err := engine.Rename(metadata.Root, "source", metadata.Root, "target")
	if err != nil || replaced == nil || replaced.ID != target.ID || replaced.Nlink != 0 {
		t.Fatalf("replacement = %+v, %v", replaced, err)
	}
	if inode, err := engine.Lookup(metadata.Root, "target"); err != nil || inode.ID != source.ID {
		t.Fatalf("renamed identity = %+v, %v", inode, err)
	}
	if _, err := engine.Lookup(metadata.Root, "source"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old name = %v", err)
	}
	if inode, err := engine.Get(target.ID); err != nil || inode.Nlink != 0 {
		t.Fatalf("retained replacement = %+v, %v", inode, err)
	}
	if err := engine.Delete(target.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Remove(metadata.Root, "parent", true); !errors.Is(err, metadata.ErrNotEmpty) {
		t.Fatalf("remove nonempty = %v", err)
	}
	other := createEntry(t, engine, metadata.Root, "other", true)
	if _, err := engine.Rename(metadata.Root, "other", metadata.Root, "parent"); !errors.Is(err, metadata.ErrNotEmpty) {
		t.Fatalf("replace nonempty = %v", err)
	}
	if _, err := engine.Rename(parent.ID, "child", other.ID, "moved"); err != nil {
		t.Fatal(err)
	}
	if inode, err := engine.Get(child.ID); err != nil || inode.Parent != other.ID {
		t.Fatalf("moved directory parent = %+v, %v", inode, err)
	}
	if inode, err := engine.Get(parent.ID); err != nil || inode.Nlink != 2 {
		t.Fatalf("old directory link count = %+v, %v", inode, err)
	}
	if inode, err := engine.Get(other.ID); err != nil || inode.Nlink != 3 {
		t.Fatalf("new directory link count = %+v, %v", inode, err)
	}
}

func checkMetadataCreate(t *testing.T, engine metadata.Engine) {
	var group sync.WaitGroup
	for i := range 12 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := engine.Create(metadata.Root, fmt.Sprintf("file-%02d", i), 0o600, 1, 1, "key"); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	entries, err := engine.ReadDir(metadata.Root)
	if err != nil || len(entries) != 12 {
		t.Fatalf("entries = %v, %v", entries, err)
	}
	seen := make(map[uint64]bool)
	for i, entry := range entries {
		if entry.Name != fmt.Sprintf("file-%02d", i) || seen[entry.Inode.ID] {
			t.Fatalf("unsorted directory or reused identity: %+v", entry)
		}
		seen[entry.Inode.ID] = true
	}
	if _, err := engine.Create(metadata.Root, "file-00", 0o600, 1, 1, "other-key"); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("duplicate = %v", err)
	}
	for _, name := range []string{"", ".", "..", "../escape", "a/b", "a\x00b"} {
		if _, err := engine.Create(metadata.Root, name, 0o600, 1, 1, "key"); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("create %q = %v", name, err)
		}
	}
	removed, err := engine.Remove(metadata.Root, "file-00", false)
	if err != nil || removed.Nlink != 0 {
		t.Fatalf("remove = %+v, %v", removed, err)
	}
	if _, err := engine.Lookup(metadata.Root, "file-00"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lookup after unlink = %v", err)
	}
	if _, err := engine.Get(removed.ID); err != nil {
		t.Fatal("unlink discarded a potentially open inode:", err)
	}
}
