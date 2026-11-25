package redis_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"testing"
	"time"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/metadata/redis"
	"example.com/dfs/internal/metadata/redistest"
)

func openEngine(t *testing.T, config redis.Config) *redis.Engine {
	t.Helper()
	engine, err := redis.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func TestReopenAndFailedCommit(t *testing.T) {
	config := redistest.Config(t)
	engine := openEngine(t, config)
	file, err := engine.Create(metadata.Root, "file", 0o600, 123, 456, "object")
	if err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1_700_000_000, 0)
	size, mode := uint64(1234), os.FileMode(0o640)
	if _, err := engine.SetAttributes(file.ID, metadata.Attributes{Size: &size, Mode: &mode, Mtime: &mtime}); err != nil {
		t.Fatal(err)
	}
	invalid := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := engine.SetAttributes(file.ID, metadata.Attributes{Mtime: &invalid}); err == nil {
		t.Fatal("invalid timestamp committed")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	config.Create = false
	engine = openEngine(t, config)
	got, err := engine.Lookup(metadata.Root, "file")
	if err != nil || got.ID != file.ID || got.Object != file.Object || got.Size != size || got.Mode != mode || !got.Mtime.Equal(mtime) || got.UID != 123 || got.GID != 456 {
		t.Fatalf("reopened attributes = %+v, %v", got, err)
	}
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

func TestConcurrentClients(t *testing.T) {
	config := redistest.Config(t)
	first := openEngine(t, config)
	config.Create = false
	second := openEngine(t, config)
	var group sync.WaitGroup
	for i := range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			engine := first
			if i%2 == 0 {
				engine = second
			}
			if _, err := engine.Create(metadata.Root, fmt.Sprintf("file-%02d", i), 0o600, 1, 1, "key"); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	entries, err := first.ReadDir(metadata.Root)
	if err != nil || len(entries) != 16 {
		t.Fatalf("concurrent entries = %v, %v", entries, err)
	}
	seen := make(map[uint64]bool)
	for _, entry := range entries {
		if seen[entry.Inode.ID] {
			t.Fatal("concurrent clients reused an inode ID")
		}
		seen[entry.Inode.ID] = true
	}
}

func TestMissingNamespaceDoesNotInitialize(t *testing.T) {
	config := redistest.Config(t)
	config.Create = false
	if engine, err := redis.Open(config); !errors.Is(err, fs.ErrNotExist) {
		if engine != nil {
			_ = engine.Close()
		}
		t.Fatalf("missing namespace = %v", err)
	}
}
