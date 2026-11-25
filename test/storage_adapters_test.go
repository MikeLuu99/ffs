package integration_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"sync"
	"testing"

	"example.com/dfs/internal/chunk"
	"example.com/dfs/internal/metadata/redis"
	"example.com/dfs/internal/metadata/redistest"
	"example.com/dfs/internal/storage"
	"example.com/dfs/internal/storage/disk"
	"example.com/dfs/internal/storage/memory"
)

// Every storage adapter runs the same contract tests. Add providers to this map
// to verify that VFS can use them without changing filesystem operations.
func TestStorageAdapters(t *testing.T) {
	for name, open := range map[string]func(*testing.T) storage.Engine{
		"chunk": func(t *testing.T) storage.Engine {
			meta, err := redis.Open(redistest.Config(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = meta.Close() })
			store, err := chunk.New(memory.NewObjects(), meta)
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
		"disk": func(t *testing.T) storage.Engine {
			store, err := disk.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
		"memory": func(*testing.T) storage.Engine { return memory.New() },
	} {
		t.Run(name, func(t *testing.T) {
			for scenario, check := range map[string]func(*testing.T, storage.Engine){
				"sparse IO and truncate":   checkSparseIO,
				"access and open lifetime": checkAccessAndLifetime,
				"concurrent handles":       checkConcurrentHandles,
				"engine close":             checkEngineClose,
			} {
				t.Run(scenario, func(t *testing.T) {
					store := open(t)
					t.Cleanup(func() {
						if err := store.Close(); err != nil {
							t.Error(err)
						}
					})
					check(t, store)
				})
			}
		})
	}
}

func contentObject(t *testing.T, store storage.Engine) (string, storage.File) {
	t.Helper()
	key, file, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return key, file
}

func checkSparseIO(t *testing.T, store storage.Engine) {
	t.Helper()
	key, file := contentObject(t, store)
	if n, err := file.WriteAt([]byte("abcdef"), 4094); err != nil || n != 6 {
		t.Fatalf("write = %d, %v", n, err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if size, err := file.Size(); err != nil || size != 4100 {
		t.Fatalf("size = %d, %v", size, err)
	}
	buffer := bytes.Repeat([]byte{0xff}, 16)
	if n, err := file.ReadAt(buffer, 4090); !errors.Is(err, io.EOF) || n != 10 || string(buffer[:n]) != "\x00\x00\x00\x00abcdef" {
		t.Fatalf("sparse read = %q, %d, %v", buffer, n, err)
	}
	if err := file.Truncate(4097); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(4100); err != nil {
		t.Fatal(err)
	}
	if n, err := file.ReadAt(buffer[:6], 4094); err != nil || n != 6 || string(buffer[:6]) != "abc\x00\x00\x00" {
		t.Fatalf("truncate and extend = %q, %d, %v", buffer[:6], n, err)
	}
	if n, err := file.WriteAt([]byte("x"), -1); !errors.Is(err, fs.ErrInvalid) || n != 0 {
		t.Fatalf("negative write = %d, %v", n, err)
	}
	if err := file.Truncate(-1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("negative truncate = %v", err)
	}
	if keys, err := store.Keys(); err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("keys = %v, %v", keys, err)
	}
}

func checkAccessAndLifetime(t *testing.T, store storage.Engine) {
	t.Helper()
	key, file := contentObject(t, store)
	reader, err := store.Open(key, storage.ReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	if _, err := reader.WriteAt([]byte("x"), 0); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("read-only write = %v", err)
	}
	if err := reader.Truncate(0); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("read-only truncate = %v", err)
	}
	writer, err := store.Open(key, storage.WriteOnly)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := writer.ReadAt(make([]byte, 1), 0); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("write-only read = %v", err)
	}
	if _, err := writer.WriteAt([]byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(key); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(key); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	if n, err := file.ReadAt(buffer, 0); err != nil || n != 1 || buffer[0] != 'x' {
		t.Fatalf("unlinked open file = %q, %d, %v", buffer, n, err)
	}
	if _, err := store.Open(key, storage.ReadOnly); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open removed object = %v", err)
	}
}

func checkConcurrentHandles(t *testing.T, store storage.Engine) {
	t.Helper()
	key, file := contentObject(t, store)
	var group sync.WaitGroup
	for i := range 12 {
		group.Add(1)
		go func() {
			defer group.Done()
			handle, err := store.Open(key, storage.ReadWrite)
			if err != nil {
				t.Error(err)
				return
			}
			_, writeErr := handle.WriteAt([]byte{byte(i + 1)}, int64(i))
			if err := errors.Join(writeErr, handle.Sync(), handle.Close()); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	buffer := make([]byte, 12)
	if _, err := file.ReadAt(buffer, 0); err != nil {
		t.Fatal(err)
	}
	for i, value := range buffer {
		if value != byte(i+1) {
			t.Fatalf("write at %d was lost: %v", i, buffer)
		}
	}
}

func checkEngineClose(t *testing.T, store storage.Engine) {
	t.Helper()
	key, file := contentObject(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Create(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("create after close = %v", err)
	}
	if _, err := store.Open(key, storage.ReadOnly); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("open after close = %v", err)
	}
	if err := store.Sync(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("sync after close = %v", err)
	}
	// Engine ownership and file ownership are separate for both providers.
	if _, err := file.WriteAt([]byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Size(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("size after file close = %v", err)
	}
}
