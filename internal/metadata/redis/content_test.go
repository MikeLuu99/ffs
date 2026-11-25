package redis_test

import (
	"errors"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/metadata/redis"
	"example.com/dfs/internal/metadata/redistest"
)

func TestContentLayoutTransactions(t *testing.T) {
	engine, err := redis.Open(redistest.Config(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	key := "0123456789abcdef0123456789abcdef"
	block := "abcdef0123456789abcdef0123456789"
	if err := engine.PutLayout(key, 0, metadata.Layout{Blocks: map[uint64]string{}}); err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			err := engine.PutLayout(key, 1, metadata.Layout{Size: 3, Blocks: map[uint64]string{0: block}})
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, metadata.ErrConflict) {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if winners.Load() != 1 {
		t.Fatalf("CAS winners = %d", winners.Load())
	}
	layout, err := engine.GetLayout(key)
	if err != nil || layout.Revision != 2 || layout.Size != 3 || layout.Blocks[0] != block {
		t.Fatalf("layout = %+v, %v", layout, err)
	}
	if err := engine.PutLayout(key, 2, metadata.Layout{Size: 3, Blocks: map[uint64]string{0: block}, Retired: []string{block}}); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("retiring live block = %v", err)
	}
	if err := engine.DeleteLayout(key, 1); !errors.Is(err, metadata.ErrConflict) {
		t.Fatalf("stale delete = %v", err)
	}
	if keys, err := engine.LayoutKeys(); err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("keys = %v, %v", keys, err)
	}
	if err := engine.DeleteLayout(key, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.GetLayout(key); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("deleted layout = %v", err)
	}
}

func TestStorageBinding(t *testing.T) {
	config := redistest.Config(t)
	engine, err := redis.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if err := engine.BindStorage("r2:bucket:prefix", false); err == nil {
		t.Fatal("missing binding accepted")
	}
	if err := engine.BindStorage("r2:bucket:prefix", true); err != nil {
		t.Fatal(err)
	}
	if err := engine.BindStorage("r2:bucket:prefix", false); err != nil {
		t.Fatal(err)
	}
	if err := engine.BindStorage("r2:other:prefix", true); err == nil {
		t.Fatal("storage mismatch accepted")
	}
}
