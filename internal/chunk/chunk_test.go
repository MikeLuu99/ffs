package chunk_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"slices"
	"testing"

	"example.com/dfs/internal/chunk"
	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"
	"example.com/dfs/internal/storage/memory"
)

type layouts struct {
	values map[string]metadata.Layout
	fail   bool
}

func clone(l metadata.Layout) metadata.Layout {
	l.Blocks = maps.Clone(l.Blocks)
	l.Retired = slices.Clone(l.Retired)
	return l
}
func (m *layouts) GetLayout(key string) (metadata.Layout, error) {
	l, ok := m.values[key]
	if !ok {
		return l, fs.ErrNotExist
	}
	return clone(l), nil
}
func (m *layouts) PutLayout(key string, revision uint64, l metadata.Layout) error {
	if m.fail {
		return errors.New("injected metadata failure")
	}
	if m.values[key].Revision != revision {
		return metadata.ErrConflict
	}
	l.Revision = revision + 1
	m.values[key] = clone(l)
	return nil
}
func (m *layouts) DeleteLayout(key string, revision uint64) error {
	if l, ok := m.values[key]; ok && l.Revision != revision {
		return metadata.ErrConflict
	}
	delete(m.values, key)
	return nil
}
func (m *layouts) LayoutKeys() ([]string, error) { return slices.Collect(maps.Keys(m.values)), nil }

type failingObjects struct {
	storage.ObjectStore
	puts       int
	failAt     int
	failDelete bool
}

func (s *failingObjects) Put(ctx context.Context, key string, data []byte) error {
	s.puts++
	if s.puts == s.failAt {
		return errors.New("injected upload failure")
	}
	return s.ObjectStore.Put(ctx, key, data)
}
func (s *failingObjects) Delete(ctx context.Context, key string) error {
	if s.failDelete {
		return errors.New("injected delete failure")
	}
	return s.ObjectStore.Delete(ctx, key)
}

func fixture(t *testing.T) (*chunk.Engine, *layouts, *failingObjects, string, storage.File) {
	t.Helper()
	m := &layouts{values: make(map[string]metadata.Layout)}
	o := &failingObjects{ObjectStore: memory.NewObjects()}
	e, err := chunk.New(o, m)
	if err != nil {
		t.Fatal(err)
	}
	key, f, err := e.Create()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close(); _ = e.Close() })
	return e, m, o, key, f
}

func TestBlockBoundariesOverwriteAndTruncate(t *testing.T) {
	e, m, objects, key, f := fixture(t)
	if _, err := f.WriteAt([]byte("abcdef"), metadata.BlockSize-3); err != nil {
		t.Fatal(err)
	}
	l, _ := m.GetLayout(key)
	if len(l.Blocks) != 2 {
		t.Fatalf("blocks = %v", l.Blocks)
	}
	first := l.Blocks[0]
	if _, err := f.WriteAt([]byte("X"), metadata.BlockSize+1); err != nil {
		t.Fatal(err)
	}
	l, _ = m.GetLayout(key)
	if l.Blocks[0] != first {
		t.Fatal("overwrite rewrote an untouched block")
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if keys, err := objects.List(context.Background()); err != nil || len(keys) != 2 {
		t.Fatalf("superseded objects remain: %v, %v", keys, err)
	}
	if err := f.Truncate(metadata.BlockSize + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(metadata.BlockSize + 3); err != nil {
		t.Fatal(err)
	}
	reader, err := e.Open(key, storage.ReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	dst := make([]byte, 8)
	n, err := reader.ReadAt(dst, metadata.BlockSize-4)
	if !errors.Is(err, io.EOF) || n != 7 || !bytes.Equal(dst[:n], []byte{0, 'a', 'b', 'c', 'd', 0, 0}) {
		t.Fatalf("read = %q, %d, %v", dst, n, err)
	}
}

func TestFailedUploadsAndMetadataPreserveCommittedData(t *testing.T) {
	for _, scenario := range []string{"upload", "metadata"} {
		t.Run(scenario, func(t *testing.T) {
			_, m, objects, key, f := fixture(t)
			if _, err := f.WriteAt([]byte("original"), 0); err != nil {
				t.Fatal(err)
			}
			before, _ := m.GetLayout(key)
			if scenario == "upload" {
				objects.failAt = objects.puts + 2
			} else {
				m.fail = true
			}
			if n, err := f.WriteAt(bytes.Repeat([]byte("x"), metadata.BlockSize+1), 0); err == nil || n != 0 {
				t.Fatalf("failed write = %d, %v", n, err)
			}
			after, _ := m.GetLayout(key)
			if after.Revision != before.Revision || after.Size != before.Size || !maps.Equal(after.Blocks, before.Blocks) {
				t.Fatal("failed write published a partial layout")
			}
			dst := make([]byte, 8)
			if _, err := f.ReadAt(dst, 0); err != nil || string(dst) != "original" {
				t.Fatalf("previous data = %q, %v", dst, err)
			}
		})
	}
}

func TestFailedCleanupRemainsRetryable(t *testing.T) {
	e, m, objects, key, f := fixture(t)
	if _, err := f.WriteAt([]byte("old"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	objects.failDelete = true
	if err := f.Sync(); err == nil {
		t.Fatal("delete failure ignored")
	}
	l, _ := m.GetLayout(key)
	if len(l.Retired) != 1 {
		t.Fatal("cleanup queue was lost")
	}
	objects.failDelete = false
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := e.Remove(key); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Open(key, storage.ReadOnly); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("reopened removed file: %v", err)
	}
	if _, err := f.WriteAt([]byte("!"), 3); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 4)
	if _, err := f.ReadAt(dst, 0); err != nil || string(dst) != "new!" {
		t.Fatalf("open lifetime = %q, %v", dst, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetLayout(key); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("unlinked layout retained: %v", err)
	}
}

func TestSparseLargeFileAndInvalidOffsets(t *testing.T) {
	_, _, objects, _, f := fixture(t)
	const offset = int64(1 << 40)
	if _, err := f.WriteAt([]byte("x"), offset); err != nil {
		t.Fatal(err)
	}
	keys, _ := objects.List(context.Background())
	if len(keys) != 1 {
		t.Fatalf("sparse allocation = %d", len(keys))
	}
	dst := []byte{1, 1}
	if _, err := f.ReadAt(dst, offset-1); err != nil || !bytes.Equal(dst, []byte{0, 'x'}) {
		t.Fatalf("sparse read = %v, %v", dst, err)
	}
	if n, err := f.WriteAt([]byte("x"), -1); n != 0 || !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("invalid write = %d, %v", n, err)
	}
	if err := f.Truncate(-1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("invalid truncate = %v", err)
	}
}
