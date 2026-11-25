// Package chunk implements offset-based file IO using immutable object blocks
// and atomically committed metadata layouts. One engine owns a volume at a time.
package chunk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"
)

// Engine adapts an object provider and a layout provider to storage.Engine.
// Providers remain caller-owned, including after Close, so open files remain
// usable until individually closed. Methods serialize within this client.
type Engine struct {
	mu      sync.Mutex
	objects storage.ObjectStore
	meta    metadata.ContentEngine
	open    map[string]int
	removed map[string]bool
	closed  bool
}

var _ storage.Engine = (*Engine)(nil)

func New(objects storage.ObjectStore, meta metadata.ContentEngine) (*Engine, error) {
	if objects == nil || meta == nil {
		return nil, fs.ErrInvalid
	}
	return &Engine{objects: objects, meta: meta, open: make(map[string]int), removed: make(map[string]bool)}, nil
}

func newKey() (string, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return hex.EncodeToString(id[:]), err
}

func (e *Engine) Create() (string, storage.File, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return "", nil, fs.ErrClosed
	}
	key, err := newKey()
	if err != nil {
		return "", nil, err
	}
	if err := e.meta.PutLayout(key, 0, metadata.Layout{Blocks: make(map[uint64]string)}); err != nil {
		return "", nil, err
	}
	e.open[key]++
	return key, &file{engine: e, key: key, access: storage.ReadWrite}, nil
}

func (e *Engine) Open(key string, access storage.Access) (storage.File, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fs.ErrClosed
	}
	if !metadata.ValidContentKey(key) || access > storage.ReadWrite {
		return nil, fs.ErrInvalid
	}
	if e.removed[key] {
		return nil, fs.ErrNotExist
	}
	if _, err := e.meta.GetLayout(key); err != nil {
		return nil, err
	}
	e.open[key]++
	return &file{engine: e, key: key, access: access}, nil
}

func (e *Engine) Remove(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fs.ErrClosed
	}
	if !metadata.ValidContentKey(key) {
		return fs.ErrInvalid
	}
	e.removed[key] = true
	if e.open[key] != 0 {
		return nil
	}
	return e.remove(key)
}

func (e *Engine) remove(key string) error {
	layout, err := e.meta.GetLayout(key)
	if errors.Is(err, fs.ErrNotExist) {
		delete(e.removed, key)
		return nil
	}
	if err != nil {
		return err
	}
	// Keep the record until every delete succeeds, so startup can retry after
	// interrupted unlink. No open reader can still reference these blocks.
	for _, object := range layout.Blocks {
		if err := e.objects.Delete(context.Background(), object); err != nil {
			return err
		}
	}
	for _, object := range layout.Retired {
		if err := e.objects.Delete(context.Background(), object); err != nil {
			return err
		}
	}
	if err := e.meta.DeleteLayout(key, layout.Revision); err != nil {
		return err
	}
	delete(e.removed, key)
	return nil
}

func (e *Engine) Keys() ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fs.ErrClosed
	}
	keys, err := e.meta.LayoutKeys()
	if err != nil {
		return nil, err
	}
	visible := keys[:0]
	for _, key := range keys {
		if !e.removed[key] {
			visible = append(visible, key)
		}
	}
	return visible, nil
}

// Verify checks live references without reclaiming anything. Startup must finish
// validating all live files before it attempts orphan or tombstone cleanup.
func (e *Engine) Verify(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fs.ErrClosed
	}
	layout, err := e.meta.GetLayout(key)
	if err != nil {
		return err
	}
	if err := layout.Validate(); err != nil {
		return err
	}
	for index, object := range layout.Blocks {
		size, err := e.objects.Head(context.Background(), object)
		if err != nil {
			return fmt.Errorf("block %d: %w", index, err)
		}
		if size <= 0 || uint64(size) > min(uint64(metadata.BlockSize), layout.Size-index*metadata.BlockSize) {
			return fs.ErrInvalid
		}
	}
	return nil
}

func (e *Engine) Sync() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fs.ErrClosed
	}
	// Mutations upload all blocks and commit their layouts synchronously.
	return nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return nil
}
