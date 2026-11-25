package memory

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"sort"
	"sync"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"
)

// Objects is a volatile immutable-block provider for offline tests.
type Objects struct {
	mu   sync.Mutex
	data map[string][]byte
}

var _ storage.ObjectStore = (*Objects)(nil)

func NewObjects() *Objects { return &Objects{data: make(map[string][]byte)} }

func (o *Objects) Put(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !metadata.ValidContentKey(key) {
		return fs.ErrInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, exists := o.data[key]; exists {
		return fs.ErrExist
	}
	o.data[key] = bytes.Clone(data)
	return nil
}

func (o *Objects) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	data, exists := o.data[key]
	if !exists {
		return nil, fs.ErrNotExist
	}
	if offset < 0 || length < 0 || offset > int64(len(data)) || length > int64(len(data))-offset {
		return nil, fs.ErrInvalid
	}
	return io.NopCloser(bytes.NewReader(data[offset : offset+length])), nil
}

func (o *Objects) Head(ctx context.Context, key string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	data, exists := o.data[key]
	if !exists {
		return 0, fs.ErrNotExist
	}
	return int64(len(data)), nil
}

func (o *Objects) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.data, key)
	return nil
}

func (o *Objects) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	keys := make([]string, 0, len(o.data))
	for key := range o.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}
