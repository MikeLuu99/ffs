package chunk

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"math"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"
)

type file struct {
	engine *Engine
	key    string
	access storage.Access
	closed bool
}

func (f *file) layout() (metadata.Layout, error) {
	if f.closed {
		return metadata.Layout{}, fs.ErrClosed
	}
	layout, err := f.engine.meta.GetLayout(f.key)
	if err != nil {
		return layout, err
	}
	if err := layout.Validate(); err != nil {
		return layout, err
	}
	if layout.Blocks == nil {
		layout.Blocks = make(map[uint64]string)
	}
	return layout, nil
}

func (f *file) Size() (uint64, error) {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	layout, err := f.layout()
	return layout.Size, err
}

// readBlock returns the stored bytes; sparse regions are filled by the caller.
func (f *file) readBlock(key string) ([]byte, error) {
	size, err := f.engine.objects.Head(context.Background(), key)
	if err != nil {
		return nil, err
	}
	if size <= 0 || size > metadata.BlockSize {
		return nil, fs.ErrInvalid
	}
	body, err := f.engine.objects.Get(context.Background(), key, 0, size)
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	_, readErr := io.ReadFull(body, data)
	return data, errors.Join(readErr, body.Close())
}

func (f *file) ReadAt(dst []byte, offset int64) (int, error) {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	layout, err := f.layout()
	if err != nil {
		return 0, err
	}
	if f.access == storage.WriteOnly {
		return 0, fs.ErrPermission
	}
	if offset < 0 {
		return 0, fs.ErrInvalid
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if uint64(offset) >= layout.Size {
		return 0, io.EOF
	}
	want := min(uint64(len(dst)), layout.Size-uint64(offset))
	n := 0
	for uint64(n) < want {
		pos := uint64(offset) + uint64(n)
		index, within := pos/metadata.BlockSize, pos%metadata.BlockSize
		count := min(want-uint64(n), metadata.BlockSize-within)
		part := dst[n : n+int(count)]
		clear(part)
		if key := layout.Blocks[index]; key != "" {
			size, err := f.engine.objects.Head(context.Background(), key)
			if err != nil {
				return n, err
			}
			if size <= 0 || size > metadata.BlockSize {
				return n, fs.ErrInvalid
			}
			if within < uint64(size) {
				length := min(count, uint64(size)-within)
				body, err := f.engine.objects.Get(context.Background(), key, int64(within), int64(length))
				if err != nil {
					return n, err
				}
				_, readErr := io.ReadFull(body, part[:length])
				if err := errors.Join(readErr, body.Close()); err != nil {
					return n, err
				}
			}
		}
		n += int(count)
	}
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

func (f *file) replace(layout *metadata.Layout, index uint64, data []byte) error {
	key, err := newKey()
	if err != nil {
		return err
	}
	if err := f.engine.objects.Put(context.Background(), key, data); err != nil {
		return err
	}
	if old := layout.Blocks[index]; old != "" {
		layout.Retired = append(layout.Retired, old)
	}
	layout.Blocks[index] = key
	return nil
}

func (f *file) WriteAt(src []byte, offset int64) (int, error) {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	layout, err := f.layout()
	if err != nil {
		return 0, err
	}
	if f.access == storage.ReadOnly {
		return 0, fs.ErrPermission
	}
	if offset < 0 {
		return 0, fs.ErrInvalid
	}
	if int64(len(src)) > math.MaxInt64-offset {
		return 0, metadata.ErrTooLarge
	}
	if len(src) == 0 {
		return 0, nil
	}
	for n := 0; n < len(src); {
		pos := uint64(offset) + uint64(n)
		index, within := pos/metadata.BlockSize, pos%metadata.BlockSize
		count := min(uint64(len(src)-n), metadata.BlockSize-within)
		var previous []byte
		if key := layout.Blocks[index]; key != "" && (within != 0 || count != metadata.BlockSize) {
			previous, err = f.readBlock(key)
			if err != nil {
				return 0, err
			}
		}
		data := make([]byte, max(uint64(len(previous)), within+count))
		copy(data, previous)
		copy(data[within:], src[n:n+int(count)])
		if err := f.replace(&layout, index, data); err != nil {
			return 0, err
		}
		n += int(count)
	}
	layout.Size = max(layout.Size, uint64(offset)+uint64(len(src)))
	// Never delete new uploads on an error: Redis may have committed before a
	// connection failure. Unreferenced uploads are safe to retain for later GC.
	if err := f.engine.meta.PutLayout(f.key, layout.Revision, layout); err != nil {
		return 0, err
	}
	return len(src), nil
}

func (f *file) Truncate(size int64) error {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	layout, err := f.layout()
	if err != nil {
		return err
	}
	if f.access == storage.ReadOnly {
		return fs.ErrPermission
	}
	if size < 0 {
		return fs.ErrInvalid
	}
	if uint64(size) == layout.Size {
		return nil
	}
	if uint64(size) < layout.Size {
		for index, key := range layout.Blocks {
			if index*metadata.BlockSize >= uint64(size) {
				layout.Retired = append(layout.Retired, key)
				delete(layout.Blocks, index)
			}
		}
		index, tail := uint64(size)/metadata.BlockSize, uint64(size)%metadata.BlockSize
		if key := layout.Blocks[index]; tail != 0 && key != "" {
			data, err := f.readBlock(key)
			if err != nil {
				return err
			}
			if uint64(len(data)) > tail {
				if err := f.replace(&layout, index, data[:tail]); err != nil {
					return err
				}
			}
		}
	}
	layout.Size = uint64(size)
	return f.engine.meta.PutLayout(f.key, layout.Revision, layout)
}

func (f *file) Sync() error {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	layout, err := f.layout()
	if err != nil {
		return err
	}
	if len(layout.Retired) == 0 {
		return nil
	}
	for _, key := range layout.Retired {
		if err := f.engine.objects.Delete(context.Background(), key); err != nil {
			return err
		}
	}
	layout.Retired = nil
	return f.engine.meta.PutLayout(f.key, layout.Revision, layout)
}

func (f *file) Close() error {
	f.engine.mu.Lock()
	defer f.engine.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	f.engine.open[f.key]--
	if f.engine.open[f.key] == 0 {
		delete(f.engine.open, f.key)
		if f.engine.removed[f.key] {
			return f.engine.remove(f.key)
		}
	}
	return nil
}
