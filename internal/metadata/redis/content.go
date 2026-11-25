package redis

import (
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"math"
	"sort"
	"time"

	"example.com/dfs/internal/metadata"

	goredis "github.com/redis/go-redis/v9"
)

var _ metadata.ContentEngine = (*Engine)(nil)

func (e *Engine) contentsKey() string { return e.prefix + ":contents" }

func readLayout(tx *transaction, key string) (metadata.Layout, error) {
	encoded, err := tx.client.HGet(tx.ctx, tx.engine.contentsKey(), key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return metadata.Layout{}, fs.ErrNotExist
	}
	if err != nil {
		return metadata.Layout{}, err
	}
	var layout metadata.Layout
	if err := json.Unmarshal(encoded, &layout); err != nil {
		return layout, err
	}
	return layout, layout.Validate()
}

func (e *Engine) GetLayout(key string) (layout metadata.Layout, err error) {
	if !metadata.ValidContentKey(key) {
		return layout, fs.ErrInvalid
	}
	err = e.run(false, func(tx *transaction) error {
		var err error
		layout, err = readLayout(tx, key)
		return err
	})
	return layout, err
}

func (e *Engine) PutLayout(key string, revision uint64, layout metadata.Layout) error {
	if !metadata.ValidContentKey(key) || revision == math.MaxUint64 {
		return fs.ErrInvalid
	}
	layout.Revision = revision + 1
	if err := layout.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(layout)
	if err != nil {
		return err
	}
	return e.run(true, func(tx *transaction) error {
		current, err := readLayout(tx, key)
		if errors.Is(err, fs.ErrNotExist) {
			if revision != 0 {
				return fs.ErrNotExist
			}
		} else if err != nil {
			return err
		} else if current.Revision != revision {
			return metadata.ErrConflict
		}
		if current.Inode != layout.Inode {
			return fs.ErrInvalid
		}
		if layout.Inode != 0 && (current.Size != layout.Size || !maps.Equal(current.Blocks, layout.Blocks)) {
			inode, err := get(tx, layout.Inode)
			if err != nil {
				return err
			}
			if inode.Object != key {
				return fs.ErrInvalid
			}
			inode.Size = layout.Size
			inode.Mtime, inode.Ctime = time.Now().UTC(), time.Now().UTC()
			if err := put(tx, inode); err != nil {
				return err
			}
		}
		tx.contents = map[string][]byte{key: encoded}
		return nil
	})
}

func (e *Engine) DeleteLayout(key string, revision uint64) error {
	if !metadata.ValidContentKey(key) {
		return fs.ErrInvalid
	}
	return e.run(true, func(tx *transaction) error {
		current, err := readLayout(tx, key)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if current.Revision != revision {
			return metadata.ErrConflict
		}
		tx.contents = map[string][]byte{key: nil}
		return nil
	})
}

func (e *Engine) LayoutKeys() (keys []string, err error) {
	err = e.run(false, func(tx *transaction) error {
		var err error
		keys, err = tx.client.HKeys(tx.ctx, e.contentsKey()).Result()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if !metadata.ValidContentKey(key) {
				return fs.ErrInvalid
			}
		}
		return nil
	})
	sort.Strings(keys)
	return keys, err
}

// BindStorage prevents reopening a metadata namespace against another bucket,
// prefix, or endpoint. Only an empty namespace can acquire a new binding.
func (e *Engine) BindStorage(identity string, create bool) error {
	if identity == "" {
		return fs.ErrInvalid
	}
	return e.run(true, func(tx *transaction) error {
		binding, err := tx.client.HGet(tx.ctx, e.stateKey(), "storage").Result()
		if err == nil {
			if binding != identity {
				return errors.New("storage does not match Redis volume binding")
			}
			return nil
		}
		if !errors.Is(err, goredis.Nil) {
			return err
		}
		if !create {
			return errors.New("redis storage binding is missing")
		}
		inodes, err := tx.client.HLen(tx.ctx, e.inodesKey()).Result()
		if err != nil {
			return err
		}
		contents, err := tx.client.HLen(tx.ctx, e.contentsKey()).Result()
		if err != nil {
			return err
		}
		if inodes != 1 || contents != 0 {
			return errors.New("refusing to bind storage to nonempty Redis metadata")
		}
		tx.storageBinding = identity
		return nil
	})
}
