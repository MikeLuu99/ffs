package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"strconv"
	"strings"
	"time"

	"example.com/dfs/internal/metadata"

	goredis "github.com/redis/go-redis/v9"
)

type transaction struct {
	engine         *Engine
	client         *goredis.Tx
	ctx            context.Context
	inodes         map[uint64]*metadata.Inode
	dirs           map[uint64]map[string]uint64
	sequence       *uint64
	contents       map[string][]byte
	storageBinding string
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return fs.ErrInvalid
	}
	if len(name) > 255 {
		return metadata.ErrNameTooLong
	}
	return nil
}

func get(tx *transaction, id uint64) (metadata.Inode, error) {
	if inode, ok := tx.inodes[id]; ok {
		if inode == nil {
			return metadata.Inode{}, fs.ErrNotExist
		}
		return *inode, nil
	}
	value, err := tx.client.HGet(tx.ctx, tx.engine.inodesKey(), strconv.FormatUint(id, 10)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return metadata.Inode{}, fs.ErrNotExist
	}
	if err != nil {
		return metadata.Inode{}, err
	}
	var inode metadata.Inode
	if err := json.Unmarshal(value, &inode); err != nil {
		return metadata.Inode{}, err
	}
	if inode.ID != id {
		return metadata.Inode{}, fmt.Errorf("invalid inode identity %d", id)
	}
	return inode, nil
}

func put(tx *transaction, inode metadata.Inode) error { tx.inodes[inode.ID] = &inode; return nil }

func directory(tx *transaction, id uint64) (metadata.Inode, error) {
	inode, err := get(tx, id)
	if err != nil {
		return metadata.Inode{}, err
	}
	if inode.Nlink == 0 {
		return metadata.Inode{}, fs.ErrNotExist
	}
	if !inode.Mode.IsDir() {
		return metadata.Inode{}, metadata.ErrNotDirectory
	}
	return inode, nil
}

func lookup(tx *transaction, parent uint64, name string) (metadata.Inode, error) {
	if id, ok := tx.dirs[parent][name]; ok {
		if id == 0 {
			return metadata.Inode{}, fs.ErrNotExist
		}
		return get(tx, id)
	}
	value, err := tx.client.HGet(tx.ctx, tx.engine.dirKey(parent), name).Result()
	if errors.Is(err, goredis.Nil) {
		return metadata.Inode{}, fs.ErrNotExist
	}
	if err != nil {
		return metadata.Inode{}, err
	}
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 {
		return metadata.Inode{}, errors.New("invalid Redis directory entry")
	}
	return get(tx, id)
}

func (tx *transaction) setEntry(parent uint64, name string, id uint64) {
	if tx.dirs[parent] == nil {
		tx.dirs[parent] = make(map[string]uint64)
	}
	tx.dirs[parent][name] = id
}

func (tx *transaction) entries(id uint64) (map[string]uint64, error) {
	values, err := tx.client.HGetAll(tx.ctx, tx.engine.dirKey(id)).Result()
	if err != nil {
		return nil, err
	}
	entries := make(map[string]uint64, len(values))
	for name, value := range values {
		child, err := strconv.ParseUint(value, 10, 64)
		if err != nil || child == 0 {
			return nil, errors.New("invalid Redis directory entry")
		}
		entries[name] = child
	}
	for name, child := range tx.dirs[id] {
		if child == 0 {
			delete(entries, name)
		} else {
			entries[name] = child
		}
	}
	return entries, nil
}

func empty(tx *transaction, id uint64) (bool, error) {
	entries, err := tx.entries(id)
	return len(entries) == 0, err
}

func touch(tx *transaction, id uint64, links int) error {
	inode, err := get(tx, id)
	if err != nil {
		return err
	}
	inode.Nlink = uint32(int64(inode.Nlink) + int64(links))
	inode.Mtime, inode.Ctime = time.Now().UTC(), time.Now().UTC()
	return put(tx, inode)
}

func (tx *transaction) nextID() (uint64, error) {
	if tx.sequence == nil {
		value, err := tx.client.HGet(tx.ctx, tx.engine.stateKey(), "sequence").Result()
		if err != nil {
			return 0, err
		}
		sequence, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, err
		}
		tx.sequence = &sequence
	}
	if *tx.sequence == math.MaxUint64 {
		return 0, metadata.ErrTooLarge
	}
	*tx.sequence = *tx.sequence + 1
	return *tx.sequence, nil
}

func (tx *transaction) commit() error {
	// Marshal before queueing any commands. Callback/encoding errors cannot
	// accidentally leave part of a namespace mutation in Redis.
	records := make(map[string]interface{})
	var deleted []string
	for id, inode := range tx.inodes {
		key := strconv.FormatUint(id, 10)
		if inode == nil {
			deleted = append(deleted, key)
			continue
		}
		value, err := json.Marshal(inode)
		if err != nil {
			return err
		}
		records[key] = value
	}
	_, err := tx.client.TxPipelined(tx.ctx, func(pipe goredis.Pipeliner) error {
		for key, value := range tx.contents {
			if value == nil {
				pipe.HDel(tx.ctx, tx.engine.contentsKey(), key)
			} else {
				pipe.HSet(tx.ctx, tx.engine.contentsKey(), key, value)
			}
		}
		if tx.storageBinding != "" {
			pipe.HSet(tx.ctx, tx.engine.stateKey(), "storage", tx.storageBinding)
		}
		if len(records) > 0 {
			pipe.HSet(tx.ctx, tx.engine.inodesKey(), records)
		}
		if len(deleted) > 0 {
			pipe.HDel(tx.ctx, tx.engine.inodesKey(), deleted...)
		}
		for parent, entries := range tx.dirs {
			for name, id := range entries {
				if id == 0 {
					pipe.HDel(tx.ctx, tx.engine.dirKey(parent), name)
				} else {
					pipe.HSet(tx.ctx, tx.engine.dirKey(parent), name, id)
				}
			}
		}
		if tx.sequence != nil {
			pipe.HSet(tx.ctx, tx.engine.stateKey(), "sequence", *tx.sequence)
		}
		pipe.HIncrBy(tx.ctx, tx.engine.stateKey(), "revision", 1)
		return nil
	})
	return err
}
