// Package redis implements metadata.Engine with Redis hashes and optimistic
// transactions. All mutations update a watched namespace revision. Reads also
// validate that revision, so directory snapshots never mix committed versions.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"example.com/dfs/internal/metadata"

	goredis "github.com/redis/go-redis/v9"
)

// Config selects an isolated metadata namespace. Create authorizes formatting
// only a new namespace; reopen existing filesystems with Create false.
// Redis must have AOF enabled, appendfsync always, and maxmemory-policy noeviction.
// The URL supports Redis authentication, database selection, and rediss TLS.
type Config struct {
	URL       string
	Namespace string
	Create    bool
	Timeout   time.Duration
}

// Engine stores inodes and directory entries separately. It serializes commits
// with WATCH/MULTI/EXEC rather than storing an entire filesystem in one value.
type Engine struct {
	client  *goredis.Client
	prefix  string
	timeout time.Duration
	closed  atomic.Bool
}

var _ metadata.Engine = (*Engine)(nil)

// Open connects, verifies durability settings, and opens or creates a namespace.
func Open(config Config) (*Engine, error) {
	if !ValidNamespace(config.Namespace) {
		return nil, fs.ErrInvalid
	}
	opts, err := goredis.ParseURL(config.URL)
	if err != nil {
		return nil, errors.New("invalid Redis URL")
	}
	if config.Timeout <= 0 {
		config.Timeout = 5 * time.Second
	}
	opts.DialTimeout, opts.ReadTimeout, opts.WriteTimeout = config.Timeout, config.Timeout, config.Timeout
	opts.ContextTimeoutEnabled = true
	// A network error after EXEC has an ambiguous outcome. Only explicit WATCH
	// conflicts are safe to retry; disable automatic command/network retries.
	opts.MaxRetries = -1
	engine := &Engine{client: goredis.NewClient(opts), prefix: "dfs:{" + config.Namespace + "}", timeout: config.Timeout}
	ctx, cancel := context.WithTimeout(context.Background(), engine.timeout)
	defer cancel()
	for key, want := range map[string]string{"appendonly": "yes", "appendfsync": "always", "maxmemory-policy": "noeviction"} {
		values, err := engine.client.ConfigGet(ctx, key).Result()
		if err != nil {
			return nil, errors.Join(fmt.Errorf("verify Redis persistence settings: %w", err), engine.Close())
		}
		if values[key] != want {
			return nil, errors.Join(fmt.Errorf("redis requires %s %s", key, want), engine.Close())
		}
	}
	if err := engine.initialize(ctx, config.Create); err != nil {
		return nil, errors.Join(err, engine.Close())
	}
	return engine, nil
}

// ValidNamespace reports whether a namespace can safely form Redis hash tags
// and namespace-specific scan patterns.
func ValidNamespace(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, c := range name {
		if c != '-' && c != '_' && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func (e *Engine) stateKey() string        { return e.prefix + ":state" }
func (e *Engine) inodesKey() string       { return e.prefix + ":inodes" }
func (e *Engine) dirKey(id uint64) string { return e.prefix + ":dir:" + strconv.FormatUint(id, 10) }

func (e *Engine) initialize(ctx context.Context, create bool) error {
	return e.retry(ctx, func(client *goredis.Tx) error {
		state, err := client.HGetAll(ctx, e.stateKey()).Result()
		if err != nil {
			return err
		}
		if len(state) != 0 {
			if state["version"] != "1" {
				return errors.New("unsupported Redis metadata format")
			}
			value, err := client.HGet(ctx, e.inodesKey(), strconv.FormatUint(metadata.Root, 10)).Result()
			if err != nil {
				return fmt.Errorf("redis metadata root is missing: %w", err)
			}
			var root metadata.Inode
			if err := json.Unmarshal([]byte(value), &root); err != nil {
				return err
			}
			if root.ID != metadata.Root || !root.Mode.IsDir() || root.Nlink == 0 {
				return errors.New("invalid Redis metadata root")
			}
			_, err = client.TxPipelined(ctx, func(pipe goredis.Pipeliner) error { pipe.Ping(ctx); return nil })
			return err
		}
		if !create {
			return fmt.Errorf("redis metadata namespace is missing: %w", fs.ErrNotExist)
		}
		count, err := client.HLen(ctx, e.inodesKey()).Result()
		if err != nil {
			return err
		}
		if count != 0 {
			return errors.New("redis metadata state is missing alongside existing inodes")
		}
		now := time.Now().UTC()
		root := metadata.Inode{ID: metadata.Root, Parent: metadata.Root, Mode: os.ModeDir | 0o755, Nlink: 2,
			UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Atime: now, Mtime: now, Ctime: now, Btime: now}
		value, err := json.Marshal(root)
		if err != nil {
			return err
		}
		_, err = client.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
			pipe.HSet(ctx, e.stateKey(), "version", "1", "sequence", metadata.Root, "revision", 0)
			pipe.HSet(ctx, e.inodesKey(), strconv.FormatUint(metadata.Root, 10), value)
			return nil
		})
		return err
	})
}

func (e *Engine) retry(ctx context.Context, operation func(*goredis.Tx) error) error {
	for attempt := 0; attempt < 128; attempt++ {
		err := e.client.Watch(ctx, operation, e.stateKey())
		if !errors.Is(err, goredis.TxFailedErr) {
			return adapterError(err)
		}
		timer := time.NewTimer(min(time.Duration(attempt+1)*time.Millisecond, 10*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("redis metadata transaction exceeded conflict retry limit")
}

func (e *Engine) run(write bool, operation func(*transaction) error) error {
	if e.closed.Load() {
		return fs.ErrClosed
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	return e.retry(ctx, func(client *goredis.Tx) error {
		version, err := client.HGet(ctx, e.stateKey(), "version").Result()
		if errors.Is(err, goredis.Nil) {
			return fmt.Errorf("redis metadata namespace is missing: %w", fs.ErrNotExist)
		}
		if err != nil {
			return err
		}
		if version != "1" {
			return errors.New("unsupported Redis metadata format")
		}
		tx := &transaction{engine: e, client: client, ctx: ctx, inodes: make(map[uint64]*metadata.Inode), dirs: make(map[uint64]map[string]uint64)}
		operationErr := operation(tx)
		// Validate even unsuccessful reads: a concurrent rename may temporarily
		// make separate Redis reads disagree. A failed callback never commits.
		if operationErr != nil || !write {
			_, err := client.TxPipelined(ctx, func(pipe goredis.Pipeliner) error { pipe.Ping(ctx); return nil })
			if err != nil {
				return err
			}
			return operationErr
		}
		return tx.commit()
	})
}

// Sync is a revision-validated barrier; Redis acknowledges our transactions
// after its AOF fsync under the required appendfsync always configuration.
func (e *Engine) Sync() error { return e.run(false, func(*transaction) error { return nil }) }

// Close closes this client's connections, without deleting its metadata.
func (e *Engine) Close() error {
	if e.closed.Swap(true) {
		return nil
	}
	return e.client.Close()
}

func adapterError(err error) error {
	if errors.Is(err, goredis.ErrClosed) {
		return fs.ErrClosed
	}
	return err
}
