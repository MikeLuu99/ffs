// Package app composes metadata/storage adapters and the filesystem mount.
package app

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"

	"example.com/dfs/internal/metadata/redis"
	"example.com/dfs/internal/storage/s3"
	"example.com/dfs/internal/vfs"
)

// DefaultRedisURL is the loopback port published by the Docker Compose service.
const DefaultRedisURL = "redis://127.0.0.1:16379/0"

// Config selects providers. An omitted Storage retains the disk adapter for
// embedded callers; the command explicitly defaults to S3. Namespace is normally
// generated and stored in the volume manifest; overrides are useful in tests.
type Config struct {
	RedisURL  string
	Namespace string
	Storage   string
	S3        s3.Config
}

type manifest struct {
	Version   int
	Namespace string
	Ready     bool
	Storage   string
	Endpoint  string
	Bucket    string
}

// Open composes Redis metadata with disk or chunked S3 content. A durable manifest
// binds the provider configuration to a Redis namespace. Once ready, missing Redis
// namespace is an error; it must never trigger formatting or orphan collection.
func Open(directory string, config Config) (*vfs.FS, error) {
	if err := configureStorage(&config); err != nil {
		return nil, err
	}
	if config.Namespace != "" && !redis.ValidNamespace(config.Namespace) {
		return nil, fs.ErrInvalid
	}
	if config.RedisURL == "" {
		config.RedisURL = DefaultRedisURL
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	lock, err := root.Open(".")
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	release := func() error { return errors.Join(lock.Close(), root.Close()) }
	fail := func(err error) (*vfs.FS, error) { return nil, errors.Join(err, release()) }
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(err)
	}
	entries, err := lock.ReadDir(-1)
	if err != nil {
		return fail(err)
	}
	for _, entry := range entries {
		if entry.Name() == "objects" && entry.IsDir() {
			continue
		}
		if (entry.Name() == "volume.json" || entry.Name() == "volume.json.tmp") && entry.Type().IsRegular() {
			continue
		}
		return fail(fmt.Errorf("unrecognized backing entry %q; choose an empty -data directory (existing files are untouched)", entry.Name()))
	}
	var volume manifest
	encoded, err := root.ReadFile("volume.json")
	if errors.Is(err, os.ErrNotExist) {
		if err := requireEmptyObjects(root); err != nil {
			return fail(err)
		}
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return fail(err)
		}
		namespace := hex.EncodeToString(id[:])
		if config.Namespace != "" {
			namespace = config.Namespace
		}
		volume = manifest{Version: 2, Namespace: namespace, Storage: config.Storage}
		if config.Storage == "s3" {
			volume.Endpoint, volume.Bucket = config.S3.Endpoint, config.S3.Bucket
		}
		if err := writeManifest(root, lock, volume); err != nil {
			return fail(err)
		}
	} else if err != nil {
		return fail(err)
	} else {
		if err := json.Unmarshal(encoded, &volume); err != nil {
			return fail(err)
		}
		if (volume.Version != 1 && volume.Version != 2) || !redis.ValidNamespace(volume.Namespace) {
			return fail(errors.New("unsupported or corrupt volume manifest"))
		}
		if config.Namespace != "" && config.Namespace != volume.Namespace {
			return fail(errors.New("redis namespace does not match the volume manifest"))
		}
		provider := volume.Storage
		if volume.Version == 1 {
			provider = "disk"
		}
		if provider != config.Storage || provider == "s3" && (volume.Endpoint != config.S3.Endpoint || volume.Bucket != config.S3.Bucket) {
			return fail(errors.New("storage configuration does not match volume manifest; use the original provider or a fresh -data directory"))
		}
	}
	if !volume.Ready {
		if err := requireEmptyObjects(root); err != nil {
			return fail(err)
		}
	}
	meta, err := redis.Open(redis.Config{URL: config.RedisURL, Namespace: volume.Namespace, Create: !volume.Ready})
	if err != nil {
		return fail(fmt.Errorf("open Redis metadata: %w", err))
	}
	store, err := openStorage(directory, config, volume, meta)
	if err != nil {
		return fail(errors.Join(err, meta.Close()))
	}
	if !volume.Ready {
		volume.Ready = true
		if err := writeManifest(root, lock, volume); err != nil {
			return fail(errors.Join(err, store.Close(), meta.Close()))
		}
	}
	files, err := vfs.New(vfs.Config{Metadata: meta, Storage: store, Release: release})
	if err != nil {
		return fail(errors.Join(err, store.Close(), meta.Close()))
	}
	return files, nil
}

func requireEmptyObjects(root *os.Root) error {
	objects, err := root.Open("objects")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	entries, readErr := objects.ReadDir(-1)
	if err := errors.Join(readErr, objects.Close()); err != nil {
		return err
	}
	if len(entries) > 0 {
		return errors.New("volume manifest is missing or unfinished alongside existing content; refusing to format")
	}
	return nil
}

func writeManifest(root *os.Root, dir *os.File, volume manifest) error {
	encoded, err := json.Marshal(volume)
	if err != nil {
		return err
	}
	file, err := root.OpenFile("volume.json.tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(encoded)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := root.Rename("volume.json.tmp", "volume.json"); err != nil {
		return err
	}
	return dir.Sync()
}
