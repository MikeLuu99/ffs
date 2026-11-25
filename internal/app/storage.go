package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"example.com/dfs/internal/chunk"
	"example.com/dfs/internal/metadata/redis"
	"example.com/dfs/internal/storage"
	"example.com/dfs/internal/storage/disk"
	"example.com/dfs/internal/storage/s3"
)

const (
	DefaultS3Endpoint = "https://59f68cb1f4d832c4b78cf873cf2069cb.r2.cloudflarestorage.com"
	DefaultS3Bucket   = "ffs"
)

func configureStorage(config *Config) error {
	if config.Storage == "" {
		config.Storage = "disk"
	}
	switch config.Storage {
	case "disk":
		return nil
	case "s3":
		if config.S3.Endpoint == "" {
			config.S3.Endpoint = DefaultS3Endpoint
		}
		config.S3.Endpoint = strings.TrimSuffix(config.S3.Endpoint, "/")
		if config.S3.Bucket == "" {
			config.S3.Bucket = DefaultS3Bucket
		}
		config.S3.Prefix = "validation"
		_, err := s3.New(config.S3)
		return err
	default:
		return errors.New("storage must be s3 or disk")
	}
}

func openStorage(directory string, config Config, volume manifest, meta *redis.Engine) (storage.Engine, error) {
	if config.Storage == "disk" {
		return disk.Open(filepath.Join(directory, "objects"))
	}
	config.S3.Prefix = "volumes/" + volume.Namespace
	objects, err := s3.New(config.S3)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := objects.Check(ctx); err != nil {
		return nil, fmt.Errorf("access S3 bucket: %w", err)
	}
	if !volume.Ready {
		keys, err := objects.List(ctx)
		if err != nil {
			return nil, err
		}
		if len(keys) != 0 {
			return nil, errors.New("unfinished volume has existing S3 content; refusing to format")
		}
	}
	identity, err := json.Marshal(struct{ Provider, Endpoint, Bucket, Prefix string }{"s3-blocks-v1", config.S3.Endpoint, config.S3.Bucket, config.S3.Prefix})
	if err != nil {
		return nil, err
	}
	if err := meta.BindStorage(string(identity), !volume.Ready); err != nil {
		return nil, err
	}
	return chunk.New(objects, meta)
}
