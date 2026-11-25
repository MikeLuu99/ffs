package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"example.com/dfs/internal/app"
	"example.com/dfs/internal/metadata/redistest"

	"github.com/jacobsa/fuse/fuseops"
	goredis "github.com/redis/go-redis/v9"
)

func TestRedisFilesystemRestart(t *testing.T) {
	redisConfig := redistest.Config(t)
	config := app.Config{RedisURL: redisConfig.URL, Namespace: redisConfig.Namespace}
	data := t.TempDir()
	files, err := app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	if second, err := app.Open(data, config); err == nil {
		_ = second.Close()
		t.Fatal("two mounts acquired the same content directory")
	}
	file := create(t, files, fuseops.RootInodeID, "before")
	write(t, files, file, "Redis metadata", 0)
	if err := files.Rename(context.Background(), &fuseops.RenameOp{OldParent: fuseops.RootInodeID, OldName: "before", NewParent: fuseops.RootInodeID, NewName: "after"}); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	if got := lookup(t, files, fuseops.RootInodeID, "after"); got != file.Entry.Child {
		t.Fatal("Redis inode identity changed after restart")
	}
	opened := fuseops.OpenFileOp{Inode: file.Entry.Child}
	if err := files.OpenFile(context.Background(), &opened); err != nil {
		t.Fatal(err)
	}
	if got := read(t, files, file.Entry.Child, opened.Handle); got != "Redis metadata" {
		t.Fatalf("reopened content = %q", got)
	}
	if _, err := os.Stat(filepath.Join(data, "metadata.db")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Redis filesystem created a local metadata database: %v", err)
	}
}

func TestLostRedisNamespaceDoesNotSweepContent(t *testing.T) {
	redisConfig := redistest.Config(t)
	config := app.Config{RedisURL: redisConfig.URL, Namespace: redisConfig.Namespace}
	data := t.TempDir()
	files, err := app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	file := create(t, files, fuseops.RootInodeID, "keep")
	write(t, files, file, "preserve", 0)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	keys := objectNames(t, data)
	opts, err := goredis.ParseURL(redisConfig.URL)
	if err != nil {
		t.Fatal("invalid test Redis URL")
	}
	client := goredis.NewClient(opts)
	defer func() { _ = client.Close() }()
	// Delete only this test namespace's format record to simulate metadata loss.
	if err := client.Del(context.Background(), "dfs:{"+config.Namespace+"}:state").Err(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := app.Open(data, config); err == nil {
		_ = reopened.Close()
		t.Fatal("missing Redis metadata was reformatted")
	} else if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing namespace error = %v", err)
	}
	if after := objectNames(t, data); len(after) != 1 || after[0] != keys[0] {
		t.Fatalf("metadata loss deleted content: %v", after)
	}
}

func TestRedisInitializationPreservesExistingFiles(t *testing.T) {
	for _, scenario := range []string{"legacy", "missing manifest", "unfinished manifest"} {
		t.Run(scenario, func(t *testing.T) {
			data := t.TempDir()
			filename := filepath.Join(data, "metadata.db")
			if scenario != "legacy" {
				if err := os.Mkdir(filepath.Join(data, "objects"), 0o700); err != nil {
					t.Fatal(err)
				}
				filename = filepath.Join(data, "objects", "0123456789abcdef0123456789abcdef")
			}
			if scenario == "unfinished manifest" {
				encoded, err := json.Marshal(map[string]interface{}{"Version": 1, "Namespace": "test", "Ready": false})
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(data, "volume.json"), encoded, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filename, []byte("preserve"), 0o600); err != nil {
				t.Fatal(err)
			}
			if files, err := app.Open(data, app.Config{}); err == nil {
				_ = files.Close()
				t.Fatal("formatted over existing content")
			}
			if content, err := os.ReadFile(filename); err != nil || strings.TrimSpace(string(content)) != "preserve" {
				t.Fatalf("existing data = %q, %v", content, err)
			}
		})
	}
}
