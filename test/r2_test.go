package integration_test

import (
	"context"
	"os"
	"testing"

	"example.com/dfs/internal/app"
	"example.com/dfs/internal/metadata/redistest"
	"example.com/dfs/internal/storage/s3"

	"github.com/jacobsa/fuse/fuseops"
)

// TestR2Filesystem is explicitly opt-in and touches only a random test prefix.
func TestR2Filesystem(t *testing.T) {
	if os.Getenv("DFS_R2_TEST") != "1" {
		t.Skip("set DFS_R2_TEST=1 for live R2 IO")
	}
	rc := redistest.Config(t)
	config := app.Config{RedisURL: rc.URL, Namespace: rc.Namespace, Storage: "s3", S3: s3.Config{
		Endpoint: os.Getenv("R2_ENDPOINT"), Bucket: os.Getenv("R2_BUCKET"), Region: os.Getenv("R2_REGION"),
		AccessKeyID: os.Getenv("R2_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("R2_SECRET_ACCESS_KEY"),
	}}
	if config.S3.Endpoint == "" {
		config.S3.Endpoint = app.DefaultS3Endpoint
	}
	if config.S3.Bucket == "" {
		config.S3.Bucket = app.DefaultS3Bucket
	}
	config.S3.Prefix = "volumes/" + rc.Namespace
	objects, err := s3.New(config.S3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys, err := objects.List(context.Background())
		if err != nil {
			t.Error(err)
			return
		}
		for _, key := range keys {
			if err := objects.Delete(context.Background(), key); err != nil {
				t.Error(err)
			}
		}
	})
	data := t.TempDir()
	files, err := app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	})
	file := create(t, files, fuseops.RootInodeID, "r2-test")
	write(t, files, file, "R2 persistence", 0)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = app.Open(data, config)
	if err != nil {
		t.Fatal(err)
	}
	id := lookup(t, files, fuseops.RootInodeID, "r2-test")
	opened := fuseops.OpenFileOp{Inode: id}
	if err := files.OpenFile(context.Background(), &opened); err != nil {
		t.Fatal(err)
	}
	if got := read(t, files, id, opened.Handle); got != "R2 persistence" {
		t.Fatalf("live R2 read = %q", got)
	}
}
