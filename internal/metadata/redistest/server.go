// Package redistest supplies isolated namespaces on an explicitly configured
// real Redis server. It never flushes a database or touches other namespaces.
package redistest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"example.com/dfs/internal/metadata/redis"

	goredis "github.com/redis/go-redis/v9"
)

// Config skips unless DFS_REDIS_TEST_URL names a disposable test-capable Redis
// with the adapter's required durability settings. Cleanup removes only this
// test's randomly generated namespace, leaving other databases and keys intact.
func Config(t testing.TB) redis.Config {
	t.Helper()
	url := os.Getenv("DFS_REDIS_TEST_URL")
	if url == "" {
		t.Skip("set DFS_REDIS_TEST_URL to run real Redis integration tests")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	namespace := "test-" + hex.EncodeToString(id[:])
	opts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal("invalid DFS_REDIS_TEST_URL")
	}
	client := goredis.NewClient(opts)
	t.Cleanup(func() {
		defer func() { _ = client.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var keys []string
		iterator := client.Scan(ctx, 0, "dfs:{"+namespace+"}:*", 100).Iterator()
		for key := range iterator.Vals(ctx) {
			keys = append(keys, key)
		}
		if err := iterator.Err(); err != nil {
			t.Error(err)
			return
		}
		if len(keys) > 0 {
			if err := client.Unlink(ctx, keys...).Err(); err != nil {
				t.Error(err)
			}
		}
	})
	return redis.Config{URL: url, Namespace: namespace, Create: true, Timeout: 10 * time.Second}
}
