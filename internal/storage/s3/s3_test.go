package s3_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"

	"example.com/dfs/internal/storage/s3"
	"example.com/dfs/test/s3test"
)

func TestS3ObjectOperationsAndIsolation(t *testing.T) {
	server := s3test.New()
	store, err := s3.New(server.Config("volumes/one"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Check(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if err := store.Put(ctx, fmt.Sprintf("%032x", i), []byte("abcdef")); err != nil {
			t.Fatal(err)
		}
	}
	key := fmt.Sprintf("%032x", 0)
	if err := store.Put(ctx, key, []byte("overwrite")); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("mutable block accepted: %v", err)
	}
	if size, err := store.Head(ctx, key); err != nil || size != 6 {
		t.Fatalf("head = %d, %v", size, err)
	}
	body, err := store.Get(ctx, key, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(body)
	if err := errors.Join(readErr, body.Close()); err != nil || string(data) != "cde" {
		t.Fatalf("range read = %q, %v", data, err)
	}
	other, err := s3.New(server.Config("volumes/two"))
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Put(ctx, key, []byte("other volume")); err != nil {
		t.Fatal(err)
	}
	if keys, err := store.List(ctx); err != nil || len(keys) != 5 {
		t.Fatalf("paginated list = %v, %v", keys, err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(ctx, key); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing object = %v", err)
	}
	if size, err := other.Head(ctx, key); err != nil || size != 12 {
		t.Fatalf("another volume was touched: %d, %v", size, err)
	}
	server.FailWrites(true)
	if err := store.Put(ctx, fmt.Sprintf("%032x", 6), []byte("x")); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("permission error = %v", err)
	}
	if err := store.Delete(ctx, "../outside"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("invalid key = %v", err)
	}
}

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRejectIgnoredRangesAndInvalidPagination(t *testing.T) {
	config := s3test.New().Config("volume")
	config.HTTPClient = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		body := "abcdef"
		header := make(http.Header)
		header.Set("Content-Length", "6")
		if r.URL.Query().Get("list-type") == "2" {
			body = "<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"
			header.Set("Content-Type", "application/xml")
		}
		return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: r}, nil
	})}
	store, err := s3.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if body, err := store.Get(context.Background(), strings.Repeat("a", 32), 2, 2); err == nil {
		_ = body.Close()
		t.Fatal("server ignored Range but read succeeded")
	}
	if _, err := store.List(context.Background()); err == nil {
		t.Fatal("truncated listing without continuation token accepted")
	}
}

func TestRejectInvalidConfiguration(t *testing.T) {
	for _, change := range []func(*s3.Config){
		func(c *s3.Config) { c.Prefix = "" },
		func(c *s3.Config) { c.Prefix = "../outside" },
		func(c *s3.Config) { c.Endpoint = "https://account.r2.cloudflarestorage.com/ffs" },
		func(c *s3.Config) { c.Endpoint = "https://key:secret@example.com" },
		func(c *s3.Config) { c.SecretAccessKey = "" },
	} {
		config := s3test.New().Config("volumes/test")
		change(&config)
		if _, err := s3.New(config); err == nil {
			t.Fatal("invalid S3 configuration accepted")
		}
	}
}
