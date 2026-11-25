package main

import (
	"bytes"
	"strings"
	"testing"

	"example.com/dfs/internal/app"
)

func TestCommandArguments(t *testing.T) {
	t.Setenv("DFS_REDIS_URL", "")
	for _, name := range []string{"DFS_STORAGE", "R2_ENDPOINT", "R2_BUCKET", "R2_REGION"} {
		t.Setenv(name, "")
	}
	var output bytes.Buffer
	if err := run([]string{"-h"}, &output); err != nil || !strings.Contains(output.String(), "Usage:") {
		t.Fatalf("help = %q, %v", output.String(), err)
	}
	for _, test := range []struct {
		name string
		args []string
		want options
	}{
		{name: "defaults", want: options{data: "./data", mountpoint: "./mnt", redisURL: app.DefaultRedisURL}},
		{name: "custom mountpoint", args: []string{"./custom"}, want: options{data: "./data", mountpoint: "./custom", redisURL: app.DefaultRedisURL}},
		{name: "custom paths", args: []string{"-data", "./store", "./custom"}, want: options{data: "./store", mountpoint: "./custom", redisURL: app.DefaultRedisURL}},
		{name: "Redis URL", args: []string{"-redis-url", "redis://example:6379/1"}, want: options{data: "./data", mountpoint: "./mnt", redisURL: "redis://example:6379/1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.want.storage = "s3"
			test.want.endpoint = app.DefaultS3Endpoint
			test.want.bucket = app.DefaultS3Bucket
			test.want.region = "auto"
			got, err := parseArgs(test.args, &output)
			if err != nil || got != test.want {
				t.Fatalf("parseArgs(%v) = %+v, %v; want %+v", test.args, got, err, test.want)
			}
		})
	}
	for _, args := range [][]string{{"one", "two"}, {"-unknown"}} {
		if _, err := parseArgs(args, &output); err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}
}

func TestRedisEnvironmentAndFlagOverride(t *testing.T) {
	t.Setenv("DFS_REDIS_URL", "redis://from-env:6379/1")
	var output bytes.Buffer
	config, err := parseArgs(nil, &output)
	if err != nil || config.redisURL != "redis://from-env:6379/1" {
		t.Fatalf("Redis environment = %+v, %v", config, err)
	}
	config, err = parseArgs([]string{"-redis-url", "redis://explicit:6379/2"}, &output)
	if err != nil || config.redisURL != "redis://explicit:6379/2" {
		t.Fatalf("Redis flag override = %+v, %v", config, err)
	}
}

func TestStorageEnvironmentAndFlagOverride(t *testing.T) {
	t.Setenv("DFS_STORAGE", "disk")
	t.Setenv("R2_ENDPOINT", "https://custom.example")
	t.Setenv("R2_BUCKET", "custom")
	t.Setenv("R2_REGION", "us-east-1")
	var output bytes.Buffer
	config, err := parseArgs(nil, &output)
	if err != nil || config.storage != "disk" || config.endpoint != "https://custom.example" || config.bucket != "custom" || config.region != "us-east-1" {
		t.Fatalf("storage environment = %+v, %v", config, err)
	}
	config, err = parseArgs([]string{"-storage", "s3", "-s3-endpoint", "https://explicit.example", "-s3-bucket", "explicit", "-s3-region", "auto"}, &output)
	if err != nil || config.storage != "s3" || config.endpoint != "https://explicit.example" || config.bucket != "explicit" || config.region != "auto" {
		t.Fatalf("storage flags = %+v, %v", config, err)
	}
	if _, err := parseArgs([]string{"-storage", "unknown"}, &output); err == nil {
		t.Fatal("unknown storage provider accepted")
	}
}
