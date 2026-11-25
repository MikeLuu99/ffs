package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"example.com/dfs/internal/app"
	"example.com/dfs/internal/storage/s3"

	"github.com/jacobsa/fuse"
)

type options struct {
	data       string
	mountpoint string
	redisURL   string
	storage    string
	endpoint   string
	bucket     string
	region     string
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func parseArgs(args []string, output io.Writer) (options, error) {
	config := options{mountpoint: "./mnt"}
	flags := flag.NewFlagSet("dfs", flag.ContinueOnError)
	flags.SetOutput(output)
	logger := log.New(output, "", 0)
	flags.StringVar(&config.data, "data", "./data", "persistent backing directory")
	redisURL := os.Getenv("DFS_REDIS_URL")
	if redisURL == "" {
		redisURL = app.DefaultRedisURL
	}
	flags.StringVar(&config.redisURL, "redis-url", redisURL, "Redis metadata URL (or DFS_REDIS_URL)")
	flags.StringVar(&config.storage, "storage", envDefault("DFS_STORAGE", "s3"), "content provider: s3 or disk (or DFS_STORAGE)")
	flags.StringVar(&config.endpoint, "s3-endpoint", envDefault("R2_ENDPOINT", app.DefaultS3Endpoint), "S3 account endpoint without bucket path (or R2_ENDPOINT)")
	flags.StringVar(&config.bucket, "s3-bucket", envDefault("R2_BUCKET", app.DefaultS3Bucket), "existing S3 bucket (or R2_BUCKET)")
	flags.StringVar(&config.region, "s3-region", envDefault("R2_REGION", "auto"), "S3 region (or R2_REGION)")
	flags.Usage = func() {
		logger.Print("Usage: dfs [-data directory] [-redis-url URL] [mountpoint] (default ./mnt)")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() > 1 {
		flags.Usage()
		return options{}, errors.New("provide at most one mountpoint")
	}
	if flags.NArg() == 1 {
		config.mountpoint = flags.Arg(0)
	}
	if config.storage != "s3" && config.storage != "disk" {
		return options{}, errors.New("storage must be s3 or disk")
	}
	return config, nil
}

func run(args []string, output io.Writer) error {
	config, err := parseArgs(args, output)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if err := os.MkdirAll(config.mountpoint, 0o755); err != nil {
		return fmt.Errorf("create mountpoint: %w", err)
	}
	mounted, err := app.Mount(config.data, config.mountpoint, app.Config{
		RedisURL: config.redisURL, Storage: config.storage,
		S3: s3.Config{Endpoint: config.endpoint, Bucket: config.bucket, Region: config.region,
			AccessKeyID:     envDefault("R2_ACCESS_KEY_ID", os.Getenv("AWS_ACCESS_KEY_ID")),
			SecretAccessKey: envDefault("R2_SECRET_ACCESS_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY"))},
	})
	if err != nil {
		return err
	}
	logger := log.New(output, "", 0)
	logger.Printf("Mounted dfs at %s; Ctrl+C to unmount", mounted.Dir())
	stopped := make(chan error, 1)
	go func() { stopped <- mounted.Join(context.Background()) }()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	for {
		select {
		case err := <-stopped:
			return errors.Join(err, mounted.Close())
		case <-signals:
			if err := fuse.Unmount(mounted.Dir()); err != nil {
				logger.Printf("Unmount failed: %v; close open files and retry Ctrl+C", err)
				continue
			}
			return mounted.Close()
		}
	}
}

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		log.New(os.Stderr, "", 0).Print(err)
		os.Exit(1)
	}
}
