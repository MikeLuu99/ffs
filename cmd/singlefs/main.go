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

	"example.com/dfs/internal/localfs"

	"github.com/jacobsa/fuse"
)

type options struct {
	data       string
	mountpoint string
}

func parseArgs(args []string, output io.Writer) (options, error) {
	config := options{mountpoint: "./mnt"}
	flags := flag.NewFlagSet("singlefs", flag.ContinueOnError)
	flags.SetOutput(output)
	logger := log.New(output, "", 0)
	flags.StringVar(&config.data, "data", "./data", "persistent backing directory")
	flags.Usage = func() {
		logger.Print("Usage: singlefs [-data directory] [mountpoint] (default ./mnt)")
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
	mounted, err := localfs.Mount(config.data, config.mountpoint)
	if err != nil {
		return err
	}
	logger := log.New(output, "", 0)
	logger.Printf("Mounted singlefs at %s; Ctrl+C to unmount", mounted.Dir())
	stopped := make(chan error, 1)
	go func() { stopped <- mounted.Join(context.Background()) }()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	for {
		select {
		case err := <-stopped:
			return err
		case <-signals:
			if err := fuse.Unmount(mounted.Dir()); err != nil {
				logger.Printf("Unmount failed: %v; close open files and retry Ctrl+C", err)
			}
		}
	}
}

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		log.New(os.Stderr, "", 0).Print(err)
		os.Exit(1)
	}
}
