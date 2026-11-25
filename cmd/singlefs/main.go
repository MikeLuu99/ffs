package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"example.com/dfs/internal/localfs"

	"github.com/jacobsa/fuse"
)

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("singlefs", flag.ContinueOnError)
	flags.SetOutput(output)
	logger := log.New(output, "", 0)
	data := flags.String("data", "./data", "persistent backing directory")
	flags.Usage = func() {
		logger.Print("Usage: singlefs [-data directory] mountpoint")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 1 {
		flags.Usage()
		return errors.New("provide exactly one mountpoint")
	}
	mounted, err := localfs.Mount(*data, flags.Arg(0))
	if err != nil {
		return err
	}
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
