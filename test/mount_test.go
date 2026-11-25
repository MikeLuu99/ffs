package integration_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"example.com/dfs/internal/app"
	"example.com/dfs/internal/metadata/bolt"
	"example.com/dfs/internal/metadata/redis"
	"example.com/dfs/internal/metadata/redistest"
	"example.com/dfs/internal/storage/memory"
	"example.com/dfs/internal/vfs"
	"example.com/dfs/test/s3test"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
)

func TestMountValidation(t *testing.T) {
	for _, scenario := range []string{"same", "mount-inside-data", "data-inside-mount", "nonempty", "symlink"} {
		t.Run(scenario, func(t *testing.T) {
			base := t.TempDir()
			data, mountpoint := filepath.Join(base, "data"), filepath.Join(base, "mount")
			expected := "overlap"
			switch scenario {
			case "same":
				mountpoint = data
			case "mount-inside-data":
				mountpoint = filepath.Join(data, "mount")
			case "data-inside-mount":
				data = filepath.Join(mountpoint, "data")
			case "nonempty":
				expected = "empty"
			case "symlink":
				if err := os.MkdirAll(data, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(data, mountpoint); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.MkdirAll(mountpoint, 0o700); err != nil {
				t.Fatal(err)
			}
			if scenario == "nonempty" {
				if err := os.WriteFile(filepath.Join(mountpoint, "keep"), []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := app.Mount(data, mountpoint, app.Config{}); err == nil || !strings.Contains(err.Error(), expected) {
				t.Fatalf("Mount error = %v, want %q", err, expected)
			}
			// Validation failures must not hold the backing-directory lock.
			lock, err := os.Open(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				_ = lock.Close()
				t.Fatal(err)
			}
			if err := lock.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMountFailureClosesInjectedEngines(t *testing.T) {
	meta, err := bolt.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	released := false
	files, err := vfs.New(vfs.Config{Metadata: meta, Storage: store, Release: func() error { released = true; return nil }})
	if err != nil {
		_ = meta.Close()
		_ = store.Close()
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	mountpoint := t.TempDir()
	if err := os.WriteFile(filepath.Join(mountpoint, "occupied"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := app.MountFS(files, mountpoint); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("mount nonempty directory = %v", err)
	}
	if !released {
		t.Fatal("failed mount retained its resource lease")
	}
	if err := files.GetInodeAttributes(context.Background(), &fuseops.GetInodeAttributesOp{Inode: fuseops.RootInodeID}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("filesystem survived failed mount: %v", err)
	}
}

func TestMountedInjectedAdapters(t *testing.T) {
	if os.Getenv("DFS_MOUNT_TEST") != "1" {
		t.Skip("set DFS_MOUNT_TEST=1 on a machine with FUSE installed")
	}
	for _, provider := range []string{"memory", "s3"} {
		t.Run(provider, func(t *testing.T) {
			config := redistest.Config(t)
			mountpoint := t.TempDir()
			stop := startMountedServer(t, t.TempDir(), mountpoint, config.URL, config.Namespace, provider)
			mountedIO(t, mountpoint, true)
			stop()
		})
	}
}

func TestMountedFilesystem(t *testing.T) {
	if os.Getenv("DFS_MOUNT_TEST") != "1" {
		t.Skip("set DFS_MOUNT_TEST=1 on a machine with FUSE installed")
	}
	config := redistest.Config(t)
	base := t.TempDir()
	data, mountpoint := filepath.Join(base, "data"), filepath.Join(base, "mount")
	if err := os.Mkdir(mountpoint, 0o700); err != nil {
		t.Fatal(err)
	}
	for iteration := range 2 {
		stop := startMountedServer(t, data, mountpoint, config.URL, config.Namespace, "disk")
		mountedIO(t, mountpoint, iteration == 0)
		stop()
	}
}

// The harness, FUSE daemon, and file-I/O client have separate runtimes. In
// particular unmount/teardown must not depend on the runtime serving the mount.
func startMountedServer(t *testing.T, data, mountpoint, url, namespace, provider string) func() {
	t.Helper()
	readyPath := filepath.Join(t.TempDir(), "ready")
	command := exec.Command(os.Args[0], "-test.run=^TestMountedServer$")
	command.Env = append(os.Environ(), "DFS_MOUNT_SERVER_DATA="+data, "DFS_MOUNT_SERVER_PATH="+mountpoint,
		"DFS_MOUNT_SERVER_READY="+readyPath, "DFS_MOUNT_SERVER_URL="+url,
		"DFS_MOUNT_SERVER_NAMESPACE="+namespace, "DFS_MOUNT_SERVER_PROVIDER="+provider)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	stopped, ready := false, false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		if ready {
			if err := fuse.Unmount(mountpoint); err != nil {
				t.Errorf("unmount: %v", err)
			}
			// Close engines explicitly after unmount. The kernel may retain a
			// blocked device reader until the daemon process exits.
			_ = command.Process.Signal(syscall.SIGTERM)
		} else {
			_ = command.Process.Kill()
		}
		select {
		case err := <-done:
			if ready && err != nil {
				t.Errorf("FUSE daemon: %v\n%s", err, output.String())
			}
		case <-time.After(10 * time.Second):
			_ = command.Process.Signal(syscall.SIGQUIT)
			select {
			case stopErr := <-done:
				t.Errorf("FUSE daemon did not exit after unmount: %v\n%s", stopErr, output.String())
			case <-time.After(5 * time.Second):
				_ = command.Process.Kill()
				t.Error("FUSE daemon could not be stopped")
			}
		}
	}
	t.Cleanup(stop)
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(readyPath); err == nil {
			ready = true
			return stop
		}
		select {
		case err := <-done:
			stopped = true
			t.Fatalf("FUSE daemon failed to start: %v\n%s", err, output.String())
		case <-deadline.C:
			stop()
			t.Fatal("FUSE daemon was not ready in time")
		case <-ticker.C:
		}
	}
}

func TestMountedServer(t *testing.T) {
	mountpoint := os.Getenv("DFS_MOUNT_SERVER_PATH")
	if mountpoint == "" {
		t.Skip("helper process for the FUSE daemon")
	}
	url, namespace := os.Getenv("DFS_MOUNT_SERVER_URL"), os.Getenv("DFS_MOUNT_SERVER_NAMESPACE")
	var files *vfs.FS
	var err error
	if os.Getenv("DFS_MOUNT_SERVER_PROVIDER") == "memory" {
		meta, openErr := redis.Open(redis.Config{URL: url, Namespace: namespace, Create: true})
		if openErr != nil {
			t.Fatal(openErr)
		}
		store := memory.New()
		files, openErr = vfs.New(vfs.Config{Metadata: meta, Storage: store})
		if openErr != nil {
			_ = meta.Close()
			_ = store.Close()
			t.Fatal(openErr)
		}
	} else {
		config := app.Config{RedisURL: url, Namespace: namespace}
		if os.Getenv("DFS_MOUNT_SERVER_PROVIDER") == "s3" {
			config.Storage, config.S3 = "s3", s3test.New().Config("ignored")
		}
		files, err = app.Open(os.Getenv("DFS_MOUNT_SERVER_DATA"), config)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	// Keep handling SIGTERM through the helper process's race-detector exit
	// delay, even if Join completes before the harness sends the signal.
	mounted, err := app.MountFS(files, mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("DFS_MOUNT_SERVER_READY"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- mounted.Join(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-signals:
		if err := mounted.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Keep the client in another process: Go runtime file syscalls must not block
// the same runtime that needs to serve their FUSE requests during a GC pause.
func mountedIO(t *testing.T, mountpoint string, write bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMountedIOClient$")
	mode := "read"
	if write {
		mode = "write"
	}
	command.Env = append(os.Environ(), "DFS_MOUNT_IO_PATH="+mountpoint, "DFS_MOUNT_IO_MODE="+mode)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("mounted client: %v\n%s", err, output)
	}
}

func TestMountedIOClient(t *testing.T) {
	mountpoint := os.Getenv("DFS_MOUNT_IO_PATH")
	if mountpoint == "" {
		t.Skip("helper process for mounted filesystem I/O")
	}
	name := filepath.Join(mountpoint, "saved.txt")
	if os.Getenv("DFS_MOUNT_IO_MODE") == "write" {
		file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.WriteString("persistent")
		if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
			t.Fatal(err)
		}
	}
	content, err := os.ReadFile(name)
	if err != nil || string(content) != "persistent" {
		t.Fatalf("mounted content = %q, %v", content, err)
	}
}
