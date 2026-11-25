package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"example.com/dfs/internal/app"
	"example.com/dfs/internal/metadata/bolt"
	"example.com/dfs/internal/storage/memory"
	"example.com/dfs/internal/vfs"

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
			if _, err := app.Mount(data, mountpoint); err == nil || !strings.Contains(err.Error(), expected) {
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
	meta, err := bolt.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	files, err := vfs.New(vfs.Config{Metadata: meta, Storage: store})
	if err != nil {
		_ = meta.Close()
		_ = store.Close()
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	mountpoint := t.TempDir()
	mounted, err := app.MountFS(files, mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fuse.Unmount(mountpoint); err != nil {
			t.Error(err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := mounted.Join(ctx); err != nil {
			t.Error(err)
		}
	}()
	filename := filepath.Join(mountpoint, "memory-backed")
	if err := os.WriteFile(filename, []byte("composed mount"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filename); err != nil || string(got) != "composed mount" {
		t.Fatalf("injected mounted content = %q, %v", got, err)
	}
}

func TestMountedFilesystem(t *testing.T) {
	if os.Getenv("DFS_MOUNT_TEST") != "1" {
		t.Skip("set DFS_MOUNT_TEST=1 on a machine with FUSE installed")
	}
	base := t.TempDir()
	data, mountpoint := filepath.Join(base, "data"), filepath.Join(base, "mount")
	if err := os.Mkdir(mountpoint, 0o700); err != nil {
		t.Fatal(err)
	}
	for iteration := range 2 {
		func() {
			mounted, err := app.Mount(data, mountpoint)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := fuse.Unmount(mountpoint); err != nil {
					t.Errorf("unmount: %v", err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := mounted.Join(ctx); err != nil {
					t.Errorf("shutdown: %v", err)
				}
			}()
			name := filepath.Join(mountpoint, "saved.txt")
			if iteration == 0 {
				file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				_, writeErr := file.WriteString("persistent")
				syncErr := file.Sync()
				closeErr := file.Close()
				if writeErr != nil || syncErr != nil || closeErr != nil {
					t.Fatalf("write=%v, sync=%v, close=%v", writeErr, syncErr, closeErr)
				}
			}
			content, err := os.ReadFile(name)
			if err != nil || string(content) != "persistent" {
				t.Fatalf("read after mount %d = %q, %v", iteration, content, err)
			}
		}()
	}
}
