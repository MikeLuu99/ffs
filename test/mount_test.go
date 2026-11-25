package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.com/dfs/internal/localfs"

	"github.com/jacobsa/fuse"
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
			if _, err := localfs.Mount(data, mountpoint); err == nil || !strings.Contains(err.Error(), expected) {
				t.Fatalf("Mount error = %v, want %q", err, expected)
			}
			// Failed setup must release the backing-directory lock.
			openFS(t, data)
		})
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
			mounted, err := localfs.Mount(data, mountpoint)
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
