package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"example.com/dfs/internal/vfs"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
)

// Mount validates the paths, opens the engines, and serves an empty mountpoint.
// Unmount with fuse.Unmount or the operating system's umount command.
func Mount(data, mountpoint string) (*fuse.MountedFileSystem, error) {
	if err := os.MkdirAll(data, 0o700); err != nil {
		return nil, err
	}
	data, err := filepath.EvalSymlinks(data)
	if err != nil {
		return nil, err
	}
	mountpoint, err = filepath.EvalSymlinks(mountpoint)
	if err != nil {
		return nil, fmt.Errorf("mountpoint must be an existing empty directory: %w", err)
	}
	data, err = filepath.Abs(data)
	if err != nil {
		return nil, err
	}
	mountpoint, err = filepath.Abs(mountpoint)
	if err != nil {
		return nil, err
	}
	inside := func(a, b string) bool {
		relative, err := filepath.Rel(a, b)
		return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
	}
	if inside(data, mountpoint) || inside(mountpoint, data) {
		return nil, errors.New("backing directory and mountpoint must not overlap")
	}
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		return nil, err
	}
	if len(entries) != 0 {
		return nil, errors.New("mountpoint must be empty")
	}
	files, err := Open(data)
	if err != nil {
		return nil, fmt.Errorf("open backing directory: %w", err)
	}
	return MountFS(files, mountpoint)
}

// MountFS mounts an already composed filesystem without selecting any providers.
// It takes ownership of files, closing it on failure. On success FUSE calls
// Destroy when the connection ends, closing the filesystem and its adapters.
// The caller ensures the mountpoint is separate from all local provider paths.
func MountFS(files *vfs.FS, mountpoint string) (_ *fuse.MountedFileSystem, err error) {
	if files == nil {
		return nil, errors.New("filesystem is required")
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, files.Close())
		}
	}()
	mountpoint, err = filepath.EvalSymlinks(mountpoint)
	if err != nil {
		return nil, err
	}
	mountpoint, err = filepath.Abs(mountpoint)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		return nil, err
	}
	if len(entries) != 0 {
		return nil, errors.New("mountpoint must be empty")
	}
	config := &fuse.MountConfig{FSName: "dfs", VolumeName: "dfs", DisableWritebackCaching: true}
	if runtime.GOOS == "darwin" {
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse"); err != nil {
			return nil, fmt.Errorf("mount requires macFUSE; install it from https://macfuse.github.io: %w", err)
		}
		config.FuseImpl = fuse.FUSEImplMacFUSE
	}
	mounted, err := fuse.Mount(mountpoint, fuseutil.NewFileSystemServer(files), config)
	if err != nil {
		return nil, fmt.Errorf("mount filesystem: %w", err)
	}
	return mounted, nil
}
