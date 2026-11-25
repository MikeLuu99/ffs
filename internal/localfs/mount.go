package localfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
)

// Mount opens a backing directory and serves it at an existing empty mountpoint.
// Unmount with fuse.Unmount or the operating system's umount command.
func Mount(data, mountpoint string) (*fuse.MountedFileSystem, error) {
	files, err := Open(data)
	if err != nil {
		return nil, fmt.Errorf("open backing directory: %w", err)
	}
	fail := func(err error) (*fuse.MountedFileSystem, error) {
		return nil, errors.Join(err, files.Close())
	}
	data, err = filepath.EvalSymlinks(data)
	if err != nil {
		return fail(err)
	}
	mountpoint, err = filepath.EvalSymlinks(mountpoint)
	if err != nil {
		return fail(fmt.Errorf("mountpoint must be an existing empty directory: %w", err))
	}
	data, err = filepath.Abs(data)
	if err != nil {
		return fail(err)
	}
	mountpoint, err = filepath.Abs(mountpoint)
	if err != nil {
		return fail(err)
	}
	inside := func(a, b string) bool {
		relative, err := filepath.Rel(a, b)
		return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
	}
	if inside(data, mountpoint) || inside(mountpoint, data) {
		return fail(errors.New("backing directory and mountpoint must not overlap"))
	}
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		return fail(err)
	}
	if len(entries) != 0 {
		return fail(errors.New("mountpoint must be empty"))
	}
	config := &fuse.MountConfig{FSName: "singlefs", VolumeName: "singlefs", DisableWritebackCaching: true}
	if runtime.GOOS == "darwin" {
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse"); err != nil {
			return fail(fmt.Errorf("mount requires macFUSE; install it from https://macfuse.github.io: %w", err))
		}
		config.FuseImpl = fuse.FUSEImplMacFUSE
	}
	mounted, err := fuse.Mount(mountpoint, fuseutil.NewFileSystemServer(files), config)
	if err != nil {
		return fail(fmt.Errorf("mount filesystem: %w", err))
	}
	return mounted, nil
}
