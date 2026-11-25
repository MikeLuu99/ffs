package localfs

import (
	"os"
	"syscall"
	"time"

	"github.com/jacobsa/fuse/fuseops"
)

func fileAttributes(info os.FileInfo) fuseops.InodeAttributes {
	stat := info.Sys().(*syscall.Stat_t)
	return fuseops.InodeAttributes{
		Size: uint64(info.Size()), Mode: info.Mode(), Nlink: uint32(stat.Nlink),
		Uid: stat.Uid, Gid: stat.Gid, Mtime: info.ModTime(),
		Atime:  time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec),
		Ctime:  time.Unix(stat.Ctimespec.Sec, stat.Ctimespec.Nsec),
		Crtime: time.Unix(stat.Birthtimespec.Sec, stat.Birthtimespec.Nsec),
	}
}

func filesystemStats(fd int, op *fuseops.StatFSOp) error {
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(fd, &stat); err != nil {
		return err
	}
	op.BlockSize, op.IoSize = stat.Bsize, stat.Bsize
	op.Blocks, op.BlocksFree, op.BlocksAvailable = stat.Blocks, stat.Bfree, stat.Bavail
	op.Inodes, op.InodesFree = stat.Files, stat.Ffree
	return nil
}
