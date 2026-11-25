package localfs

import (
	"math"
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
		Atime: time.Unix(stat.Atim.Sec, stat.Atim.Nsec),
		Ctime: time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec),
	}
}

func filesystemStats(fd int, op *fuseops.StatFSOp) error {
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(fd, &stat); err != nil {
		return err
	}
	if stat.Bsize <= 0 || stat.Bsize > math.MaxUint32 {
		return syscall.EOVERFLOW
	}
	op.BlockSize, op.IoSize = uint32(stat.Bsize), uint32(stat.Bsize)
	op.Blocks, op.BlocksFree, op.BlocksAvailable = stat.Blocks, stat.Bfree, stat.Bavail
	op.Inodes, op.InodesFree = stat.Files, stat.Ffree
	return nil
}
