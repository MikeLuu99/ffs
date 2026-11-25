package disk

import (
	"math"
	"os"
	"syscall"

	"example.com/dfs/internal/storage"
)

func (d *Engine) Capacity() (storage.Capacity, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.root == nil {
		return storage.Capacity{}, os.ErrClosed
	}
	var result storage.Capacity
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(int(d.dir.Fd()), &stat); err != nil {
		return storage.Capacity{}, err
	}
	if stat.Bsize <= 0 || stat.Bsize > math.MaxUint32 {
		return storage.Capacity{}, syscall.EOVERFLOW
	}
	result.BlockSize, result.IOSize = uint32(stat.Bsize), uint32(stat.Bsize)
	result.Blocks, result.BlocksFree, result.BlocksAvailable = stat.Blocks, stat.Bfree, stat.Bavail
	result.Inodes, result.InodesFree = stat.Files, stat.Ffree
	return result, nil
}
