package disk

import (
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
	result.BlockSize, result.IOSize = stat.Bsize, stat.Bsize
	result.Blocks, result.BlocksFree, result.BlocksAvailable = stat.Blocks, stat.Bfree, stat.Bavail
	result.Inodes, result.InodesFree = stat.Files, stat.Ffree
	return result, nil
}
