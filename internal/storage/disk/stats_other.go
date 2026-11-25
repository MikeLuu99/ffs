//go:build !linux && !darwin

package disk

import (
	"errors"

	"example.com/dfs/internal/storage"
)

// Capacity is unavailable when the platform has no disk-capacity implementation.
func (*Engine) Capacity() (storage.Capacity, error) {
	return storage.Capacity{}, errors.ErrUnsupported
}
