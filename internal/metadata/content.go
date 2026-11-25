package metadata

import (
	"encoding/hex"
	"errors"
	"io/fs"
	"math"
)

// BlockSize is the size of a logical content block. Holes have no object key.
const BlockSize = 4 << 20

// ErrConflict means a content layout changed since it was read.
var ErrConflict = errors.New("content revision conflict")

// Layout maps file block offsets to immutable objects. Retired objects are no
// longer readable through Blocks and can be deleted after this record commits.
// Size and Blocks are always committed together.
type Layout struct {
	Revision uint64
	Inode    uint64
	Size     uint64
	Blocks   map[uint64]string
	Retired  []string
}

// ContentEngine persists layouts independently of directory entries. A content
// key is allocated before linking an inode and survives rename and unlink.
// PutLayout is compare-and-swap: revision zero creates, otherwise it replaces
// exactly that revision. Implementations advance Revision by one.
type ContentEngine interface {
	GetLayout(key string) (Layout, error)
	PutLayout(key string, revision uint64, layout Layout) error
	DeleteLayout(key string, revision uint64) error
	LayoutKeys() ([]string, error)
}

// ValidContentKey accepts only the opaque IDs used for layouts and blocks.
func ValidContentKey(key string) bool {
	if len(key) != 32 {
		return false
	}
	_, err := hex.DecodeString(key)
	return err == nil
}

// Validate rejects corrupt layouts before any content deletion can take place.
func (l Layout) Validate() error {
	if l.Size > math.MaxInt64 || l.Revision == 0 {
		return fs.ErrInvalid
	}
	live := make(map[string]bool, len(l.Blocks))
	for index, key := range l.Blocks {
		if l.Size == 0 || index > (l.Size-1)/BlockSize || !ValidContentKey(key) || live[key] {
			return fs.ErrInvalid
		}
		live[key] = true
	}
	for _, key := range l.Retired {
		if !ValidContentKey(key) || live[key] {
			return fs.ErrInvalid
		}
	}
	return nil
}
