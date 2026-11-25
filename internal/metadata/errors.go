package metadata

import "errors"

// Namespace errors supplement io/fs errors. Adapters wrap these values so
// callers can use errors.Is without knowing a provider's error types.
var (
	ErrNotDirectory = errors.New("not a directory")
	ErrIsDirectory  = errors.New("is a directory")
	ErrNotEmpty     = errors.New("directory is not empty")
	ErrBusy         = errors.New("inode is still linked")
	ErrNameTooLong  = errors.New("name is too long")
	ErrTooLarge     = errors.New("file is too large")
)
