package storage

import (
	"context"
	"io"
)

// ObjectStore stores immutable blocks under opaque 32-character hexadecimal
// keys. A successful Put is durable under the provider's persistence model.
// Get returns exactly the requested range or an error; callers close its body.
// Delete is idempotent. List is limited to this store's dedicated volume prefix.
// Providers translate missing objects and access failures to io/fs errors.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	Head(ctx context.Context, key string) (int64, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context) ([]string, error)
}
