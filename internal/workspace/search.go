package workspace

import (
	"context"
	"errors"
	"fmt"
)

// OutboxEvent is one committed mutation event delivered at least once.
// Consumers deduplicate by ID; indexes use events to catch up.
type OutboxEvent struct {
	ID        string
	Type      string // e.g. "changeset.committed", "publication.completed"
	Space     SpaceID
	Changeset ChangesetID
	Snapshot  Snapshot
	Paths     []string
	Files     []FileID
	Author    Principal
	At        int64 // unix nanos
}

// SearchQuery is one read-model query. Queries are served from a derived
// index whose coverage can lag committed changesets; every result reports
// its freshness.
type SearchQuery struct {
	Space SpaceID
	// Text matches full-text content when the index supports it.
	Text string
	// Name matches file names (lexical substring or prefix).
	Name string
	// ScopePrefix restricts hits to a path prefix.
	ScopePrefix string
	// Snapshot searches within one pinned snapshot; nil searches the latest
	// indexed state.
	Snapshot *Snapshot
	Limit    int
	Cursor   string
}

// SearchHit is one result. It must be resolved against snapshots for
// content; the index never serves bytes.
type SearchHit struct {
	Space    SpaceID
	File     FileID
	Path     string
	Version  Version
	Snapshot Snapshot
	Snippet  string
	Score    float64
}

// SearchResultPage carries hits plus the index freshness contract: the
// watermark identifies the committed event coverage used, and Lagging
// reports whether newer commits are not yet reflected.
type SearchResultPage struct {
	Hits       []SearchHit
	Watermark  string
	Lagging    bool
	NextCursor string
}

// SearchIndex is a DERIVED read model, independent of authoritative storage.
// It can be rebuilt from the outbox at any time; it never holds authority
// and never serves bytes.
type SearchIndex interface {
	// Search must apply CURRENT authorization before exposing results.
	Search(ctx context.Context, actor Principal, q SearchQuery) (SearchResultPage, error)
	// Apply ingests one committed event. Delivery is at-least-once, so
	// implementations must be idempotent per event ID.
	Apply(ctx context.Context, event OutboxEvent) error
	// Watermark reports the indexing coverage for a space and whether
	// newer committed changesets are not yet indexed.
	Watermark(ctx context.Context, space SpaceID) (watermark string, lagging bool, err error)
}

// AuthorizedSearchIndex wraps any SearchIndex so hits are filtered by
// CURRENT authorization before exposure. It is the reference pattern for
// the requirement that authorization applies to derived views: cached,
// historical, and indexed results all re-check access. Production indexes
// should filter server-side; adapters must not rely on the wrapper alone.
type AuthorizedSearchIndex struct {
	Inner SearchIndex
	Auth  Authorizer
}

// NewAuthorizedSearchIndex wraps an index with current authorization checks.
func NewAuthorizedSearchIndex(inner SearchIndex, auth Authorizer) (*AuthorizedSearchIndex, error) {
	if inner == nil || auth == nil {
		return nil, fmt.Errorf("%w: inner index and authorizer are required", ErrInvalidRequest)
	}
	return &AuthorizedSearchIndex{Inner: inner, Auth: auth}, nil
}

// Search removes hits the principal is no longer authorized to read.
func (s *AuthorizedSearchIndex) Search(ctx context.Context, actor Principal, q SearchQuery) (SearchResultPage, error) {
	page, err := s.Inner.Search(ctx, actor, q)
	if err != nil {
		return SearchResultPage{}, err
	}
	kept := make([]SearchHit, 0, len(page.Hits))
	for _, hit := range page.Hits {
		if err := s.Auth.CheckAccess(ctx, actor, hit.Space, hit.Path, AccessRead); err != nil {
			if errors.Is(err, ErrDenied) {
				continue // filtered, not an error for the caller
			}
			return SearchResultPage{}, err
		}
		kept = append(kept, hit)
	}
	page.Hits = kept
	return page, nil
}

// Apply forwards a committed event to the underlying derived index.
func (s *AuthorizedSearchIndex) Apply(ctx context.Context, event OutboxEvent) error {
	return s.Inner.Apply(ctx, event)
}

// Watermark reports the underlying index's coverage and lag.
func (s *AuthorizedSearchIndex) Watermark(ctx context.Context, space SpaceID) (string, bool, error) {
	return s.Inner.Watermark(ctx, space)
}
