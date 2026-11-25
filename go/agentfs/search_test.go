package agentfs

import (
	"context"
	"errors"
	"testing"
)

// fakeIndex records Apply calls and serves a fixed hit list. It deduplicates
// by event ID to mimic the at-least-once contract.
type fakeIndex struct {
	hits      []SearchHit
	seen      map[string]int
	watermark string
	lagging   bool
}

func (f *fakeIndex) Search(ctx context.Context, _ Principal, q SearchQuery) (SearchResultPage, error) {
	if err := ctx.Err(); err != nil {
		return SearchResultPage{}, err
	}
	hits := make([]SearchHit, 0, len(f.hits))
	for _, hit := range f.hits {
		if q.Space != "" && hit.Space != q.Space {
			continue
		}
		if q.ScopePrefix != "" && !startsWithPrefix(hit.Path, q.ScopePrefix) {
			continue
		}
		hits = append(hits, hit)
	}
	return SearchResultPage{Hits: hits, Watermark: f.watermark, Lagging: f.lagging}, nil
}

func startsWithPrefix(path, prefix string) bool {
	if len(path) < len(prefix) {
		return false
	}
	if path[:len(prefix)] != prefix {
		return false
	}
	return len(path) == len(prefix) || path[len(prefix)] == '/' || prefix[len(prefix)-1] == '/'
}

func (f *fakeIndex) Apply(ctx context.Context, event OutboxEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.seen[event.ID] > 0 {
		return nil // idempotent per event ID
	}
	f.seen[event.ID]++
	f.watermark = event.ID
	return nil
}

func (f *fakeIndex) Watermark(ctx context.Context, _ SpaceID) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	return f.watermark, f.lagging, nil
}

func TestAuthorizedSearchFiltersDeniedPaths(t *testing.T) {
	ctx := context.Background()
	inner := &fakeIndex{
		seen: make(map[string]int),
		hits: []SearchHit{
			{Space: "acme", Path: "public/roadmap.md", File: "f-1"},
			{Space: "acme", Path: "secret/comp.md", File: "f-2"},
			{Space: "other", Path: "public/other.md", File: "f-3"},
		},
	}
	index, err := NewAuthorizedSearchIndex(inner, prefixDeny{deny: "secret/"})
	if err != nil {
		t.Fatal(err)
	}
	actor := testActor("reader")
	page, err := index.Search(ctx, actor, SearchQuery{Space: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Hits) != 1 || page.Hits[0].Path != "public/roadmap.md" {
		t.Fatalf("authorization did not filter hits: %+v", page.Hits)
	}

	// Freshness is part of every result, authorized or not.
	inner.watermark = "evt-9"
	inner.lagging = true
	page, err = index.Search(ctx, actor, SearchQuery{Space: "acme"})
	if err != nil || !page.Lagging || page.Watermark != "evt-9" {
		t.Fatalf("index freshness not reported: %+v %v", page, err)
	}

	// Ingest is idempotent per event ID.
	event := OutboxEvent{ID: "evt-1", Type: "changeset.committed", Space: "acme"}
	if err := index.Apply(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := index.Apply(ctx, event); err != nil {
		t.Fatal(err)
	}
	if inner.seen["evt-1"] != 1 {
		t.Fatalf("event applied %d times", inner.seen["evt-1"])
	}
	if _, lagging, err := index.Watermark(ctx, "acme"); err != nil || !lagging {
		t.Fatalf("watermark passthrough failed: %v", err)
	}
}

func TestDocumentSessionIsExplicitlyUnsupported(t *testing.T) {
	sessions := UnsupportedDocumentSession()
	_, err := sessions.Attach(context.Background(), testActor("a"), "acme", "f-1", SessionEdit, Version{Exists: true, Digest: "d"})
	if !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("sessions must be explicitly unsupported, got %v", err)
	}
}
