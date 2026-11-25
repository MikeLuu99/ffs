package agentfs

import (
	"context"
	"errors"
	"testing"
)

type failingMoveCache struct {
	LocalCache
	operation string
	err       error
}

func (c failingMoveCache) Get(ctx context.Context, copyID, path string) ([]byte, error) {
	if c.operation == "read" {
		return nil, c.err
	}
	return c.LocalCache.Get(ctx, copyID, path)
}

func (c failingMoveCache) Put(ctx context.Context, copyID, path string, content []byte) error {
	if c.operation == "write" {
		return c.err
	}
	return c.LocalCache.Put(ctx, copyID, path, content)
}

func (c failingMoveCache) Delete(ctx context.Context, copyID, path string) error {
	if c.operation == "delete" {
		return c.err
	}
	return c.LocalCache.Delete(ctx, copyID, path)
}

type failingMoveJournal struct {
	SyncJournal
	err error
}

func (j failingMoveJournal) Put(context.Context, string, JournalEntry) error {
	return j.err
}

func TestMovePreservesContentOnFailure(t *testing.T) {
	for _, operation := range []string{"read", "write", "delete", "journal"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			stack := newCoreStack(t, testCaps(), nil)
			actor := testActor("editor")
			seedRemoteFiles(ctx, t, stack, map[string]string{"a.txt": "original"})
			journal, cache := newMemJournal(), newMemCache()
			manager := newManager(t, stack, journal, cache)
			wc, err := manager.Checkout(ctx, actor, "acme", "main", t.TempDir(), "move-copy")
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected local storage failure")
			if operation == "journal" {
				manager.journal = failingMoveJournal{SyncJournal: journal, err: injected}
			} else {
				manager.cache = failingMoveCache{LocalCache: cache, operation: operation, err: injected}
			}
			if _, err := manager.Move(ctx, actor, wc, "a.txt", "b.txt", nil, nil); !errors.Is(err, injected) {
				t.Fatalf("Move error = %v, want storage failure", err)
			}
			if data, err := cache.Get(ctx, wc.ID, "a.txt"); err != nil || string(data) != "original" {
				t.Fatalf("source lost after %s failure: %q, %v", operation, data, err)
			}
			entries, err := journal.List(ctx, wc.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantEntries := 1
			if operation == "read" || operation == "journal" {
				wantEntries = 0
			}
			if len(entries) != wantEntries {
				t.Fatalf("journal has %d entries, want %d", len(entries), wantEntries)
			}
			manager.cache, manager.journal = cache, journal
			if _, err := manager.Move(ctx, actor, wc, "a.txt", "b.txt", nil, nil); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			if _, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true}); err != nil {
				t.Fatalf("recovered move did not publish: %v", err)
			}
		})
	}
}

func TestMovePreservesUnpublishedEdits(t *testing.T) {
	for _, path := range []string{"a.txt", "b.txt"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			stack := newCoreStack(t, testCaps(), nil)
			actor := testActor("editor")
			seedRemoteFiles(ctx, t, stack, map[string]string{"a.txt": "original"})
			journal, cache := newMemJournal(), newMemCache()
			manager := newManager(t, stack, journal, cache)
			wc, err := manager.Checkout(ctx, actor, "acme", "main", t.TempDir(), "edited-copy")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := manager.Write(ctx, actor, wc, path, nil, []byte("unpublished edit")); err != nil {
				t.Fatal(err)
			}
			if _, err := manager.Move(ctx, actor, wc, "a.txt", "b.txt", nil, nil); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Move error = %v, want rejection of overlapping edits", err)
			}
			if data, err := cache.Get(ctx, wc.ID, path); err != nil || string(data) != "unpublished edit" {
				t.Fatalf("unpublished content lost: %q, %v", data, err)
			}
			if _, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true}); err != nil {
				t.Fatalf("preserved edit did not publish: %v", err)
			}
		})
	}
}

func TestMoveRetryPreservesSavedBatch(t *testing.T) {
	ctx := context.Background()
	stack := newCoreStack(t, testCaps(), nil)
	actor := testActor("editor")
	seedRemoteFiles(ctx, t, stack, map[string]string{"a.txt": "original"})
	journal, cache := newMemJournal(), newMemCache()
	manager := newManager(t, stack, journal, cache)
	wc, err := manager.Checkout(ctx, actor, "acme", "main", t.TempDir(), "saved-move")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Move(ctx, actor, wc, "a.txt", "b.txt", nil, nil); err != nil {
		t.Fatal(err)
	}
	saved, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := manager.Move(ctx, actor, wc, "a.txt", "b.txt", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if entry.State != SyncSaved || entry.BatchID != saved.BatchID {
		t.Fatalf("retry reset saved batch: %+v", entry)
	}
	result, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true})
	if err != nil || result.Published == nil {
		t.Fatalf("saved move did not publish after retry: %+v, %v", result, err)
	}
	content, stat, err := stack.files.ReadFile(ctx, actor, "acme", result.Published.Published, "b.txt")
	if err != nil || string(content) != "original" || stat.File == "" {
		t.Fatalf("published move has wrong content or identity: %q, %+v, %v", content, stat, err)
	}
}
