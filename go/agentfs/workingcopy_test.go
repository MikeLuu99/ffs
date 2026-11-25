package agentfs

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func newManager(t *testing.T, stack *coreStack, journal *memJournal, cache *memCache) *WorkingCopyManager {
	t.Helper()
	m, err := NewWorkingCopyManager(stack.files, stack.publisher, journal, cache, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func seedRemoteFiles(ctx context.Context, t *testing.T, stack *coreStack, files map[string]string) {
	t.Helper()
	other := testActor("publisher")
	pin, err := stack.files.ResolveAndPin(ctx, other, "acme", "main", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseTestPin(ctx, t, stack.files, other, pin.ID)
	ws, err := stack.files.CreateWorkspace(ctx, other, "acme", pin.Snapshot, WorkspaceID("seed"), 0)
	if err != nil {
		t.Fatal(err)
	}
	var ops []Mutation
	for path, content := range files {
		ref, err := stack.files.Stage(ctx, other, "acme", []byte(content))
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, PutMutation(path, Version{Exists: false}, ref))
	}
	ws, err = stack.files.Commit(ctx, other, "acme", ws.ID, CommitRequest{
		Ops: ops, Message: "seed", IdempotencyKey: "seed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stack.publisher.Publish(ctx, PublishRequest{
		Space: "acme", Target: "main", Proposal: ws.Head, Actor: other,
		Mode: PublishRebaseIfSafe, IdempotencyKey: "seed-publish",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkingCopyCheckoutAndSyncPublish(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("editor")
	seedRemoteFiles(ctx, t, stack, map[string]string{"docs/readme.md": "hello"})

	journal, cache := newMemJournal(), newMemCache()
	manager := newManager(t, stack, journal, cache)

	// Record the FileID of the published file for the identity check below.
	mainHead, err := stack.pubs.Head(ctx, actor, "acme", "main")
	if err != nil {
		t.Fatal(err)
	}
	before := versionAtHead(ctx, t, stack, actor, mainHead.Version.Head, "docs/readme.md")

	wc, err := manager.Checkout(ctx, actor, "acme", "main", "/tmp/wc-1", "copy-1")
	if err != nil {
		t.Fatal(err)
	}
	if content, err := cache.Get(ctx, "copy-1", "docs/readme.md"); err != nil || string(content) != "hello" {
		t.Fatalf("checkout did not materialize content: %v %q", err, content)
	}
	manifest, err := journal.Base(ctx, "copy-1")
	if err != nil || !manifest.Files["docs/readme.md"].Exists {
		t.Fatalf("checkout did not record the base manifest: %+v err=%v", manifest, err)
	}

	// Local edit with the recorded base as the implicit expectation.
	if _, err := manager.Write(ctx, actor, wc, "docs/readme.md", nil, []byte("hello v2")); err != nil {
		t.Fatal(err)
	}
	result, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Published == nil {
		t.Fatal("sync did not publish")
	}

	// The published file kept its identity; content moved forward.
	after := versionAtHead(ctx, t, stack, actor, result.Published.Ref.Version.Head, "docs/readme.md")
	if after.File != before.File {
		t.Fatalf("sync changed the FileID: %s -> %s", before.File, after.File)
	}
	if content, _, err := stack.files.ReadFile(ctx, actor, "acme", after.Snapshot, "docs/readme.md"); err != nil || string(content) != "hello v2" {
		t.Fatalf("published content is wrong: %q %v", content, err)
	}

	// Entry confirmed; manifest refreshed to the new ground truth.
	confirmed, err := journal.List(ctx, "copy-1", SyncConfirmed)
	if err != nil || len(confirmed) != 1 {
		t.Fatalf("entry not confirmed: %v %v", confirmed, err)
	}
	fresh, err := journal.Base(ctx, "copy-1")
	if err != nil || fresh.Files["docs/readme.md"].Digest != after.Version.Digest {
		t.Fatalf("manifest was not refreshed after publication: %+v", fresh)
	}

	// An immediate second sync is a no-op and publishes nothing.
	if _, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true}); err != nil {
		t.Fatal(err)
	}
	if stack.pubs.outbox != 2 { // seed + our single sync publication
		t.Fatalf("duplicate sync published again: %d events", stack.pubs.outbox)
	}
}

func TestSyncSaveOnlyDefersPublication(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("editor")
	seedRemoteFiles(ctx, t, stack, map[string]string{"notes.txt": "n1"})

	journal, cache := newMemJournal(), newMemCache()
	manager := newManager(t, stack, journal, cache)
	wc, err := manager.Checkout(ctx, actor, "acme", "main", "/tmp/wc-2", "copy-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(ctx, actor, wc, "notes.txt", nil, []byte("n2")); err != nil {
		t.Fatal(err)
	}
	mainBefore, err := stack.pubs.Head(ctx, actor, "acme", "main")
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: false})
	if err != nil {
		t.Fatal(err)
	}
	if result.Published != nil {
		t.Fatal("save-only sync published")
	}
	mainAfter, err := stack.pubs.Head(ctx, actor, "acme", "main")
	if err != nil {
		t.Fatal(err)
	}
	if mainAfter.Version != mainBefore.Version {
		t.Fatal("save-only sync moved main")
	}
	// A later sync publishes the saved work without new edits.
	result, err = manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true})
	if err != nil || result.Published == nil {
		t.Fatalf("publishing saved work failed: %+v %v", result, err)
	}
}

func TestSyncRecoveryAfterCrash(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("editor")
	seedRemoteFiles(ctx, t, stack, map[string]string{"a.txt": "a1", "b.txt": "b1"})

	journal, cache := newMemJournal(), newMemCache()
	manager := newManager(t, stack, journal, cache)
	wc, err := manager.Checkout(ctx, actor, "acme", "main", "/tmp/wc-3", "copy-3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(ctx, actor, wc, "a.txt", nil, []byte("a2")); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(ctx, actor, wc, "b.txt", nil, []byte("b2")); err != nil {
		t.Fatal(err)
	}

	// Crash 1: content staged, changeset not saved. The commit fails once.
	failing := failingCommitService{FileService: stack.files, failOnce: true}
	manager.files = &failing
	if _, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true}); err == nil {
		t.Fatal("injected commit failure did not surface")
	}
	staged, err := journal.List(ctx, "copy-3", SyncPending)
	if err != nil || len(staged) != 2 {
		t.Fatalf("staged edits were lost: %+v %v", staged, err)
	}
	for _, entry := range staged {
		if entry.Blob == nil {
			t.Fatalf("entry %s lost its staged blob", entry.ID)
		}
	}

	// Recovery: a fresh manager over the same durable journal + cache.
	manager.files = stack.files
	manager2 := newManager(t, stack, journal, cache)
	wcFresh := *wc
	result, err := manager2.Sync(ctx, &wcFresh, SyncOptions{Actor: actor, Publish: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Published == nil {
		t.Fatal("recovered sync did not publish")
	}
	after := versionAtHead(ctx, t, stack, actor, result.Published.Ref.Version.Head, "a.txt")
	if after.Version.Digest != digestBytes([]byte("a2")) {
		t.Fatal("recovered sync published wrong content")
	}

	// Crash 2: publication succeeded, but the receipt response was lost.
	// Roll the journal back to the saved state to simulate it.
	saved, err := journal.List(ctx, "copy-3", SyncConfirmed)
	if err != nil || len(saved) != 2 {
		t.Fatalf("confirmed entries missing: %v", err)
	}
	for i := range saved {
		saved[i].State = SyncSaved
		if err := journal.Put(ctx, "copy-3", saved[i]); err != nil {
			t.Fatal(err)
		}
	}
	outboxBefore := stack.pubs.outbox
	result2, err := manager2.Sync(ctx, &wcFresh, SyncOptions{Actor: actor, Publish: true})
	if err != nil || result2.Published == nil {
		t.Fatalf("lost-receipt replay failed: %v", err)
	}
	if result2.Published.Ref.Version.Generation != result.Published.Ref.Version.Generation {
		t.Fatalf("replay created a new generation: %+v vs %+v", result2.Published, result.Published)
	}
	if stack.pubs.outbox != outboxBefore {
		t.Fatal("replay published a redundant event")
	}
	confirmed, err := journal.List(ctx, "copy-3", SyncConfirmed)
	if err != nil {
		t.Fatal(err)
	}
	if len(confirmed) != 2 {
		t.Fatalf("replay did not confirm entries: %+v", confirmed)
	}
}

type failingCommitService struct {
	FileService
	failOnce bool
}

func (f *failingCommitService) Commit(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req CommitRequest) (Workspace, error) {
	if f.failOnce {
		f.failOnce = false
		return Workspace{}, fmt.Errorf("simulated crash during save")
	}
	return f.FileService.Commit(ctx, actor, space, workspace, req)
}

func TestSyncDetectsConflictOnReconnect(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	editor := testActor("editor")
	seedRemoteFiles(ctx, t, stack, map[string]string{"pricing.json": "{\"p\":1}"})

	journal, cache := newMemJournal(), newMemCache()
	manager := newManager(t, stack, journal, cache)
	wc, err := manager.Checkout(ctx, editor, "acme", "main", "/tmp/wc-4", "copy-4")
	if err != nil {
		t.Fatal(err)
	}
	base := versionAtHead(ctx, t, stack, editor, func() ChangesetID {
		ref, err := stack.pubs.Head(ctx, editor, "acme", "main")
		if err != nil {
			t.Fatal(err)
		}
		return ref.Version.Head
	}(), "pricing.json")

	// Another writer advances main underneath our working copy.
	other := testActor("other")
	if _, _, err := stack.publishDirect(ctx, other, "acme", "pricing.json", presentVersion(base.Version.Digest), []byte("{\"p\":2}"), "other-1"); err != nil {
		t.Fatal(err)
	}

	// Our edit still believes in the recorded base.
	if _, err := manager.Write(ctx, editor, wc, "forecast.md", absentVersion(), []byte("f1")); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(ctx, editor, wc, "pricing.json", nil, []byte("{\"p\":3}")); err != nil {
		t.Fatal(err)
	}
	result, err := manager.Sync(ctx, wc, SyncOptions{Actor: editor, Publish: true})
	if err != nil {
		t.Fatalf("sync with conflicts should preserve edits, not fail: %v", err)
	}
	// The healthy edit lands; the stale edit does not.
	if got := versionAtHead(ctx, t, stack, editor, result.Published.Ref.Version.Head, "forecast.md"); got.Version.Digest != digestBytes([]byte("f1")) {
		t.Fatal("healthy edit was not published")
	}
	mainHead := result.Published.Ref.Version.Head
	if got := versionAtHead(ctx, t, stack, editor, mainHead, "pricing.json"); got.Version.Digest != digestBytes([]byte("{\"p\":2}")) {
		t.Fatalf("a conflicted edit reached main: %+v", got)
	}
	conflicted, err := journal.List(ctx, "copy-4", SyncConflict)
	if err != nil || len(conflicted) != 1 || conflicted[0].Path != "pricing.json" {
		t.Fatalf("stale edit was not marked conflicted: %+v %v", conflicted, err)
	}

	// Resolution: the editor re-reads remote truth and records the new base.
	remote := versionAtHead(ctx, t, stack, editor, func() ChangesetID {
		ref, err := stack.pubs.Head(ctx, editor, "acme", "main")
		if err != nil {
			t.Fatal(err)
		}
		return ref.Version.Head
	}(), "pricing.json")
	if _, err := manager.Write(ctx, editor, wc, "pricing.json", presentVersion(remote.Version.Digest), []byte("{\"p\":3}")); err != nil {
		t.Fatal(err)
	}
	result, err = manager.Sync(ctx, wc, SyncOptions{Actor: editor, Publish: true})
	if err != nil || result.Published == nil {
		t.Fatalf("resolution sync failed: %v", err)
	}
	head := result.Published.Ref.Version.Head
	if got := versionAtHead(ctx, t, stack, editor, head, "forecast.md"); got.Version.Digest != digestBytes([]byte("f1")) {
		t.Fatal("resolved sync lost the healthy edit")
	}
	if got := versionAtHead(ctx, t, stack, editor, head, "pricing.json"); got.Version.Digest != digestBytes([]byte("{\"p\":3}")) {
		t.Fatal("resolved sync did not publish the re-based edit")
	}
}

func TestSyncBoundedUploads(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("editor")
	seed := make(map[string]string)
	for i := 0; i < 6; i++ {
		seed[fmt.Sprintf("f%d.txt", i)] = fmt.Sprintf("content-%d", i)
	}
	seedRemoteFiles(ctx, t, stack, seed)

	journal, cache := newMemJournal(), newMemCache()
	counting := &countingService{FileService: stack.files, stageGate: make(chan struct{})}
	files := stack.files
	stack.files = counting // sync uses the counting wrapper
	manager := newManager(t, stack, journal, cache)
	wc, err := manager.Checkout(ctx, actor, "acme", "main", "/tmp/wc-5", "copy-5")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		path := fmt.Sprintf("f%d.txt", i)
		if _, err := manager.Write(ctx, actor, wc, path, nil, []byte(seed[path]+"-edited")); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		for i := 0; i < 6; i++ {
			counting.stageGate <- struct{}{}
		}
	}()
	result, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Published == nil {
		t.Fatal("bounded sync did not publish")
	}
	if counting.peak > 2 {
		t.Fatalf("staging exceeded the concurrency bound: peak=%d", counting.peak)
	}
	stack.files = files
}

func TestSyncHonorsCancellation(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("editor")
	seedRemoteFiles(ctx, t, stack, map[string]string{"c.txt": "c1"})

	journal, cache := newMemJournal(), newMemCache()
	manager := newManager(t, stack, journal, cache)
	wc, err := manager.Checkout(ctx, actor, "acme", "main", "/tmp/wc-6", "copy-6")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(ctx, actor, wc, "c.txt", nil, []byte("c2")); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := manager.Sync(canceled, wc, SyncOptions{Actor: actor, Publish: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled sync reported %v", err)
	}
	pending, err := journal.List(ctx, "copy-6", SyncPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("cancellation disturbed the journal: %+v", pending)
	}
	// Retry after cancellation converges.
	result, err := manager.Sync(ctx, wc, SyncOptions{Actor: actor, Publish: true})
	if err != nil || result.Published == nil {
		t.Fatalf("post-cancellation sync failed: %v", err)
	}
}

func TestEvictKeepsUnconfirmedEdits(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("editor")
	seedRemoteFiles(ctx, t, stack, map[string]string{"keep.txt": "k", "drop.txt": "d"})

	journal, cache := newMemJournal(), newMemCache()
	manager := newManager(t, stack, journal, cache)
	wc, err := manager.Checkout(ctx, actor, "acme", "main", "/tmp/wc-7", "copy-7")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(ctx, actor, wc, "keep.txt", nil, []byte("k-edited")); err != nil {
		t.Fatal(err)
	}
	evicted, err := manager.Evict(ctx, wc)
	if err != nil {
		t.Fatal(err)
	}
	if evicted != 1 {
		t.Fatalf("unexpected eviction count: %d", evicted)
	}
	if _, err := cache.Get(ctx, "copy-7", "drop.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatal("evictable content was kept")
	}
	if _, err := cache.Get(ctx, "copy-7", "keep.txt"); err != nil {
		t.Fatal("content with an unconfirmed edit was evicted")
	}
}
