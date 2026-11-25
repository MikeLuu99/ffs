package agentfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// PublisherClient is the publication surface used by sync workflows. The
// concrete Publisher implements it; tests may substitute it.
type PublisherClient interface {
	Publish(ctx context.Context, request PublishRequest) (Receipt, error)
}

// SyncState is the durable state of one pending local edit. States exist so
// a crash at any point leaves a recoverable, idempotently retryable record:
//
//	pending -> (staged, preconditions recorded) -> saved -> confirmed
//	   \-> conflict            (recorded base no longer current remotely)
type SyncState string

const (
	// SyncPending identifies a durable unpublished local edit, possibly with verified
	// staged content, not yet part of a saved changeset.
	SyncPending SyncState = "pending"
	// SyncSaved indicates the edit is inside a durable saved changeset that has not
	// been confirmed as published.
	SyncSaved SyncState = "saved"
	// SyncConfirmed indicates remote acceptance (save and, when requested,
	// publication) has been confirmed by a durable receipt.
	SyncConfirmed SyncState = "confirmed"
	// SyncConflict indicates the recorded base version is no longer current on the
	// sync target; the edit is preserved until the caller resolves it by
	// writing again with a fresh expected version.
	SyncConflict SyncState = "conflict"
)

// JournalEntry is one durable record of a local edit. IDs are stable per
// logical operation (copy, kind, path), so editing the same path again
// replaces the pending record while preserving the newest durable content.
type JournalEntry struct {
	ID     string `json:"id"`
	Kind   OpKind `json:"kind"`
	Path   string `json:"path"`
	Target string `json:"target,omitempty"`
	// Base is the version of Path in the snapshot the edit was derived
	// from. It doubles as the declared read dependency validated at
	// publication time.
	Base Version `json:"base"`
	// TargetBase is the recorded expectation at Target for moves.
	TargetBase Version `json:"target_base,omitempty"`
	// WorkspaceBase is the precondition recorded at save time: the version
	// of Path at the workspace head when the changeset was prepared. It is
	// set once and reused verbatim across retries so replay hashes match.
	WorkspaceBase *Version `json:"workspace_base,omitempty"`
	// Blob is the verified, staged content for puts (nil until staged).
	Blob *BlobRef `json:"blob,omitempty"`
	// LocalDigest and Size describe the local content of a put.
	LocalDigest Digest `json:"local_digest,omitempty"`
	Size        int64  `json:"size,omitempty"`

	State     SyncState `json:"state"`
	BatchID   string    `json:"batch_id,omitempty"`
	Attempts  int       `json:"attempts"`
	Error     string    `json:"error,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// BaseManifest records the versions observed at checkout (and refreshed
// after confirmed publications). It is the working copy's recorded base.
type BaseManifest struct {
	Ref      RefName            `json:"ref"`
	Snapshot Snapshot           `json:"snapshot"`
	Files    map[string]Version `json:"files"`
}

// SyncJournal is the durable local record of a working copy: pending
// operations plus the base manifest. Implementations must survive crashes
// (write-ahead: record before acting) and are the recovery source of truth.
type SyncJournal interface {
	Put(ctx context.Context, copyID string, entry JournalEntry) error
	Get(ctx context.Context, copyID, entryID string) (JournalEntry, error)
	// List returns entries in insertion order, optionally filtered by state.
	List(ctx context.Context, copyID string, states ...SyncState) ([]JournalEntry, error)
	Delete(ctx context.Context, copyID, entryID string) error
	PutBase(ctx context.Context, copyID string, base BaseManifest) error
	Base(ctx context.Context, copyID string) (BaseManifest, error)
}

// LocalCache stores working-copy content locally. Content for paths with
// unconfirmed journal entries is DURABLE UNPUBLISHED WORK and must never be
// evicted; content matching the recorded base snapshot is EVICTABLE CACHE
// and may be re-fetched from the remote snapshot.
type LocalCache interface {
	Put(ctx context.Context, copyID, path string, content []byte) error
	Get(ctx context.Context, copyID, path string) ([]byte, error)
	Delete(ctx context.Context, copyID, path string) error
}

// WorkingCopy is a local directory backed by a pinned remote snapshot. The
// shared refs stay authoritative remotely: this copy records its base and
// pending edits in the journal and never mutates shared state directly.
type WorkingCopy struct {
	ID        string
	Space     SpaceID
	Workspace WorkspaceID
	Root      string // local directory (informational)
	Target    RefName
	CreatedAt time.Time
}

// SyncOptions configure one synchronization pass.
type SyncOptions struct {
	Actor Principal
	// Target is the shared ref to detect conflicts against and publish to.
	// Empty defaults to the checkout ref.
	Target RefName
	// Publish publishes the synchronized workspace head after saving. When
	// false, edits are only saved to the private workspace.
	Publish bool
	// Mode is the publication mode; empty defaults to rebase_if_safe.
	Mode PublishMode
}

// EntryOutcome reports what happened to one journal entry during a sync.
type EntryOutcome struct {
	EntryID string
	Path    string
	Target  string
	State   SyncState
	Detail  string
}

// SyncResult summarizes one synchronization pass.
type SyncResult struct {
	BatchID   string
	Workspace Workspace
	// Changeset is the saved (or replayed) changeset covering this pass.
	Changeset ChangesetID
	// Published is the durable receipt when publication was requested and
	// succeeded.
	Published *Receipt
	Outcomes  []EntryOutcome
}

// WorkingCopyManager implements checkout and synchronization over the
// application services. It holds no authority: shared refs move only
// through the Publisher.
type WorkingCopyManager struct {
	files      FileService
	publisher  PublisherClient
	journal    SyncJournal
	cache      LocalCache
	maxUploads int
	pinTTL     time.Duration

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// NewWorkingCopyManager wires a manager. maxUploads bounds concurrent
// staging work; pinTTL is used for the pins taken during checkout and sync.
func NewWorkingCopyManager(files FileService, publisher PublisherClient, journal SyncJournal, cache LocalCache, maxUploads int, pinTTL time.Duration) (*WorkingCopyManager, error) {
	if files == nil || journal == nil || cache == nil {
		return nil, fmt.Errorf("%w: file service, journal, and cache are required", ErrInvalidRequest)
	}
	if maxUploads < 1 {
		maxUploads = 4
	}
	if pinTTL <= 0 {
		pinTTL = 30 * time.Minute
	}
	return &WorkingCopyManager{
		files: files, publisher: publisher, journal: journal, cache: cache,
		maxUploads: maxUploads, pinTTL: pinTTL,
		locks: make(map[string]*sync.Mutex),
	}, nil
}

// copyLock serializes journal mutations per working copy. Synchronization of
// one copy is not safe for concurrent invocation.
func (m *WorkingCopyManager) copyLock(copyID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.locks[copyID]
	if !ok {
		l = &sync.Mutex{}
		m.locks[copyID] = l
	}
	return l
}

// Checkout materializes a pinned snapshot into a new working copy: content
// goes to the (evictable) cache, recorded base versions go to the journal.
func (m *WorkingCopyManager) Checkout(ctx context.Context, actor Principal, space SpaceID, ref RefName, localDir, copyID string) (*WorkingCopy, error) {
	if err := validatePrincipal(actor); err != nil {
		return nil, err
	}
	if copyID == "" {
		return nil, fmt.Errorf("%w: copy id is required", ErrInvalidRequest)
	}
	pin, err := m.files.ResolveAndPin(ctx, actor, space, ref, m.pinTTL)
	if err != nil {
		return nil, err
	}
	snap := pin.Snapshot
	ws, err := m.files.CreateWorkspace(ctx, actor, space, snap, WorkspaceID("wc-"+copyID), 0)
	if err != nil {
		return nil, err
	}
	manifest := BaseManifest{Ref: ref, Snapshot: snap, Files: make(map[string]Version)}
	cursor := ""
	for {
		page, err := m.files.List(ctx, actor, space, snap, "", cursor, 500)
		if err != nil {
			return nil, err
		}
		for _, entry := range page.Entries {
			content, stat, err := m.files.ReadFile(ctx, actor, space, snap, entry.Path)
			if err != nil {
				return nil, err
			}
			if err := m.cache.Put(ctx, copyID, entry.Path, content); err != nil {
				return nil, err
			}
			manifest.Files[entry.Path] = stat.Version
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if err := m.journal.PutBase(ctx, copyID, manifest); err != nil {
		return nil, err
	}
	return &WorkingCopy{
		ID: copyID, Space: space, Workspace: ws.ID, Root: localDir,
		Target: ref, CreatedAt: time.Now().UTC(),
	}, nil
}

// Write records a local edit durably BEFORE any remote interaction. A nil
// expected version defaults to the recorded base version of the path.
func (m *WorkingCopyManager) Write(ctx context.Context, _ Principal, wc *WorkingCopy, path string, expected *Version, content []byte) (JournalEntry, error) {
	lock := m.copyLock(wc.ID)
	lock.Lock()
	defer lock.Unlock()
	if !validTreePath(path) {
		return JournalEntry{}, fmt.Errorf("%w: invalid path %q", ErrInvalidRequest, path)
	}
	if expected == nil {
		manifest, err := m.journal.Base(ctx, wc.ID)
		if err != nil {
			return JournalEntry{}, err
		}
		base, ok := manifest.Files[path]
		if !ok {
			base = Version{Exists: false}
		}
		expected = &base
	}
	digest := digestBytes(content)
	entry := JournalEntry{
		ID: opEntryID(wc.ID, OpPut, path, ""), Kind: OpPut, Path: path,
		Base: *expected, LocalDigest: digest, Size: int64(len(content)),
		State: SyncPending, UpdatedAt: time.Now().UTC(),
	}
	// Re-editing a path whose staged content is identical keeps the staged
	// blob and recorded workspace precondition; otherwise they are invalid
	// and are restaged on the next sync.
	if prior, err := m.journal.Get(ctx, wc.ID, entry.ID); err == nil {
		if prior.LocalDigest == digest && prior.Blob != nil && prior.WorkspaceBase != nil {
			entry.Blob, entry.WorkspaceBase = prior.Blob, prior.WorkspaceBase
		}
	}
	if err := m.cache.Put(ctx, wc.ID, path, content); err != nil {
		return JournalEntry{}, err
	}
	if err := m.journal.Put(ctx, wc.ID, entry); err != nil {
		return JournalEntry{}, err
	}
	return entry, nil
}

// Delete records a local deletion.
func (m *WorkingCopyManager) Delete(ctx context.Context, _ Principal, wc *WorkingCopy, path string, expected *Version) (JournalEntry, error) {
	lock := m.copyLock(wc.ID)
	lock.Lock()
	defer lock.Unlock()
	if !validTreePath(path) {
		return JournalEntry{}, fmt.Errorf("%w: invalid path %q", ErrInvalidRequest, path)
	}
	if expected == nil {
		manifest, err := m.journal.Base(ctx, wc.ID)
		if err != nil {
			return JournalEntry{}, err
		}
		base, ok := manifest.Files[path]
		if !ok {
			return JournalEntry{}, fmt.Errorf("%w: no recorded base for %q", ErrInvalidRequest, path)
		}
		expected = &base
	}
	entry := JournalEntry{
		ID: opEntryID(wc.ID, OpDelete, path, ""), Kind: OpDelete, Path: path,
		Base: *expected, State: SyncPending, UpdatedAt: time.Now().UTC(),
	}
	if err := m.journal.Put(ctx, wc.ID, entry); err != nil {
		return JournalEntry{}, err
	}
	return entry, nil
}

// Move records a local rename. File identity is preserved by the remote
// move operation; local content follows when cached. Both paths must be free
// of other unconfirmed edits; synchronize those edits before moving them.
func (m *WorkingCopyManager) Move(ctx context.Context, _ Principal, wc *WorkingCopy, from, to string, expectedFrom, expectedTo *Version) (JournalEntry, error) {
	lock := m.copyLock(wc.ID)
	lock.Lock()
	defer lock.Unlock()
	if !validTreePath(from) || !validTreePath(to) || from == to {
		return JournalEntry{}, fmt.Errorf("%w: invalid move %q -> %q", ErrInvalidRequest, from, to)
	}
	entryID := opEntryID(wc.ID, OpMove, from, to)
	entries, err := m.journal.List(ctx, wc.ID)
	if err != nil {
		return JournalEntry{}, err
	}
	var prior *JournalEntry
	for _, pending := range entries {
		if pending.State == SyncConfirmed {
			continue
		}
		if pending.ID == entryID && pending.State != SyncConflict {
			prior = &pending
			continue
		}
		if pending.Path == from || pending.Path == to || pending.Target == from || pending.Target == to {
			return JournalEntry{}, fmt.Errorf("%w: synchronize edits to %q and %q before moving", ErrInvalidRequest, from, to)
		}
	}
	if prior != nil {
		if (expectedFrom != nil && !expectedFrom.Equal(prior.Base)) ||
			(expectedTo != nil && !expectedTo.Equal(prior.TargetBase)) {
			return JournalEntry{}, ErrIdempotencyConflict
		}
		if prior.State == SyncSaved {
			return *prior, nil
		}
	}
	manifest, err := m.journal.Base(ctx, wc.ID)
	if err != nil {
		return JournalEntry{}, err
	}
	if expectedFrom == nil {
		base, ok := manifest.Files[from]
		if !ok {
			return JournalEntry{}, fmt.Errorf("%w: no recorded base for %q", ErrInvalidRequest, from)
		}
		expectedFrom = &base
	}
	if expectedTo == nil {
		expectedTo = &Version{Exists: false}
	}
	entry := JournalEntry{
		ID: entryID, Kind: OpMove, Path: from, Target: to,
		Base: *expectedFrom, TargetBase: *expectedTo,
		State: SyncPending, UpdatedAt: time.Now().UTC(),
	}
	if prior != nil {
		entry = *prior // retries preserve recorded preconditions and batch identity
	}
	data, cacheErr := m.cache.Get(ctx, wc.ID, from)
	if cacheErr != nil && !errors.Is(cacheErr, ErrNotFound) {
		return JournalEntry{}, cacheErr
	}
	// Record intent before changing cached paths. A failed cache operation
	// leaves a durable move that can be retried or synchronized after restart.
	if err := m.journal.Put(ctx, wc.ID, entry); err != nil {
		return JournalEntry{}, err
	}
	if cacheErr == nil {
		if err := m.cache.Put(ctx, wc.ID, to, data); err != nil {
			return entry, err
		}
		if err := m.cache.Delete(ctx, wc.ID, from); err != nil {
			return entry, err
		}
	}
	return entry, nil
}

// EntryStatus is the observed state of one journal entry against the
// current head of a ref.
type EntryStatus struct {
	Entry JournalEntry
	// Remote is the current version at the entry's path on the resolved ref.
	Remote Version
	// Current reports whether the recorded base still matches Remote.
	Current bool
}

// Status resolves the ref once and reports each entry against that head.
func (m *WorkingCopyManager) Status(ctx context.Context, actor Principal, wc *WorkingCopy, ref RefName) (statuses []EntryStatus, err error) {
	if ref == "" {
		ref = wc.Target
	}
	pin, err := m.files.ResolveAndPin(ctx, actor, wc.Space, ref, m.pinTTL)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, m.files.ReleasePin(ctx, actor, pin.ID)) }()
	entries, err := m.journal.List(ctx, wc.ID)
	if err != nil {
		return nil, err
	}
	statuses = make([]EntryStatus, 0, len(entries))
	for _, entry := range entries {
		remote, err := m.versionAt(ctx, actor, wc.Space, pin.Snapshot, entry.Path)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, EntryStatus{
			Entry: entry, Remote: remote, Current: remote.Equal(entry.Base),
		})
	}
	return statuses, nil
}

// Evict drops cached content that is re-fetchable from the recorded base
// snapshot: every path WITHOUT unconfirmed journal entries. Pending or
// unsaved edits are never evicted.
func (m *WorkingCopyManager) Evict(ctx context.Context, wc *WorkingCopy) (int, error) {
	lock := m.copyLock(wc.ID)
	lock.Lock()
	defer lock.Unlock()
	manifest, err := m.journal.Base(ctx, wc.ID)
	if err != nil {
		return 0, err
	}
	entries, err := m.journal.List(ctx, wc.ID)
	if err != nil {
		return 0, err
	}
	protected := make(map[string]struct{})
	for _, entry := range entries {
		if entry.State == SyncConfirmed {
			continue
		}
		protected[entry.Path] = struct{}{}
		if entry.Target != "" {
			protected[entry.Target] = struct{}{}
		}
	}
	evicted := 0
	for path := range manifest.Files {
		if _, ok := protected[path]; ok {
			continue
		}
		if err := m.cache.Delete(ctx, wc.ID, path); err != nil {
			return evicted, err
		}
		evicted++
	}
	return evicted, nil
}

// Sync synchronizes pending local edits:
//
//  1. Pending entries are checked against the CURRENT target head using
//     their recorded base versions; stale entries become conflicts and are
//     preserved untouched (reconnect conflict detection).
//  2. Clean puts are staged (verified upload) with bounded concurrency and
//     journaled immediately, so a crash mid-pass leaves a retryable state.
//  3. All clean edits are saved as ONE atomic multi-file changeset with a
//     deterministic idempotency key; retries replay the original save.
//  4. When requested, the workspace head is published through the
//     Publisher; a durable receipt confirms acceptance, after which entries
//     become confirmed and the recorded base manifest is refreshed.
func (m *WorkingCopyManager) Sync(ctx context.Context, wc *WorkingCopy, opts SyncOptions) (SyncResult, error) {
	lock := m.copyLock(wc.ID)
	lock.Lock()
	defer lock.Unlock()
	if err := validatePrincipal(opts.Actor); err != nil {
		return SyncResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SyncResult{}, err
	}
	target := opts.Target
	if target == "" {
		target = wc.Target
	}

	entries, err := m.journal.List(ctx, wc.ID, SyncPending, SyncSaved)
	if err != nil {
		return SyncResult{}, err
	}
	if len(entries) == 0 {
		return SyncResult{}, nil
	}

	var saved []JournalEntry
	var clean []JournalEntry
	var conflicts []EntryOutcome
	if anyPending(entries) {
		// Reconnect conflict detection against the current target head.
		pin, err := m.files.ResolveAndPin(ctx, opts.Actor, wc.Space, target, m.pinTTL)
		if err != nil {
			return SyncResult{}, err
		}
		headSnap := pin.Snapshot
		for _, entry := range entries {
			if entry.State == SyncSaved {
				saved = append(saved, entry)
				continue
			}
			remote, err := m.versionAt(ctx, opts.Actor, wc.Space, headSnap, entry.Path)
			if err != nil {
				return SyncResult{}, err
			}
			if !remote.Equal(entry.Base) {
				entry.State, entry.Error = SyncConflict,
					fmt.Sprintf("recorded base on %q is no longer current", target)
				entry.UpdatedAt = time.Now().UTC()
				if err := m.journal.Put(ctx, wc.ID, entry); err != nil {
					return SyncResult{}, err
				}
				conflicts = append(conflicts, outcomeOf(entry))
				continue
			}
			clean = append(clean, entry)
		}
	} else {
		saved = entries
	}

	result := SyncResult{Outcomes: conflicts}

	// Stage content and record preconditions for the clean pending edits.
	if len(clean) > 0 {
		if err := m.stageAndPrepare(ctx, opts.Actor, wc, clean); err != nil {
			return result, err
		}
	}
	if len(clean) == 0 && len(saved) == 0 {
		// Every pending edit is a conflict; there is nothing healthy to
		// synchronize and no proposal to publish.
		return result, nil
	}

	// Save (or replay) the batch as one atomic changeset.
	var ws Workspace
	var publishKey string
	if len(clean) > 0 {
		batchID := syncBatchID(wc.ID, clean)
		publishKey = batchID
		ops := make([]Mutation, 0, len(clean))
		inputs := make([]Dependency, 0, len(clean))
		for _, entry := range clean {
			before := entry.Base
			if entry.WorkspaceBase != nil {
				before = *entry.WorkspaceBase
			}
			switch entry.Kind {
			case OpPut:
				ops = append(ops, Mutation{Kind: OpPut, Path: entry.Path, Before: before, After: entry.Blob})
			case OpDelete:
				ops = append(ops, Mutation{Kind: OpDelete, Path: entry.Path, Before: before})
			case OpMove:
				ops = append(ops, Mutation{
					Kind: OpMove, Path: entry.Path, Before: before,
					Target: entry.Target, BeforeTarget: entry.TargetBase,
				})
				inputs = append(inputs, Dependency{Path: entry.Target, Kind: FileDependency, Expected: entry.TargetBase})
			case OpCopy:
				return result, fmt.Errorf("%w: working-copy copy operations", ErrUnsupportedCapability)
			default:
				return result, fmt.Errorf("%w: unsupported sync op %q", ErrInvalidRequest, entry.Kind)
			}
			inputs = append(inputs, Dependency{Path: entry.Path, Kind: FileDependency, Expected: entry.Base})
		}
		var err error
		ws, err = m.files.Commit(ctx, opts.Actor, wc.Space, wc.Workspace, CommitRequest{
			Ops: ops, Inputs: inputs,
			Message:        "sync batch " + batchID,
			IdempotencyKey: batchID,
		})
		if err != nil {
			result.BatchID = batchID
			return result, err
		}
		for i := range clean {
			clean[i].State, clean[i].BatchID = SyncSaved, batchID
			if err := m.journal.Put(ctx, wc.ID, clean[i]); err != nil {
				return result, err
			}
		}
	} else {
		// Saved-but-unconfirmed edits only: publish the workspace head. The
		// publish key is the batch key of the newest saved changeset, so a
		// retry whose publication actually succeeded replays the ORIGINAL
		// receipt instead of creating a redundant generation.
		ws, err = m.files.Workspace(ctx, opts.Actor, wc.Space, wc.Workspace)
		if err != nil {
			return result, err
		}
		publishKey = saved[len(saved)-1].BatchID
		if publishKey == "" {
			publishKey = "publish-" + syncBatchID(wc.ID, saved)
		}
	}

	result.BatchID = publishKey
	result.Workspace = ws
	result.Changeset = ws.Head

	if !opts.Publish {
		for _, entry := range append(append([]JournalEntry{}, saved...), clean...) {
			result.Outcomes = append(result.Outcomes, outcomeOf(entry))
		}
		return result, nil
	}

	mode := opts.Mode
	if mode == "" {
		mode = PublishRebaseIfSafe
	}
	receipt, err := m.publisher.Publish(ctx, PublishRequest{
		Space: wc.Space, Target: target, Proposal: ws.Head,
		Actor: opts.Actor, Mode: mode, IdempotencyKey: publishKey,
	})
	if err != nil {
		// The saved proposal is preserved; a structured conflict or approval
		// requirement surfaces to the caller.
		return result, err
	}
	result.Published = &receipt
	confirmed := append(append([]JournalEntry{}, saved...), clean...)
	for i := range confirmed {
		confirmed[i].State = SyncConfirmed
		confirmed[i].BatchID = publishKey
		if err := m.journal.Put(ctx, wc.ID, confirmed[i]); err != nil {
			return result, err
		}
		result.Outcomes = append(result.Outcomes, outcomeOf(confirmed[i]))
	}
	// Refresh the recorded base manifest to the published snapshot so later
	// edits derive from the new ground truth.
	if err := m.refreshBase(ctx, opts.Actor, wc, receipt.Published, confirmed); err != nil {
		return result, err
	}
	return result, nil
}

// stageAndPrepare uploads unstaged content with bounded concurrency and
// records each entry immediately after its upload succeeds. Preconditions
// (workspace head versions) are recorded once per entry so retries build
// byte-identical changesets.
func (m *WorkingCopyManager) stageAndPrepare(ctx context.Context, actor Principal, wc *WorkingCopy, clean []JournalEntry) error {
	ws, err := m.files.Workspace(ctx, actor, wc.Space, wc.Workspace)
	if err != nil {
		return err
	}
	wsSnap := Snapshot{Space: wc.Space, Changeset: ws.Head}

	// Preconditions first: one bounded pass, no uploads yet.
	for i := range clean {
		if clean[i].WorkspaceBase != nil {
			continue
		}
		v, err := m.versionAt(ctx, actor, wc.Space, wsSnap, clean[i].Path)
		if err != nil {
			return err
		}
		if clean[i].Kind == OpMove {
			tv, err := m.versionAt(ctx, actor, wc.Space, wsSnap, clean[i].Target)
			if err != nil {
				return err
			}
			clean[i].TargetBase = tv
		}
		clean[i].WorkspaceBase = &v
		if err := m.journal.Put(ctx, wc.ID, clean[i]); err != nil {
			return err
		}
	}

	// Verified uploads, bounded by maxUploads.
	sem := make(chan struct{}, m.maxUploads)
	errs := make(chan error, len(clean))
	var wg sync.WaitGroup
	for i := range clean {
		entry := &clean[i]
		if entry.Kind != OpPut || entry.Blob != nil {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := ctx.Err(); err != nil {
				errs <- err
				return
			}
			content, err := m.cache.Get(ctx, wc.ID, entry.Path)
			if err != nil {
				errs <- fmt.Errorf("local content for %q is missing: %w", entry.Path, err)
				return
			}
			if digestBytes(content) != entry.LocalDigest {
				errs <- fmt.Errorf("%w: local content of %q changed since the edit was recorded", ErrInvalidRequest, entry.Path)
				return
			}
			ref, err := m.files.Stage(ctx, actor, wc.Space, content)
			if err != nil {
				errs <- err
				return
			}
			entry.Blob = &ref
			entry.UpdatedAt = time.Now().UTC()
			if err := m.journal.Put(ctx, wc.ID, *entry); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

// refreshBase updates the recorded manifest from a published snapshot.
func (m *WorkingCopyManager) refreshBase(ctx context.Context, actor Principal, wc *WorkingCopy, published Snapshot, entries []JournalEntry) error {
	manifest, err := m.journal.Base(ctx, wc.ID)
	if err != nil {
		return err
	}
	manifest.Snapshot = published
	for _, entry := range entries {
		switch entry.Kind {
		case OpPut:
			v, err := m.versionAt(ctx, actor, wc.Space, published, entry.Path)
			if err != nil {
				return err
			}
			manifest.Files[entry.Path] = v
		case OpDelete:
			delete(manifest.Files, entry.Path)
		case OpMove:
			delete(manifest.Files, entry.Path)
			v, err := m.versionAt(ctx, actor, wc.Space, published, entry.Target)
			if err != nil {
				return err
			}
			manifest.Files[entry.Target] = v
		case OpCopy:
			return fmt.Errorf("%w: working-copy copy operations", ErrUnsupportedCapability)
		default:
			return fmt.Errorf("%w: unknown sync op %q", ErrInvalidRequest, entry.Kind)
		}
	}
	return m.journal.PutBase(ctx, wc.ID, manifest)
}

// versionAt resolves one path's version in a snapshot; absence is a value.
func (m *WorkingCopyManager) versionAt(ctx context.Context, actor Principal, space SpaceID, snap Snapshot, path string) (Version, error) {
	stat, err := m.files.Stat(ctx, actor, space, snap, path)
	if err == nil {
		return stat.Version, nil
	}
	if errors.Is(err, ErrNotFound) {
		return Version{Exists: false}, nil
	}
	return Version{}, err
}

func anyPending(entries []JournalEntry) bool {
	for _, e := range entries {
		if e.State == SyncPending {
			return true
		}
	}
	return false
}

func outcomeOf(entry JournalEntry) EntryOutcome {
	return EntryOutcome{
		EntryID: entry.ID, Path: entry.Path, Target: entry.Target,
		State: entry.State, Detail: entry.Error,
	}
}

// opEntryID is stable per logical operation so re-editing the same path
// replaces its pending record instead of accumulating duplicates.
func opEntryID(copyID string, kind OpKind, path, target string) string {
	sum := sha256.Sum256([]byte(copyID + "\x00" + string(kind) + "\x00" + path + "\x00" + target))
	return "op-" + hex.EncodeToString(sum[:8])
}

// syncBatchID is deterministic across retries: identical pending edits (same
// IDs, content digests, and recorded preconditions) produce the same
// idempotency key; any new edit produces a fresh one.
func syncBatchID(copyID string, entries []JournalEntry) string {
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		base := e.Base
		wb := e.WorkspaceBase
		ids = append(ids, fmt.Sprintf("%s|%s|%s|%t|%v", e.ID, e.LocalDigest, base.Digest, base.Exists, wb != nil))
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(copyID + "\x00" + fmt.Sprint(ids)))
	return "sync-" + hex.EncodeToString(sum[:8])
}
