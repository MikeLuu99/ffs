package agentfs

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// In-memory reference implementations. They exist to exercise the
// application services and the coordinator contracts; they do not establish
// database durability, real content merging, or production outbox delivery.
// The tree store clones whole roots (O(N) per apply), which the production
// incremental-tree adapter must never do.
// ---------------------------------------------------------------------------

type memBlobStore struct {
	mu    sync.Mutex
	blobs map[BlobRef][]byte
}

func newMemBlobs() *memBlobStore {
	return &memBlobStore{blobs: make(map[BlobRef][]byte)}
}

func (b *memBlobStore) Put(ctx context.Context, expected BlobRef, src io.Reader) (BlobRef, error) {
	if err := ctx.Err(); err != nil {
		return BlobRef{}, err
	}
	content, err := io.ReadAll(src)
	if err != nil {
		return BlobRef{}, err
	}
	if int64(len(content)) != expected.Size {
		return BlobRef{}, fmt.Errorf("%w: staged %d bytes, expected %d", ErrInvalidRequest, len(content), expected.Size)
	}
	if digestBytes(content) != expected.SHA256 {
		return BlobRef{}, fmt.Errorf("%w: staged digest does not match the expected digest", ErrInvalidRequest)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.blobs[expected]; ok {
		// Content addressing makes an equal ref idempotent.
		if string(existing) != string(content) {
			return BlobRef{}, fmt.Errorf("%w: blob ref collision", ErrInvalidRequest)
		}
		return expected, nil
	}
	b.blobs[expected] = content
	return expected, nil
}

func (b *memBlobStore) Open(ctx context.Context, ref BlobRef) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	content, ok := b.blobs[ref]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]byte, len(content))
	copy(out, content)
	return io.NopCloser(strings.NewReader(string(out))), nil
}

// ---------------------------------------------------------------------------
// Tree store with FileID identity semantics.
// ---------------------------------------------------------------------------

type memNode struct {
	dir      bool
	entry    FileEntry
	children map[string]*memNode
}

func newDirNode() *memNode { return &memNode{dir: true, children: make(map[string]*memNode)} }

type memTreeStore struct {
	mu    sync.Mutex
	trees map[SpaceID]map[TreeID]*memNode
	next  uint64
}

func newMemTrees() *memTreeStore {
	return &memTreeStore{trees: make(map[SpaceID]map[TreeID]*memNode)}
}

func (s *memTreeStore) store(space SpaceID, root *memNode) TreeID {
	s.next++
	id := TreeID("t-" + strconv.FormatUint(s.next, 16))
	if s.trees[space] == nil {
		s.trees[space] = make(map[TreeID]*memNode)
	}
	s.trees[space][id] = root
	return id
}

func (s *memTreeStore) rootFor(space SpaceID, root TreeID) (*memNode, error) {
	if root == "" {
		return newDirNode(), nil // empty base: the genesis of a space
	}
	spaceTrees, ok := s.trees[space]
	if !ok {
		return nil, ErrNotFound
	}
	node, ok := spaceTrees[root]
	if !ok {
		return nil, ErrNotFound
	}
	return node, nil
}

func cloneNode(n *memNode) *memNode {
	out := &memNode{dir: n.dir, entry: n.entry}
	if n.dir {
		out.children = make(map[string]*memNode, len(n.children))
		for name, child := range n.children {
			out.children[name] = cloneNode(child)
		}
	}
	return out
}

func lookupNode(root *memNode, path string) *memNode {
	node := root
	for _, part := range strings.Split(path, "/") {
		if node == nil || !node.dir {
			return nil
		}
		node = node.children[part]
	}
	return node
}

func setNode(root *memNode, path string, file *memNode) {
	parts := strings.Split(path, "/")
	node := root
	for _, part := range parts[:len(parts)-1] {
		child, ok := node.children[part]
		if !ok || !child.dir {
			child = newDirNode()
			node.children[part] = child
		}
		node = child
	}
	node.children[parts[len(parts)-1]] = file
}

func deleteNode(root *memNode, path string) {
	parts := strings.Split(path, "/")
	node := root
	for _, part := range parts[:len(parts)-1] {
		node = node.children[part]
		if node == nil || !node.dir {
			return
		}
	}
	delete(node.children, parts[len(parts)-1])
}

func (s *memTreeStore) fileVersion(n *memNode) Version {
	return Version{Exists: true, Digest: n.entry.Blob.SHA256}
}

// fingerprintLocked computes the deterministic version of a node: content
// digest for files, a hash over sorted child names and versions for dirs.
func (s *memTreeStore) fingerprintLocked(n *memNode) Version {
	if !n.dir {
		return s.fileVersion(n)
	}
	h := fnv.New64a()
	names := make([]string, 0, len(n.children))
	for name := range n.children {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		child := n.children[name]
		cv := s.fingerprintLocked(child)
		// In-memory hash writers cannot fail; a failure is a broken test fixture.
		if _, err := fmt.Fprintf(h, "%s\x00%v\x00%s\x00", name, cv.Exists, cv.Digest); err != nil {
			panic(err)
		}
	}
	return Version{Exists: true, Digest: Digest(strconv.FormatUint(h.Sum64(), 16))}
}

func (s *memTreeStore) Apply(ctx context.Context, space SpaceID, base TreeID, delta []Mutation) (TreeID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	baseRoot, err := s.rootFor(space, base)
	if err != nil {
		return "", err
	}
	working := cloneNode(baseRoot) // test impl: O(N) clone; production must share structure
	for _, m := range delta {
		if err := m.ValidMutation(); err != nil {
			return "", err
		}
		if m.Kind == OpMove || m.Kind == OpCopy {
			if m.Target == m.Path || strings.HasPrefix(m.Target, m.Path+"/") {
				return "", fmt.Errorf("%w: target %q nests inside source %q", ErrInvalidRequest, m.Target, m.Path)
			}
		}
		if err := s.applyOneLocked(working, m); err != nil {
			return "", err
		}
	}
	return s.store(space, working), nil
}

func (s *memTreeStore) applyOneLocked(root *memNode, m Mutation) error {
	conflict := func(path string, actual Version) *Conflict {
		return &Conflict{Kind: ErrContentConflict, Path: path, Expected: m.Before, Actual: actual}
	}
	switch m.Kind {
	case OpPut:
		existing := lookupNode(root, m.Path)
		if existing != nil && !existing.dir {
			if !m.Before.Exists {
				return conflict(m.Path, s.fileVersion(existing))
			}
			if s.fileVersion(existing).Digest != m.Before.Digest {
				return conflict(m.Path, s.fileVersion(existing))
			}
			// Editing content preserves the FileID.
			setNode(root, m.Path, &memNode{entry: FileEntry{Path: m.Path, File: existing.entry.File, Blob: *m.After}})
			return nil
		}
		if existing != nil && existing.dir {
			return conflict(m.Path, s.fingerprintLocked(existing))
		}
		if m.Before.Exists {
			return conflict(m.Path, Version{Exists: false})
		}
		// Creation mints a new FileID.
		setNode(root, m.Path, &memNode{entry: FileEntry{Path: m.Path, File: NewFileID(), Blob: *m.After}})
		return nil
	case OpDelete:
		existing := lookupNode(root, m.Path)
		if existing == nil || existing.dir {
			return conflict(m.Path, Version{Exists: false})
		}
		if s.fileVersion(existing).Digest != m.Before.Digest {
			return conflict(m.Path, s.fileVersion(existing))
		}
		deleteNode(root, m.Path)
		return nil
	case OpMove, OpCopy:
		source := lookupNode(root, m.Path)
		if source == nil || source.dir {
			return conflict(m.Path, Version{Exists: false})
		}
		if s.fileVersion(source).Digest != m.Before.Digest {
			return conflict(m.Path, s.fileVersion(source))
		}
		target := lookupNode(root, m.Target)
		if target != nil && target.dir {
			return &Conflict{Kind: ErrContentConflict, Path: m.Target, Expected: m.BeforeTarget, Actual: s.fingerprintLocked(target)}
		}
		var actualTarget Version
		if target != nil {
			actualTarget = s.fileVersion(target)
		}
		if !m.BeforeTarget.Equal(actualTarget) {
			return &Conflict{Kind: ErrContentConflict, Path: m.Target, Expected: m.BeforeTarget, Actual: actualTarget}
		}
		file := &memNode{entry: FileEntry{Path: m.Target, File: source.entry.File, Blob: source.entry.Blob}}
		if m.Kind == OpCopy {
			file.entry.File = NewFileID() // copying creates a new FileID
		}
		if m.Kind == OpMove {
			deleteNode(root, m.Path)
		}
		setNode(root, m.Target, file)
		return nil
	}
	return fmt.Errorf("%w: unknown op kind %q", ErrInvalidRequest, m.Kind)
}

func (s *memTreeStore) Lookup(ctx context.Context, space SpaceID, root TreeID, path string) (FileEntry, error) {
	if err := ctx.Err(); err != nil {
		return FileEntry{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	baseRoot, err := s.rootFor(space, root)
	if err != nil {
		return FileEntry{}, err
	}
	node := lookupNode(baseRoot, path)
	if node == nil || node.dir {
		return FileEntry{}, ErrNotFound
	}
	entry := node.entry
	entry.Path = path
	return entry, nil
}

func (s *memTreeStore) Fingerprint(ctx context.Context, space SpaceID, root TreeID, prefix string) (Version, error) {
	if err := ctx.Err(); err != nil {
		return Version{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	baseRoot, err := s.rootFor(space, root)
	if err != nil {
		return Version{}, err
	}
	node := baseRoot
	if prefix != "" {
		node = lookupNode(baseRoot, prefix)
		if node == nil {
			return Version{Exists: false}, nil
		}
	}
	return s.fingerprintLocked(node), nil
}

func (s *memTreeStore) List(ctx context.Context, space SpaceID, root TreeID, prefix, cursor string, limit int) (TreePage, error) {
	if err := ctx.Err(); err != nil {
		return TreePage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	baseRoot, err := s.rootFor(space, root)
	if err != nil {
		return TreePage{}, err
	}
	// Cursor binds to the root it was issued for.
	afterPath := ""
	if cursor != "" {
		rootPart, pathPart, ok := strings.Cut(cursor, "\x00")
		if !ok || rootPart != string(root) {
			return TreePage{}, fmt.Errorf("%w: cursor does not belong to this root", ErrInvalidRequest)
		}
		afterPath = pathPart
	}

	var paths []string
	var collect func(node *memNode, path string)
	collect = func(node *memNode, path string) {
		if !node.dir {
			paths = append(paths, path)
			return
		}
		names := make([]string, 0, len(node.children))
		for name := range node.children {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			child := path
			if child != "" {
				child += "/"
			}
			collect(node.children[name], child+name)
		}
	}
	collect(baseRoot, "")

	out := TreePage{}
	for _, path := range paths {
		if prefix != "" && path != prefix && !strings.HasPrefix(path, prefix+"/") {
			continue
		}
		if afterPath != "" && path <= afterPath {
			continue
		}
		if limit > 0 && len(out.Entries) >= limit {
			out.NextCursor = string(root) + "\x00" + out.Entries[len(out.Entries)-1].Path
			break
		}
		node := lookupNode(baseRoot, path)
		if node == nil || node.dir {
			return TreePage{}, ErrNotFound
		}
		entry := node.entry
		entry.Path = path
		out.Entries = append(out.Entries, entry)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Draft store: workspace CAS, idempotent saves, changeset/root records.
// ---------------------------------------------------------------------------

type memDraftStore struct {
	mu    sync.Mutex
	blobs *memBlobStore
	ws    map[WorkspaceID]*Workspace
	cs    map[ChangesetID]Changeset
	roots map[ChangesetID]TreeID
	ops   map[operationScope]struct {
		hash Digest
		ws   Workspace
	}
}

func newMemDrafts(blobs *memBlobStore) *memDraftStore {
	return &memDraftStore{
		blobs: blobs,
		ws:    make(map[WorkspaceID]*Workspace),
		cs:    make(map[ChangesetID]Changeset),
		roots: make(map[ChangesetID]TreeID),
		ops: make(map[operationScope]struct {
			hash Digest
			ws   Workspace
		}),
	}
}

func (d *memDraftStore) CreateWorkspace(ctx context.Context, actor Principal, space SpaceID, base Snapshot, id WorkspaceID, _ time.Duration) (Workspace, error) {
	if err := ctx.Err(); err != nil {
		return Workspace{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.ws[id]; ok {
		if existing.Space == space && existing.Base == base.Changeset && existing.Owner == actor.ID {
			return *existing, nil // idempotent bootstrap
		}
		return Workspace{}, fmt.Errorf("%w: workspace %q already exists", ErrInvalidRequest, id)
	}
	ws := Workspace{
		ID: id, Owner: actor.ID, Space: space,
		Base: base.Changeset, Head: base.Changeset, Generation: 1,
	}
	d.ws[id] = &ws
	return ws, nil
}

func (d *memDraftStore) Workspace(_ context.Context, space SpaceID, id WorkspaceID) (Workspace, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	ws, ok := d.ws[id]
	if !ok || ws.Space != space {
		return Workspace{}, ErrNotFound
	}
	return *ws, nil
}

func (d *memDraftStore) Save(ctx context.Context, draft SaveDraft) (Workspace, error) {
	if err := ctx.Err(); err != nil {
		return Workspace{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	sc := scope(draft.Operation)
	if record, ok := d.ops[sc]; ok {
		if record.hash != draft.Operation.RequestHash {
			return Workspace{}, ErrIdempotencyConflict
		}
		return record.ws, nil // matching retry replays the original save
	}
	ws, ok := d.ws[draft.Workspace]
	if !ok {
		return Workspace{}, ErrNotFound
	}
	if draft.ExpectedGeneration != ws.Generation {
		return Workspace{}, ErrGenerationConflict
	}
	cs := draft.Changeset
	if cs.ID == "" || cs.Space != ws.Space || cs.Parent != ws.Head {
		return Workspace{}, fmt.Errorf("%w: changeset identity, space, or parent is invalid", ErrInvalidRequest)
	}
	for _, m := range cs.Mutations {
		if err := m.ValidMutation(); err != nil {
			return Workspace{}, err
		}
		if m.Kind == OpPut {
			if _, err := d.blobs.Open(ctx, *m.After); err != nil {
				return Workspace{}, fmt.Errorf("%w: put of %q references an unregistered blob", ErrInvalidRequest, m.Path)
			}
		}
	}
	ws.Generation++
	ws.Head = cs.ID
	stored := Changeset{
		ID: cs.ID, SchemaVersion: cs.SchemaVersion, Space: cs.Space, Parent: cs.Parent,
		Source: cs.Source, Author: cs.Author, Message: cs.Message, CreatedAt: cs.CreatedAt,
		Mutations: cloneMutations(cs.Mutations), Inputs: cloneDependencies(cs.Inputs),
	}
	d.cs[cs.ID] = stored
	d.roots[cs.ID] = draft.Root
	d.ops[sc] = struct {
		hash Digest
		ws   Workspace
	}{draft.Operation.RequestHash, *ws}
	return *ws, nil
}

func (d *memDraftStore) Load(_ context.Context, space SpaceID, id ChangesetID) (Changeset, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cs, ok := d.cs[id]
	if !ok || cs.Space != space {
		return Changeset{}, ErrNotFound
	}
	return cs, nil
}

func (d *memDraftStore) Root(_ context.Context, snap Snapshot) (TreeID, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	root, ok := d.roots[snap.Changeset]
	if !ok {
		return "", ErrNotFound
	}
	return root, nil
}

// ---------------------------------------------------------------------------
// Refs, publication store, and snapshot store sharing one ref registry.
// ---------------------------------------------------------------------------

type memRefState struct {
	head ChangesetID
	gen  uint64
}

type memRefs struct {
	mu   sync.Mutex
	refs map[string]*memRefState
}

func newMemRefs() *memRefs { return &memRefs{refs: make(map[string]*memRefState)} }

func refKey(space SpaceID, name RefName) string { return string(space) + "\x00" + string(name) }

func (r *memRefs) set(space SpaceID, name RefName, head ChangesetID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.refs[refKey(space, name)]
	if !ok {
		state = &memRefState{}
		r.refs[refKey(space, name)] = state
	}
	state.head, state.gen = head, state.gen+1
}

func (r *memRefs) get(space SpaceID, name RefName) (Ref, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.refs[refKey(space, name)]
	if !ok {
		return Ref{}, false
	}
	return Ref{Space: space, Name: name, Version: RefVersion{Head: state.head, Generation: state.gen}}, true
}

type memPubStore struct {
	refs    *memRefs
	mu      sync.Mutex
	records map[operationScope]storedReceipt
	outbox  int
}

func newMemPubs(refs *memRefs) *memPubStore {
	return &memPubStore{refs: refs, records: make(map[operationScope]storedReceipt)}
}

func (p *memPubStore) LookupPublication(ctx context.Context, op OperationKey) (Receipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	record, found := p.records[scope(op)]
	if found && record.hash != op.RequestHash {
		return Receipt{}, false, ErrIdempotencyConflict
	}
	return record.receipt, found, nil
}

func (p *memPubStore) Head(ctx context.Context, _ Principal, space SpaceID, name RefName) (Ref, error) {
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	if ref, ok := p.refs.get(space, name); ok {
		return ref, nil
	}
	return Ref{}, ErrNotFound
}

func (p *memPubStore) lookupLocked(op OperationKey) (Receipt, bool, error) {
	record, found := p.records[scope(op)]
	if found && record.hash != op.RequestHash {
		return Receipt{}, false, ErrIdempotencyConflict
	}
	return record.receipt, found, nil
}

func (p *memPubStore) TryPublish(ctx context.Context, pub Publication) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if receipt, found, err := p.lookupLocked(pub.Operation); found || err != nil {
		return receipt, err
	}
	current, ok := p.refs.get(pub.Request.Space, pub.Request.Target)
	if !ok {
		return Receipt{}, ErrNotFound
	}
	if current.Version != pub.Expected.Version {
		return Receipt{}, ErrHeadMoved
	}
	if pub.Candidate.Parent != current.Version.Head || pub.Candidate.Snapshot.Space != current.Space {
		return Receipt{}, ErrInvalidCandidate
	}
	if pub.Permit.Actor != pub.Request.Actor.ID || pub.Permit.Target != pub.Request.Target ||
		pub.Permit.Candidate != pub.Candidate.Snapshot || pub.Permit.InputDigest != pub.Candidate.InputDigest {
		return Receipt{}, ErrInvalidPermit
	}
	p.refs.set(pub.Request.Space, pub.Request.Target, pub.Candidate.Snapshot.Changeset)
	published, _ := p.refs.get(pub.Request.Space, pub.Request.Target)
	receipt := Receipt{
		Ref: published, Proposal: pub.Request.Proposal, Published: pub.Candidate.Snapshot,
		PublishedAt: time.Now().UTC(),
	}
	p.outbox++
	p.records[scope(pub.Operation)] = storedReceipt{hash: pub.Operation.RequestHash, receipt: receipt}
	return receipt, nil
}

type memSnapshotStore struct {
	refs *memRefs
	mu   sync.Mutex
	pins map[string]struct {
		pin   Pin
		owner PrincipalID
	}
	next int
}

func newMemSnapshots(refs *memRefs) *memSnapshotStore {
	return &memSnapshotStore{refs: refs, pins: make(map[string]struct {
		pin   Pin
		owner PrincipalID
	})}
}

func (s *memSnapshotStore) ResolveAndPin(ctx context.Context, actor Principal, space SpaceID, ref RefName, ttl time.Duration) (Pin, error) {
	if err := ctx.Err(); err != nil {
		return Pin{}, err
	}
	head, ok := s.refs.get(space, ref)
	if !ok {
		return Pin{}, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	pin := Pin{
		Snapshot:  Snapshot{Space: space, Changeset: head.Version.Head},
		ID:        fmt.Sprintf("pin-%d", s.next),
		ExpiresAt: time.Now().Add(ttl).UTC(),
	}
	s.pins[pin.ID] = struct {
		pin   Pin
		owner PrincipalID
	}{pin, actor.ID}
	return pin, nil
}

func (s *memSnapshotStore) RenewPin(ctx context.Context, actor Principal, pinID string, ttl time.Duration) (Pin, error) {
	if err := ctx.Err(); err != nil {
		return Pin{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.pins[pinID]
	if !ok {
		return Pin{}, ErrNotFound
	}
	if record.owner != actor.ID {
		return Pin{}, ErrDenied
	}
	record.pin.ExpiresAt = time.Now().Add(ttl).UTC()
	s.pins[pinID] = record
	return record.pin, nil
}

func (s *memSnapshotStore) ReleasePin(ctx context.Context, actor Principal, pinID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.pins[pinID]
	if !ok {
		return ErrNotFound
	}
	if record.owner != actor.ID {
		return ErrDenied
	}
	delete(s.pins, pinID)
	return nil
}

// ---------------------------------------------------------------------------
// Authorizers.
// ---------------------------------------------------------------------------

type allowAll struct{}

func (allowAll) CheckAccess(context.Context, Principal, SpaceID, string, AccessKind) error {
	return nil
}

type prefixDeny struct {
	deny string
	next Authorizer
}

func (p prefixDeny) CheckAccess(ctx context.Context, actor Principal, space SpaceID, path string, access AccessKind) error {
	if strings.HasPrefix(path, p.deny) {
		return ErrDenied
	}
	if p.next != nil {
		return p.next.CheckAccess(ctx, actor, space, path, access)
	}
	return nil
}

// ---------------------------------------------------------------------------
// A candidate builder for saved workspaces. It rebases the saved delta onto
// the CURRENT head: declared dependencies are validated first (they carry
// the main-relative beliefs), then the mutations apply to the head tree
// with their preconditions transformed to those same declared bases. A
// stale dependency or a failed precondition never moves a ref.
// ---------------------------------------------------------------------------

type workspaceBuilder struct {
	drafts DraftStore
	trees  TreeStore
	// record persists a rebased candidate changeset and its tree. The
	// production adapter saves candidates through the SQL metadata layer;
	// the memstore records them in memory.
	record func(space SpaceID, cs Changeset, root TreeID)
}

func (b workspaceBuilder) Prepare(ctx context.Context, request PublishRequest, head Ref) (Candidate, error) {
	cs, err := b.drafts.Load(ctx, request.Space, request.Proposal)
	if err != nil {
		return Candidate{}, err
	}
	headRoot, err := b.drafts.Root(ctx, Snapshot{Space: request.Space, Changeset: head.Version.Head})
	if err != nil {
		return Candidate{}, err
	}

	// Declared dependencies are validated against the current head.
	expectations := make(map[string]Version)
	for _, dep := range cs.Inputs {
		var actual Version
		switch dep.Kind {
		case FileDependency:
			entry, err := b.trees.Lookup(ctx, request.Space, headRoot, dep.Path)
			switch {
			case err == nil:
				actual = Version{Exists: true, Digest: entry.Blob.SHA256}
			case errors.Is(err, ErrNotFound):
				actual = Version{Exists: false}
			default:
				return Candidate{}, err
			}
		case SubtreeDependency:
			actual, err = b.trees.Fingerprint(ctx, request.Space, headRoot, dep.Path)
			if err != nil {
				return Candidate{}, err
			}
		default:
			return Candidate{}, fmt.Errorf("%w: unknown dependency kind %q", ErrInvalidRequest, dep.Kind)
		}
		if !actual.Equal(dep.Expected) {
			return Candidate{}, &Conflict{Kind: ErrDependencyChanged, Path: dep.Path, Expected: dep.Expected, Actual: actual}
		}
		if dep.Kind == FileDependency {
			expectations[dep.Path] = dep.Expected
		}
	}

	// Rebase the mutations onto the head tree. Preconditions become the
	// declared main-relative bases where the agent declared them.
	rebased := cloneMutations(cs.Mutations)
	for i := range rebased {
		if base, ok := expectations[rebased[i].Path]; ok {
			rebased[i].Before = base
		}
		if rebased[i].Target != "" {
			if base, ok := expectations[rebased[i].Target]; ok {
				rebased[i].BeforeTarget = base
			}
		}
	}
	newRoot, err := b.trees.Apply(ctx, request.Space, headRoot, rebased)
	if err != nil {
		return Candidate{}, err // structured content conflict; proposal preserved
	}

	candidate := cs
	candidate.ID = NewChangesetID()
	candidate.Parent = head.Version.Head
	candidate.Source = cs.ID
	b.record(request.Space, candidate, newRoot)
	return Candidate{
		Snapshot: Snapshot{Space: request.Space, Changeset: candidate.ID},
		Parent:   head.Version.Head, Tree: newRoot, InputDigest: inputDigest(cs.Inputs),
	}, nil
}

func inputDigest(deps []Dependency) Digest {
	h := fnv.New64a()
	sorted := cloneDependencies(deps)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Path != sorted[j].Path {
			return sorted[i].Path < sorted[j].Path
		}
		return sorted[i].Kind < sorted[j].Kind
	})
	for _, dep := range sorted {
		// In-memory hash writers cannot fail; a failure is a broken test fixture.
		if _, err := fmt.Fprintf(h, "%s\x00%s\x00%v\x00%s\x00", dep.Path, dep.Kind, dep.Expected.Exists, dep.Expected.Digest); err != nil {
			panic(err)
		}
	}
	return Digest("inputs-" + strconv.FormatUint(h.Sum64(), 16))
}

// ---------------------------------------------------------------------------
// Test stack wiring.
// ---------------------------------------------------------------------------

type coreStack struct {
	blobs     *memBlobStore
	trees     *memTreeStore
	drafts    *memDraftStore
	snaps     *memSnapshotStore
	pubs      *memPubStore
	auth      Authorizer
	files     FileService
	publisher *Publisher
}

func testActor(id string) Principal {
	return Principal{ID: PrincipalID(id), Owner: "team-a", Kind: "agent", Tool: "test", RunID: "run-" + id}
}

func testCaps() Capabilities {
	return NewCapabilities("agentfs-test", "v1", TransferLimits{
		MaxFileSize: 1 << 20, MaxOperationsPerCommit: 100, MaxConcurrentUploads: 4,
	}, CapConditionalWrites, CapStableFileIDs, CapSnapshots, CapAtomicMultiFile)
}

func newCoreStack(t *testing.T, caps Capabilities, auth Authorizer) *coreStack {
	t.Helper()
	if auth == nil {
		auth = allowAll{}
	}
	blobs := newMemBlobs()
	trees := newMemTrees()
	refs := newMemRefs()
	drafts := newMemDrafts(blobs)
	snaps := newMemSnapshots(refs)
	pubs := newMemPubs(refs)
	files, err := NewFileService(blobs, trees, drafts, snaps, auth, caps)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPublisher(pubs, workspaceBuilder{
		drafts: drafts, trees: trees,
		record: func(_ SpaceID, cs Changeset, root TreeID) {
			drafts.mu.Lock()
			defer drafts.mu.Unlock()
			drafts.cs[cs.ID] = cs
			drafts.roots[cs.ID] = root
		},
	}, policyFunc(allowCandidate), 4)
	if err != nil {
		t.Fatal(err)
	}
	stack := &coreStack{blobs: blobs, trees: trees, drafts: drafts, snaps: snaps, pubs: pubs, auth: auth, files: files, publisher: publisher}
	stack.seedSpace(context.Background(), "acme")
	return stack
}

// seedSpace creates a genesis changeset (empty tree) and main.
func (s *coreStack) seedSpace(ctx context.Context, space SpaceID) {
	root, err := s.trees.Apply(ctx, space, "", nil)
	if err != nil {
		panic(err)
	}
	genesis := Changeset{ID: NewChangesetID(), SchemaVersion: 1, Space: space, Message: "genesis"}
	s.drafts.mu.Lock()
	s.drafts.cs[genesis.ID] = genesis
	s.drafts.roots[genesis.ID] = root
	s.drafts.mu.Unlock()
	s.pubs.refs.set(space, "main", genesis.ID)
}

// publishDirect saves and publishes a single-file change through the full
// stack (workspace, verified ingest, publication) — the "other writer" in
// concurrency tests.
func (s *coreStack) publishDirect(ctx context.Context, actor Principal, space SpaceID, path string, expected *Version, content []byte, key string) (receipt Receipt, ws Workspace, err error) {
	pin, err := s.files.ResolveAndPin(ctx, actor, space, "main", time.Minute)
	if err != nil {
		return Receipt{}, Workspace{}, err
	}
	defer func() { err = errors.Join(err, s.files.ReleasePin(ctx, actor, pin.ID)) }()
	ws, err = s.files.CreateWorkspace(ctx, actor, space, pin.Snapshot, WorkspaceID("ws-"+key), 0)
	if err != nil {
		return Receipt{}, Workspace{}, err
	}
	ws, err = s.files.WriteFile(ctx, actor, space, ws.ID, WriteFileRequest{
		Path: path, Expected: expected, Content: content,
		Message: "direct write", IdempotencyKey: key,
	})
	if err != nil {
		return Receipt{}, Workspace{}, err
	}
	receipt, err = s.publisher.Publish(ctx, PublishRequest{
		Space: space, Target: "main", Proposal: ws.Head, Actor: actor,
		Mode: PublishRebaseIfSafe, IdempotencyKey: "pub-" + key,
	})
	return receipt, ws, err
}

// ---------------------------------------------------------------------------
// Local journal and cache implementations for working-copy tests.
// ---------------------------------------------------------------------------

type memJournal struct {
	mu     sync.Mutex
	copies map[string]*memCopyJournal
}

type memCopyJournal struct {
	entries []JournalEntry
	base    BaseManifest
}

func newMemJournal() *memJournal { return &memJournal{copies: make(map[string]*memCopyJournal)} }

func (j *memJournal) copy(id string) *memCopyJournal {
	if j.copies[id] == nil {
		j.copies[id] = &memCopyJournal{}
	}
	return j.copies[id]
}

func (j *memJournal) Put(_ context.Context, copyID string, entry JournalEntry) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.copy(copyID)
	for i := range c.entries {
		if c.entries[i].ID == entry.ID {
			c.entries[i] = entry
			return nil
		}
	}
	c.entries = append(c.entries, entry)
	return nil
}

func (j *memJournal) Get(_ context.Context, copyID, entryID string) (JournalEntry, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, entry := range j.copy(copyID).entries {
		if entry.ID == entryID {
			return entry, nil
		}
	}
	return JournalEntry{}, ErrNotFound
}

func (j *memJournal) List(_ context.Context, copyID string, states ...SyncState) ([]JournalEntry, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []JournalEntry
	for _, entry := range j.copy(copyID).entries {
		if len(states) == 0 {
			out = append(out, entry)
			continue
		}
		for _, state := range states {
			if entry.State == state {
				out = append(out, entry)
				break
			}
		}
	}
	return out, nil
}

func (j *memJournal) Delete(_ context.Context, copyID, entryID string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.copy(copyID)
	for i := range c.entries {
		if c.entries[i].ID == entryID {
			c.entries = append(c.entries[:i], c.entries[i+1:]...)
			return nil
		}
	}
	return ErrNotFound
}

func (j *memJournal) PutBase(_ context.Context, copyID string, base BaseManifest) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	files := make(map[string]Version, len(base.Files))
	for path, v := range base.Files {
		files[path] = v
	}
	base.Files = files
	j.copy(copyID).base = base
	return nil
}

func (j *memJournal) Base(_ context.Context, copyID string) (BaseManifest, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	base := j.copy(copyID).base
	files := make(map[string]Version, len(base.Files))
	for path, v := range base.Files {
		files[path] = v
	}
	base.Files = files
	return base, nil
}

type memCache struct {
	mu   sync.Mutex
	data map[string]map[string][]byte
}

func newMemCache() *memCache { return &memCache{data: make(map[string]map[string][]byte)} }

func (c *memCache) Put(_ context.Context, copyID, path string, content []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data[copyID] == nil {
		c.data[copyID] = make(map[string][]byte)
	}
	c.data[copyID][path] = append([]byte(nil), content...)
	return nil
}

func (c *memCache) Get(_ context.Context, copyID, path string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	content, ok := c.data[copyID][path]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), content...), nil
}

func (c *memCache) Delete(_ context.Context, copyID, path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data[copyID], path)
	return nil
}

// countingService wraps a FileService and records the peak concurrent Stage
// calls, verifying the sync staging bound.
type countingService struct {
	FileService
	mu        sync.Mutex
	inflight  int
	peak      int
	stageGate chan struct{}
}

func (c *countingService) Stage(ctx context.Context, actor Principal, space SpaceID, content []byte) (BlobRef, error) {
	c.mu.Lock()
	c.inflight++
	if c.inflight > c.peak {
		c.peak = c.inflight
	}
	c.mu.Unlock()
	if c.stageGate != nil {
		<-c.stageGate
	}
	defer func() {
		c.mu.Lock()
		c.inflight--
		c.mu.Unlock()
	}()
	return c.FileService.Stage(ctx, actor, space, content)
}
