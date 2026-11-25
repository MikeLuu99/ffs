package agentfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// AccessKind is the operation an authorizer is consulted for. An empty path
// means the space level (space-level reads, workspace management, unattached
// content staging).
type AccessKind string

// Access kinds distinguish reading content from changing workspace state.
const (
	AccessRead  AccessKind = "read"
	AccessWrite AccessKind = "write"
)

// Authorizer is consulted by every application service before exposing
// paths, entries, or bytes. REST, CLI, MCP, and future mount adapters must
// share the same application services so authorization rules cannot diverge
// between transports.
type Authorizer interface {
	CheckAccess(ctx context.Context, actor Principal, space SpaceID, path string, access AccessKind) error
}

// FileStat describes one path as observed in one immutable snapshot.
type FileStat struct {
	Space    SpaceID
	Path     string
	File     FileID
	Version  Version // content version; absence is expressible elsewhere
	Size     int64
	Snapshot Snapshot
}

// EntryRef reconstructs the blob reference for a present file.
func (f FileStat) EntryRef() BlobRef {
	return BlobRef{Space: f.Space, SHA256: f.Version.Digest, Size: f.Size}
}

// FileService is the filesystem-facing application service: familiar file
// operations over the core stores, with explicit preconditions and atomic
// multi-file commits. Every transport (REST, CLI, MCP, future mount) uses
// this service; none of them touch the storage ports directly.
//
// Save and publish remain separate operations: WriteFile/Commit record
// durable, unpublished changesets on a private workspace; publication goes
// through the Publisher and its policy validation.
type FileService interface {
	CapabilityDiscoverer

	// ResolveAndPin resolves a ref once and protects the resulting snapshot
	// against garbage collection for the pin lifetime.
	ResolveAndPin(ctx context.Context, actor Principal, space SpaceID, ref RefName, ttl time.Duration) (Pin, error)
	RenewPin(ctx context.Context, actor Principal, pinID string, ttl time.Duration) (Pin, error)
	ReleasePin(ctx context.Context, actor Principal, pinID string) error

	// CreateWorkspace bootstraps a private draft lineage on a snapshot.
	CreateWorkspace(ctx context.Context, actor Principal, space SpaceID, base Snapshot, id WorkspaceID, ttl time.Duration) (Workspace, error)
	// Workspace returns the current state of a private workspace.
	Workspace(ctx context.Context, actor Principal, space SpaceID, id WorkspaceID) (Workspace, error)

	// ReadFile returns the content and stat of one path in one snapshot.
	ReadFile(ctx context.Context, actor Principal, space SpaceID, snap Snapshot, path string) ([]byte, FileStat, error)
	Stat(ctx context.Context, actor Principal, space SpaceID, snap Snapshot, path string) (FileStat, error)
	// List pages one snapshot's tree; the cursor is bound to that snapshot.
	List(ctx context.Context, actor Principal, space SpaceID, snap Snapshot, prefix, cursor string, limit int) (TreePage, error)

	// Stage verifies and stores content without attaching it to a path.
	Stage(ctx context.Context, actor Principal, space SpaceID, content []byte) (BlobRef, error)

	// WriteFile records one file change as a durable, unpublished changeset.
	// Expected is REQUIRED: a replacement names the digest it replaces and a
	// creation declares expected absence. A nil Expected is rejected with
	// ErrPreconditionRequired, never defaulted.
	WriteFile(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req WriteFileRequest) (Workspace, error)
	// Commit records several mutations as ONE atomic changeset: readers of
	// any snapshot never see a partial group.
	Commit(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req CommitRequest) (Workspace, error)
	MoveFile(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req MoveFileRequest) (Workspace, error)
	CopyFile(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req CopyFileRequest) (Workspace, error)
	DeleteFile(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req DeleteFileRequest) (Workspace, error)
}

// WriteFileRequest saves one conditional content replacement or creation.
type WriteFileRequest struct {
	Path string
	// Expected must be non-nil. Replacement writes name the version they
	// replace; creation declares expected absence.
	Expected *Version
	Content  []byte
	Message  string
	// IdempotencyKey is required: retries with the same key and request
	// return the originally saved workspace.
	IdempotencyKey string
}

// CommitRequest saves a group of conditional mutations atomically.
type CommitRequest struct {
	Ops []Mutation
	// Inputs are declared read dependencies validated again at publication.
	Inputs []Dependency
	// Message and IdempotencyKey are required.
	Message        string
	IdempotencyKey string
}

// MoveFileRequest renames a file while preserving its logical identity.
type MoveFileRequest struct {
	From           string
	To             string
	ExpectedFrom   *Version // required
	ExpectedTo     *Version // required; normally expected absence
	Message        string
	IdempotencyKey string
}

// CopyFileRequest creates a file with a new identity from existing content.
type CopyFileRequest struct {
	From           string
	To             string
	ExpectedFrom   *Version // required
	ExpectedTo     *Version // required; normally expected absence
	Message        string
	IdempotencyKey string
}

// DeleteFileRequest removes a file at an explicitly expected version.
type DeleteFileRequest struct {
	Path           string
	Expected       *Version // required
	Message        string
	IdempotencyKey string
}

type fileService struct {
	blobs  BlobStore
	trees  TreeStore
	drafts DraftStore
	snaps  SnapshotStore
	auth   Authorizer
	caps   Capabilities
}

// NewFileService wires the filesystem-facing service over the core ports.
// The service refuses to start over a backend that cannot enforce the core
// guarantees: conditional writes, stable file identities, snapshots, and
// atomic multi-file commits. A reduced backend must present reduced
// capabilities through a different adapter, never through silent downgrade.
func NewFileService(blobs BlobStore, trees TreeStore, drafts DraftStore, snaps SnapshotStore, auth Authorizer, caps Capabilities) (FileService, error) {
	if blobs == nil || trees == nil || drafts == nil || snaps == nil || auth == nil {
		return nil, fmt.Errorf("%w: all storage ports and an authorizer are required", ErrInvalidRequest)
	}
	for _, capability := range []Capability{CapConditionalWrites, CapStableFileIDs, CapSnapshots, CapAtomicMultiFile} {
		if !caps.Has(capability) {
			return nil, fmt.Errorf("%w: file service requires %s", ErrUnsupportedCapability, capability)
		}
	}
	return &fileService{blobs: blobs, trees: trees, drafts: drafts, snaps: snaps, auth: auth, caps: caps}, nil
}

func (s *fileService) DiscoverCapabilities(ctx context.Context) (Capabilities, error) {
	if err := ctx.Err(); err != nil {
		return Capabilities{}, err
	}
	return s.caps, nil
}

func (s *fileService) check(ctx context.Context, actor Principal, space SpaceID, path string, access AccessKind) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.auth.CheckAccess(ctx, actor, space, path, access)
}

func (s *fileService) ResolveAndPin(ctx context.Context, actor Principal, space SpaceID, ref RefName, ttl time.Duration) (Pin, error) {
	if err := s.check(ctx, actor, space, "", AccessRead); err != nil {
		return Pin{}, err
	}
	return s.snaps.ResolveAndPin(ctx, actor, space, ref, ttl)
}

func (s *fileService) RenewPin(ctx context.Context, actor Principal, pinID string, ttl time.Duration) (Pin, error) {
	if err := ctx.Err(); err != nil {
		return Pin{}, err
	}
	return s.snaps.RenewPin(ctx, actor, pinID, ttl)
}

func (s *fileService) ReleasePin(ctx context.Context, actor Principal, pinID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.snaps.ReleasePin(ctx, actor, pinID)
}

func (s *fileService) CreateWorkspace(ctx context.Context, actor Principal, space SpaceID, base Snapshot, id WorkspaceID, ttl time.Duration) (Workspace, error) {
	if err := validatePrincipal(actor); err != nil {
		return Workspace{}, err
	}
	if err := s.check(ctx, actor, space, "", AccessWrite); err != nil {
		return Workspace{}, err
	}
	if base.Space != space || base.Changeset == "" {
		return Workspace{}, fmt.Errorf("%w: workspace base must be a snapshot of the same space", ErrInvalidRequest)
	}
	return s.drafts.CreateWorkspace(ctx, actor, space, base, id, ttl)
}

func (s *fileService) Workspace(ctx context.Context, actor Principal, space SpaceID, id WorkspaceID) (Workspace, error) {
	if err := s.check(ctx, actor, space, "", AccessWrite); err != nil {
		return Workspace{}, err
	}
	return s.drafts.Workspace(ctx, space, id)
}

func (s *fileService) rootOf(ctx context.Context, space SpaceID, snap Snapshot) (TreeID, error) {
	if snap.Space != space || snap.Changeset == "" {
		return "", fmt.Errorf("%w: snapshot must belong to the space", ErrInvalidRequest)
	}
	return s.drafts.Root(ctx, snap)
}

func (s *fileService) Stat(ctx context.Context, actor Principal, space SpaceID, snap Snapshot, path string) (FileStat, error) {
	if !validTreePath(path) {
		return FileStat{}, fmt.Errorf("%w: invalid path %q", ErrInvalidRequest, path)
	}
	if err := s.check(ctx, actor, space, path, AccessRead); err != nil {
		return FileStat{}, err
	}
	root, err := s.rootOf(ctx, space, snap)
	if err != nil {
		return FileStat{}, err
	}
	entry, err := s.trees.Lookup(ctx, space, root, path)
	if err != nil {
		return FileStat{}, err
	}
	return FileStat{
		Space: space, Path: path, File: entry.File,
		Version: Version{Exists: true, Digest: entry.Blob.SHA256},
		Size:    entry.Blob.Size, Snapshot: snap,
	}, nil
}

func (s *fileService) ReadFile(ctx context.Context, actor Principal, space SpaceID, snap Snapshot, path string) ([]byte, FileStat, error) {
	stat, err := s.Stat(ctx, actor, space, snap, path)
	if err != nil {
		return nil, FileStat{}, err
	}
	if s.caps.Limits.MaxFileSize > 0 && stat.Size > s.caps.Limits.MaxFileSize {
		return nil, FileStat{}, fmt.Errorf("%w: %d bytes exceeds the file limit", ErrLimitExceeded, stat.Size)
	}
	reader, err := s.blobs.Open(ctx, stat.EntryRef())
	if err != nil {
		return nil, FileStat{}, err
	}
	content, readErr := io.ReadAll(reader)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return nil, FileStat{}, err
	}
	return content, stat, nil
}

func (s *fileService) List(ctx context.Context, actor Principal, space SpaceID, snap Snapshot, prefix, cursor string, limit int) (TreePage, error) {
	if prefix != "" && !validTreePath(prefix) {
		return TreePage{}, fmt.Errorf("%w: invalid prefix %q", ErrInvalidRequest, prefix)
	}
	if err := s.check(ctx, actor, space, prefix, AccessRead); err != nil {
		return TreePage{}, err
	}
	root, err := s.rootOf(ctx, space, snap)
	if err != nil {
		return TreePage{}, err
	}
	return s.trees.List(ctx, space, root, prefix, cursor, limit)
}

// Stage verifies and stores unattached content. It never publishes a file;
// a changeset must reference the returned blob for it to appear anywhere.
func (s *fileService) Stage(ctx context.Context, actor Principal, space SpaceID, content []byte) (BlobRef, error) {
	if err := validatePrincipal(actor); err != nil {
		return BlobRef{}, err
	}
	if err := s.check(ctx, actor, space, "", AccessWrite); err != nil {
		return BlobRef{}, err
	}
	if s.caps.Limits.MaxFileSize > 0 && int64(len(content)) > s.caps.Limits.MaxFileSize {
		return BlobRef{}, fmt.Errorf("%w: %d bytes exceeds the file limit", ErrLimitExceeded, len(content))
	}
	expected := BlobRef{Space: space, SHA256: digestBytes(content), Size: int64(len(content))}
	return s.blobs.Put(ctx, expected, bytes.NewReader(content))
}

func (s *fileService) WriteFile(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req WriteFileRequest) (Workspace, error) {
	if req.Expected == nil {
		return Workspace{}, fmt.Errorf("%w: WriteFile requires the expected version or expected absence", ErrPreconditionRequired)
	}
	op := Mutation{Kind: OpPut, Path: req.Path, Before: *req.Expected}
	return s.commit(ctx, actor, space, workspace, commitInput{
		ops:     []Mutation{op},
		message: req.Message, key: req.IdempotencyKey,
		content: map[string][]byte{req.Path: req.Content},
	})
}

func (s *fileService) MoveFile(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req MoveFileRequest) (Workspace, error) {
	if req.ExpectedFrom == nil || req.ExpectedTo == nil {
		return Workspace{}, fmt.Errorf("%w: MoveFile requires expected versions for both paths", ErrPreconditionRequired)
	}
	return s.commit(ctx, actor, space, workspace, commitInput{
		ops:     []Mutation{MoveMutation(req.From, *req.ExpectedFrom, req.To, *req.ExpectedTo)},
		message: req.Message, key: req.IdempotencyKey,
	})
}

func (s *fileService) CopyFile(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req CopyFileRequest) (Workspace, error) {
	if req.ExpectedFrom == nil || req.ExpectedTo == nil {
		return Workspace{}, fmt.Errorf("%w: CopyFile requires expected versions for both paths", ErrPreconditionRequired)
	}
	return s.commit(ctx, actor, space, workspace, commitInput{
		ops:     []Mutation{CopyMutation(req.From, *req.ExpectedFrom, req.To, *req.ExpectedTo)},
		message: req.Message, key: req.IdempotencyKey,
	})
}

func (s *fileService) DeleteFile(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req DeleteFileRequest) (Workspace, error) {
	if req.Expected == nil {
		return Workspace{}, fmt.Errorf("%w: DeleteFile requires the expected version", ErrPreconditionRequired)
	}
	return s.commit(ctx, actor, space, workspace, commitInput{
		ops:     []Mutation{DeleteMutation(req.Path, *req.Expected)},
		message: req.Message, key: req.IdempotencyKey,
	})
}

func (s *fileService) Commit(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, req CommitRequest) (Workspace, error) {
	return s.commit(ctx, actor, space, workspace, commitInput{
		ops: req.Ops, inputs: req.Inputs, message: req.Message, key: req.IdempotencyKey,
	})
}

// commitInput carries one save request. Convenience single-file writes carry
// bytes that still need staging; sync batches pass complete, pre-staged
// mutations.
type commitInput struct {
	ops     []Mutation
	inputs  []Dependency
	message string
	key     string
	content map[string][]byte
}

func (s *fileService) commit(ctx context.Context, actor Principal, space SpaceID, workspace WorkspaceID, in commitInput) (Workspace, error) {
	if err := validatePrincipal(actor); err != nil {
		return Workspace{}, err
	}
	if in.message == "" || in.key == "" {
		return Workspace{}, fmt.Errorf("%w: message and idempotency key are required", ErrInvalidRequest)
	}
	if len(in.ops) == 0 {
		return Workspace{}, fmt.Errorf("%w: a commit requires at least one operation", ErrInvalidRequest)
	}
	if err := RequireCapability(s.caps, CapConditionalWrites); err != nil {
		return Workspace{}, err
	}
	if err := RequireCapability(s.caps, CapStableFileIDs); err != nil {
		return Workspace{}, err
	}
	if len(in.ops) > 1 {
		if err := RequireCapability(s.caps, CapAtomicMultiFile); err != nil {
			return Workspace{}, err
		}
	}
	if s.caps.Limits.MaxOperationsPerCommit > 0 && len(in.ops) > s.caps.Limits.MaxOperationsPerCommit {
		return Workspace{}, fmt.Errorf("%w: %d operations exceeds the commit limit", ErrLimitExceeded, len(in.ops))
	}

	// Content staging happens AFTER authorization and BEFORE the metadata
	// write. A failure here may leave a verified orphan blob; it can never
	// publish or attach bytes, and no bytes move for unauthorized paths.
	for i := range in.ops {
		op := &in.ops[i]
		if err := op.validShape(); err != nil {
			return Workspace{}, err
		}
		if op.Kind == OpPut && op.After == nil {
			if _, ok := in.content[op.Path]; !ok {
				return Workspace{}, fmt.Errorf("%w: put of %q is missing staged content", ErrInvalidRequest, op.Path)
			}
		}
		if err := s.check(ctx, actor, space, op.Path, AccessWrite); err != nil {
			return Workspace{}, err
		}
		if op.Target != "" {
			if err := s.check(ctx, actor, space, op.Target, AccessWrite); err != nil {
				return Workspace{}, err
			}
		}
		if op.Kind == OpPut && op.After == nil {
			ref, err := s.Stage(ctx, actor, space, in.content[op.Path])
			if err != nil {
				return Workspace{}, err
			}
			after := ref
			op.After = &after
		}
	}

	ws, err := s.drafts.Workspace(ctx, space, workspace)
	if err != nil {
		return Workspace{}, err
	}
	baseRoot, err := s.rootOf(ctx, space, Snapshot{Space: space, Changeset: ws.Head})
	if err != nil {
		return Workspace{}, err
	}
	newRoot, err := s.trees.Apply(ctx, space, baseRoot, in.ops)
	if err != nil {
		return Workspace{}, err // structured *Conflict; nothing was saved
	}
	changeset := Changeset{
		ID: NewChangesetID(), SchemaVersion: 1,
		Space: space, Parent: ws.Head,
		Author: actor, Message: in.message,
		CreatedAt: time.Now().UTC(),
		Mutations: cloneMutations(in.ops), Inputs: cloneDependencies(in.inputs),
	}
	draft := SaveDraft{
		Operation: OperationKey{
			Space: space, Actor: actor.ID, Operation: "save",
			Key: in.key, RequestHash: saveRequestHash(space, workspace, ws.Head, in.ops, in.inputs, in.message),
		},
		Workspace: workspace, ExpectedGeneration: ws.Generation,
		Changeset: changeset, Root: newRoot,
	}
	return s.drafts.Save(ctx, draft)
}

// saveRequestHash is stable across retries of one logical save: it covers
// the workspace, parent, operations, declared inputs, and message, but not
// freshly minted changeset IDs or timestamps.
func saveRequestHash(space SpaceID, workspace WorkspaceID, parent ChangesetID, ops []Mutation, inputs []Dependency, message string) Digest {
	wire := struct {
		Version   int          `json:"version"`
		Space     SpaceID      `json:"space"`
		Workspace WorkspaceID  `json:"workspace"`
		Parent    ChangesetID  `json:"parent"`
		Ops       []Mutation   `json:"ops"`
		Inputs    []Dependency `json:"inputs"`
		Message   string       `json:"message"`
	}{Version: 1, Space: space, Workspace: workspace, Parent: parent, Ops: ops, Inputs: inputs, Message: message}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return Digest(hex.EncodeToString(sum[:]))
}

func cloneMutations(ops []Mutation) []Mutation {
	out := make([]Mutation, len(ops))
	for i := range ops {
		out[i] = ops[i]
		if ops[i].After != nil {
			after := *ops[i].After
			out[i].After = &after
		}
	}
	return out
}

func cloneDependencies(deps []Dependency) []Dependency {
	if deps == nil {
		return nil
	}
	out := make([]Dependency, len(deps))
	copy(out, deps)
	return out
}

func validatePrincipal(actor Principal) error {
	if actor.ID == "" || actor.Owner == "" || actor.Kind == "" {
		return fmt.Errorf("%w: principal id, owner, and kind are required", ErrInvalidRequest)
	}
	return nil
}
