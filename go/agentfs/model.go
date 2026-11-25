// Package agentfs defines the storage contracts and publication workflow for an
// online, versioned workspace shared by humans and agents.
//
// Blob bytes and saved changesets are immutable. Saving a changeset does not
// move a shared ref. Publishing moves a ref in one metadata transaction.
// Public methods take a context; implementations must support concurrent calls.
package agentfs

import (
	"fmt"
	"io/fs"
	"strings"
	"time"
)

type (
	// SpaceID identifies a transaction and administration boundary.
	SpaceID string
	// PrincipalID identifies an authenticated human, agent, or service.
	PrincipalID string
	// WorkspaceID identifies a private draft lineage.
	WorkspaceID string
	// ChangesetID identifies an immutable saved delta.
	ChangesetID string
	// TreeID identifies an immutable materialized path index.
	TreeID string
	// Digest identifies exact content or a deterministic subtree fingerprint.
	Digest string
	// RefName names a mutable shared pointer within a space.
	RefName string
	// FileID identifies a logical document, independent of its path and of
	// its bytes. The tree layer mints a new FileID when a path gains a file
	// that had none (create, copy destination, recreate after delete) and
	// preserves the FileID across content edits (OpPut on an existing path)
	// and moves/renames (OpMove). A blob digest identifies exact bytes; a
	// FileID identifies the document so comments, references, and logical
	// history can attach to it.
	FileID string
)

// Principal must come from a trusted authentication layer. Owner identifies
// the human or team responsible for an agent. RunID distinguishes executions.
type Principal struct {
	ID    PrincipalID `json:"id"`
	Owner PrincipalID `json:"owner"`
	Kind  string      `json:"kind"` // human, agent, or service
	Tool  string      `json:"tool"`
	RunID string      `json:"run_id"`
}

// BlobRef is scoped to a space; knowing a digest never grants access.
// SHA256 identifies the exact file bytes, independent of storage encoding.
type BlobRef struct {
	Space  SpaceID `json:"space"`
	SHA256 Digest  `json:"sha256"`
	Size   int64   `json:"size"`
}

// Version represents both a known value and known absence. For an absent path,
// Exists is false and Digest is empty. File versions use the content digest;
// subtree versions use a deterministic tree fingerprint.
type Version struct {
	Exists bool   `json:"exists"`
	Digest Digest `json:"digest"`
}

// Equal reports whether two versions describe the same state. Absence is a
// value: an absent version differs from any present one.
func (v Version) Equal(other Version) bool {
	return v.Exists == other.Exists && v.Digest == other.Digest
}

// Mutation is a file operation with an explicit old-value precondition.
// The identity semantics are:
//   - OpPut on an absent path (Before.Exists == false) creates the file and
//     mints a new FileID.
//   - OpPut on an existing path (Before names the current digest) edits it
//     and preserves the FileID.
//   - OpMove preserves the FileID at the new path.
//   - OpCopy places a new FileID at the destination.
//   - OpDelete retires the FileID; recreating the path later (an OpPut whose
//     Before expects absence) mints a new FileID.
//
// Paths use canonical slash-separated relative names without '.' or '..'.
type Mutation struct {
	Kind OpKind `json:"kind"`
	// Path is the subject of the operation: the written/deleted path, or the
	// source of a move/copy.
	Path string `json:"path"`
	// Before is the expected current Version at Path. Creation requires the
	// absent version; replacement and deletion require a specific digest.
	Before Version `json:"before"`
	// After is the new content for OpPut and must be nil otherwise.
	After *BlobRef `json:"after,omitempty"`
	// Target is the destination path for OpMove and OpCopy.
	Target string `json:"target,omitempty"`
	// BeforeTarget is the expected Version at Target for OpMove/OpCopy,
	// normally the absent version: overwriting a destination must be explicit.
	BeforeTarget Version `json:"before_target"`
}

// OpKind identifies a conditional file mutation.
type OpKind string

// Mutation kinds supported by tree stores.
const (
	OpPut    OpKind = "put"    // create or replace file content at Path
	OpDelete OpKind = "delete" // remove the file at Path
	OpMove   OpKind = "move"   // move Path -> Target, preserving file identity
	OpCopy   OpKind = "copy"   // copy Path -> Target; the copy gets a new FileID
)

// ValidMutation checks kind-specific fields and path canonicality for a
// mutation whose content is already staged (After set for puts).
func (m Mutation) ValidMutation() error {
	if m.Kind == OpPut && m.After == nil {
		return fmt.Errorf("%w: put requires content", ErrInvalidRequest)
	}
	return m.validShape()
}

// validShape checks everything that does not require staged content.
func (m Mutation) validShape() error {
	if !validTreePath(m.Path) {
		return fmt.Errorf("%w: invalid path %q", ErrInvalidRequest, m.Path)
	}
	switch m.Kind {
	case OpPut:
		if m.Target != "" {
			return fmt.Errorf("%w: put must not name a target", ErrInvalidRequest)
		}
	case OpDelete:
		if m.After != nil {
			return fmt.Errorf("%w: delete must not carry content", ErrInvalidRequest)
		}
		if m.Target != "" {
			return fmt.Errorf("%w: delete must not name a target", ErrInvalidRequest)
		}
	case OpMove, OpCopy:
		if m.After != nil {
			return fmt.Errorf("%w: move/copy must not carry content", ErrInvalidRequest)
		}
		if !validTreePath(m.Target) {
			return fmt.Errorf("%w: invalid target %q", ErrInvalidRequest, m.Target)
		}
		if m.Target == m.Path {
			return fmt.Errorf("%w: move/copy target equals source", ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: unknown operation kind %q", ErrInvalidRequest, m.Kind)
	}
	return nil
}

func validTreePath(p string) bool {
	if p == "" || p == "." || p == ".." || strings.ContainsAny(p, "\\\x00") {
		return false
	}
	return fs.ValidPath(p)
}

// PutMutation creates or replaces the file at path.
func PutMutation(path string, before Version, blob BlobRef) Mutation {
	return Mutation{Kind: OpPut, Path: path, Before: before, After: &blob}
}

// DeleteMutation removes the file at path.
func DeleteMutation(path string, before Version) Mutation {
	return Mutation{Kind: OpDelete, Path: path, Before: before}
}

// MoveMutation renames path to target, preserving file identity.
func MoveMutation(path string, before Version, target string, beforeTarget Version) Mutation {
	return Mutation{Kind: OpMove, Path: path, Before: before, Target: target, BeforeTarget: beforeTarget}
}

// CopyMutation duplicates path at target; the copy receives a new FileID.
func CopyMutation(path string, before Version, target string, beforeTarget Version) Mutation {
	return Mutation{Kind: OpCopy, Path: path, Before: before, Target: target, BeforeTarget: beforeTarget}
}

// DependencyKind distinguishes exact-file inputs from subtree inputs.
type DependencyKind string

// Dependency kinds determine how a declared input is fingerprinted.
const (
	FileDependency    DependencyKind = "file"
	SubtreeDependency DependencyKind = "subtree"
)

// Dependency records an input used to produce a changeset. A subtree
// fingerprint detects directory membership changes as well as file edits.
type Dependency struct {
	Path     string         `json:"path"`
	Kind     DependencyKind `json:"kind"`
	Expected Version        `json:"expected"`
}

// Changeset is a persistence DTO for an immutable, single-parent delta.
// Rebased candidates retain their original proposal in Source.
// Stores must defensively copy slices and nested pointers on save/load.
// Content IDs use a versioned canonical encoding defined by the implementation;
// ordinary json.Marshal of this DTO is not the storage format specification.
type Changeset struct {
	ID            ChangesetID  `json:"id"`
	SchemaVersion uint32       `json:"schema_version"`
	Space         SpaceID      `json:"space"`
	Parent        ChangesetID  `json:"parent"`
	Source        ChangesetID  `json:"source"`
	Author        Principal    `json:"author"`
	Message       string       `json:"message"`
	CreatedAt     time.Time    `json:"created_at"`
	Mutations     []Mutation   `json:"mutations"`
	Inputs        []Dependency `json:"inputs"`
}

// Snapshot identifies a stable view. A Pin protects its reachable objects
// against garbage collection for the documented lifetime.
type Snapshot struct {
	Space     SpaceID     `json:"space"`
	Changeset ChangesetID `json:"changeset"`
}

// Pin protects a snapshot's reachable objects from garbage collection until expiry.
type Pin struct {
	Snapshot  Snapshot
	ID        string
	ExpiresAt time.Time
}

// RefVersion includes a generation to detect head movement even if a ref moves
// away from a commit and then back to it (the ABA problem).
type RefVersion struct {
	Head       ChangesetID `json:"head"`
	Generation uint64      `json:"generation"`
}

// Ref is a shared named pointer and its current concurrency version.
type Ref struct {
	Space   SpaceID
	Name    RefName
	Version RefVersion
}

// Workspace is a private draft lineage and a retention root. Updating its head
// has its own CAS generation; it does not update main.
type Workspace struct {
	ID         WorkspaceID
	Owner      PrincipalID
	Space      SpaceID
	Base       ChangesetID
	Head       ChangesetID
	Generation uint64
	ExpiresAt  time.Time
}

// SaveDraft binds a prepared derived root to the immutable delta being saved.
// Workspace identity and expected generation are checked against stored state;
// a caller cannot supply a replacement owner or retention policy.
type SaveDraft struct {
	Operation          OperationKey
	Workspace          WorkspaceID
	ExpectedGeneration uint64
	Changeset          Changeset
	Root               TreeID
}

// FileEntry binds one path in one tree root to a stable logical file
// identity and its current content. File survives edits and moves; see
// FileID for the identity rules.
type FileEntry struct {
	Path string
	File FileID
	Blob BlobRef
}

// TreePage contains one snapshot-bound page of file entries.
type TreePage struct {
	Entries    []FileEntry
	NextCursor string
}

// PublishMode selects strict head matching or conservative rebase retries.
type PublishMode string

const (
	// PublishStrict requires exactly the caller's expected ref version.
	PublishStrict PublishMode = "strict"
	// PublishRebaseIfSafe permits bounded server-side retries. The builder must
	// still validate all write preconditions and declared input dependencies.
	PublishRebaseIfSafe PublishMode = "rebase_if_safe"
)

// PublishRequest proposes a saved changeset for a shared ref.
type PublishRequest struct {
	Space          SpaceID     `json:"space"`
	Target         RefName     `json:"target"`
	Proposal       ChangesetID `json:"proposal"`
	Actor          Principal   `json:"actor"`
	Mode           PublishMode `json:"mode"`
	Expected       *RefVersion `json:"expected"`
	IdempotencyKey string      `json:"idempotency_key"`
}

// OperationKey scopes retry identity. RequestHash covers the original request,
// excluding any candidate generated during a retry against a newer head.
type OperationKey struct {
	Space       SpaceID
	Actor       PrincipalID
	Operation   string
	Key         string
	RequestHash Digest
}

// Candidate is prepared outside the publication transaction. Its changeset and
// required tree are already saved, readable, and retained until the attempt ends.
type Candidate struct {
	Snapshot    Snapshot
	Parent      ChangesetID
	Tree        TreeID
	InputDigest Digest
}

// Permit binds policy evidence to the actor, target, candidate, and dependencies.
// The metadata backend verifies the evidence and current policy revision again
// inside the publication transaction; this value is not client authorization.
type Permit struct {
	Actor          PrincipalID
	Target         RefName
	Candidate      Snapshot
	InputDigest    Digest
	PolicyRevision string
	Evidence       string
}

// Publication binds one atomic ref update to its prepared candidate and permit.
type Publication struct {
	Operation OperationKey
	Request   PublishRequest
	Expected  Ref
	Candidate Candidate
	Permit    Permit
}

// Receipt records the original durable result of a publication request.
type Receipt struct {
	Ref         Ref
	Proposal    ChangesetID
	Published   Snapshot
	PublishedAt time.Time
}
