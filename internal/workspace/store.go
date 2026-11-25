package workspace

import (
	"context"
	"io"
	"time"
)

// BlobStore owns verified immutable bytes. Implementations verify the complete
// size/digest before success, enforce create-only storage, and scope all keys
// by space. Large uploads can add a staging/presign adapter behind this port.
// Put may leave an unreferenced blob after cancellation; it cannot publish it.
// Open is an internal trusted operation, not a public digest-based API.
type BlobStore interface {
	Put(ctx context.Context, expected BlobRef, src io.Reader) (BlobRef, error)
	Open(ctx context.Context, ref BlobRef) (io.ReadCloser, error)
}

// TreeStore is a derived immutable read model, separate from changeset history.
// Apply atomically checks Before values and produces a structurally shared root;
// it never mutates base. Implementations must not rewrite the full namespace.
// A deterministic fingerprint includes names and child versions in a subtree.
// Every read uses an explicit root. List cursors bind to root/prefix and use a
// bounded page size. Lookup returns ErrNotFound for absence. Implementations
// own the cursor encoding. None of these operations authorize a public caller.
type TreeStore interface {
	Apply(ctx context.Context, space SpaceID, base TreeID, delta []Mutation) (TreeID, error)
	Lookup(ctx context.Context, space SpaceID, root TreeID, path string) (FileEntry, error)
	Fingerprint(ctx context.Context, space SpaceID, root TreeID, prefix string) (Version, error)
	List(ctx context.Context, space SpaceID, root TreeID, prefix, cursor string, limit int) (TreePage, error)
}

// DraftStore saves work without moving a shared ref. Save validates canonical
// paths, content IDs, parent scope, metadata, and registered verified blobs.
// It atomically saves the changeset, retention root, audit/outbox entries, and
// idempotency result. Caller inputs must be copied before they are retained.
// A workspace head update, when requested, uses its own expected generation.
// Matching retries of one operation key return the ORIGINAL workspace;
// a reused key with a different request hash is ErrIdempotencyConflict.
// The canonical changeset encoder and SQL implementation are later adapters.
type DraftStore interface {
	// CreateWorkspace bootstraps a private draft lineage based on a pinned
	// snapshot. Creating an existing workspace with identical parameters is
	// idempotent; a conflicting reuse is an error.
	CreateWorkspace(ctx context.Context, actor Principal, space SpaceID, base Snapshot, id ID, ttl time.Duration) (Workspace, error)
	// Workspace returns the current workspace state (head, generation).
	Workspace(ctx context.Context, space SpaceID, id ID) (Workspace, error)
	Save(ctx context.Context, draft SaveDraft) (Workspace, error)
	Load(ctx context.Context, space SpaceID, id ChangesetID) (Changeset, error)
	Root(ctx context.Context, snapshot Snapshot) (TreeID, error)
}

// SnapshotStore resolves and pins one ref version atomically with respect to
// GC. Renewal/release is authorized to the pin owner. Reads must still apply
// current ACLs; a pin protects retention and is not an access capability.
type SnapshotStore interface {
	ResolveAndPin(ctx context.Context, actor Principal, space SpaceID, ref RefName, ttl time.Duration) (Pin, error)
	RenewPin(ctx context.Context, actor Principal, pinID string, ttl time.Duration) (Pin, error)
	ReleasePin(ctx context.Context, actor Principal, pinID string) error
}

// CandidateBuilder checks a proposal's input dependencies and write
// preconditions against head, optionally performs a supported merge, then saves
// a ready candidate whose parent is head. A conflict never moves a ref.
// It must apply current read permissions and retain preparation outputs.
type CandidateBuilder interface {
	Prepare(ctx context.Context, request PublishRequest, head Ref) (Candidate, error)
}

// PublicationPolicy performs slow checks outside the metadata transaction:
// schema validation, review/approval lookup, and authorization evaluation.
// Its permit is bound to the actor, target, candidate, inputs, and policy revision.
// It returns ErrApprovalRequired instead of waiting forever for a human.
type PublicationPolicy interface {
	Evaluate(ctx context.Context, request PublishRequest, candidate Candidate) (Permit, error)
}

// PublicationStore owns the publication linearization point. Its methods are
// safe for concurrent calls and read authoritative metadata, not stale caches.
//
// LookupPublication and TryPublish reject a reused operation key whose request
// hash differs. Matching retries return the ORIGINAL receipt, including its
// original ref generation, even after main advances again.
// Looking up a retry result still requires current permission to see it.
//
// TryPublish is ONE metadata transaction:
//   - replay an existing matching idempotency result, if present;
//   - compare both expected head and generation;
//   - verify candidate scope/parent/tree readiness and retained verified blobs;
//   - enforce current ACLs and bound policy evidence/revision;
//   - move the ref and increment its generation;
//   - insert audit/outbox records and the idempotency receipt together.
//
// On ErrHeadMoved, nothing is published. No network, merge, or slow policy call
// runs inside this transaction. Acknowledgement follows configured DB durability.
// There is deliberately no independent SetRef or EmitEvent method here.
// Adding reservations later also requires fencing tokens in Publication and
// atomic validation of those tokens in TryPublish.
type PublicationStore interface {
	LookupPublication(ctx context.Context, operation OperationKey) (Receipt, bool, error)
	Head(ctx context.Context, actor Principal, space SpaceID, name RefName) (Ref, error)
	TryPublish(ctx context.Context, publication Publication) (Receipt, error)
}
