package agentfs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"sync/atomic"
	"time"
)

// NewFileID creates an opaque logical document identity. Identity rules are
// enforced by the tree layer and exposed through
// FileEntry.File:
//
//   - Editing content preserves the FileID (an OpPut whose Before names the
//     current digest of an existing path).
//   - Renaming or moving preserves the FileID (OpMove).
//   - Copying creates a new FileID at the destination (OpCopy).
//   - Deleting retires the FileID; deleting and recreating a path therefore
//     yields a new FileID (the recreation is an OpPut whose Before expects
//     absence).
//
// A blob hash identifies exact bytes; a FileID identifies the logical
// document across those changes. Comments, references, and logical file
// history attach to the FileID.
func NewFileID() FileID { return FileID(randomID("f-")) }

// NewChangesetID creates an opaque identifier for a saved changeset.
func NewChangesetID() ChangesetID { return ChangesetID(randomID("c-")) }

var idCounter atomic.Uint64

// randomID prefers cryptographic randomness and falls back to a monotonic
// counter if the system source is unavailable. IDs are opaque; production
// adapters may derive content-addressed identifiers instead.
func randomID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err == nil {
		return prefix + hex.EncodeToString(b)
	}
	return prefix + strconv.FormatUint(idCounter.Add(1), 16) + "-" +
		strconv.FormatInt(time.Now().UnixNano(), 16)
}

// FileEventKind describes a change in a logical document's history.
type FileEventKind string

// File event kinds distinguish content, identity, and annotation changes.
const (
	FileCreated   FileEventKind = "created"
	FileEdited    FileEventKind = "edited"
	FileMoved     FileEventKind = "moved"
	FileCopied    FileEventKind = "copied"
	FileDeleted   FileEventKind = "deleted"
	FileAnnotated FileEventKind = "annotated"
)

// AnnotationKind identifies the meaning of a document annotation.
type AnnotationKind string

// Annotation kinds supported by the file metadata read model.
const (
	AnnotationComment   AnnotationKind = "comment"
	AnnotationReference AnnotationKind = "reference"
)

// FileEvent is one logical-history record for a FileID: identity-bearing
// changes derived from committed changesets, and annotations attached by
// principals.
type FileEvent struct {
	File       FileID
	Space      SpaceID
	Path       string
	Kind       FileEventKind
	Changeset  ChangesetID
	Snapshot   Snapshot
	Author     Principal
	At         time.Time
	Annotation string // comment or reference payload for FileAnnotated
}

// FileMetaStore is a DERIVED read model keyed by stable FileID. It indexes
// committed changesets (for example, by consuming outbox events) and may lag
// them; callers should treat results as hints and resolve content against a
// pinned snapshot. Annotations are recorded as events bound to (FileID,
// snapshot) and never modify file content.
//
// Implementations do not authorize: adapters must apply current ACL checks
// before exposing entries, history, or annotations to a caller.
type FileMetaStore interface {
	// Resolve returns the current path and content of a FileID within one
	// pinned snapshot. Absence is reported as ok == false, not an error.
	Resolve(ctx context.Context, actor Principal, space SpaceID, snap Snapshot, file FileID) (FileEntry, bool, error)
	// History lists logical events for a FileID, oldest first, with an
	// opaque continuation cursor.
	History(ctx context.Context, actor Principal, space SpaceID, file FileID, cursor string, limit int) ([]FileEvent, string, error)
	// Annotate attaches a comment or reference to a FileID.
	Annotate(ctx context.Context, actor Principal, space SpaceID, file FileID, kind AnnotationKind, payload string, idempotencyKey string) (FileEvent, error)
}

// digestBytes is the shared SHA-256 helper for content digests.
func digestBytes(content []byte) Digest {
	sum := sha256.Sum256(content)
	return Digest(hex.EncodeToString(sum[:]))
}
