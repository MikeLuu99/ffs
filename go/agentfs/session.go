package agentfs

import (
	"context"
	"time"
)

// SessionMode selects observation or editing within a document session.
type SessionMode string

// Session modes distinguish presence from editing access.
const (
	SessionObserve SessionMode = "observe"
	SessionEdit    SessionMode = "edit"
)

// SessionHandle binds a live session to a file and durable base version.
type SessionHandle struct {
	ID     string
	Space  SpaceID
	File   FileID
	Mode   SessionMode
	Base   Version
	Expiry time.Time
}

// DocumentSession is the FUTURE boundary for live collaboration on one
// supported document format. It is deliberately separate from the durable
// file system and separates three kinds of state:
//
//   - Ephemeral presence: who is attached to the document right now.
//     Presence is transient and never recorded in changesets.
//   - Live editing state: the ephemeral operation/CRDT state of the session.
//     It is authoritative for NOTHING; the durable file system always
//     remains ground truth.
//   - Durable file content: bytes and changesets, written through the same
//     verified ingest, preconditions, and publication policy as everything
//     else.
//
// Checkpointing: a session must periodically save durable checkpoints as
// ordinary workspace-scoped changesets (the same save path as any other
// client) so a crashed session loses only its ephemeral tail. A checkpoint
// is a save, never a publication; publication policy still applies.
//
// External file writes: a session binds to a FileID at a specific base
// Version. Before continuing (checkpoint, further edits) it must verify the
// durable version still equals its base; a divergence requires the session
// to rebase onto the new durable state or restart. Sessions may never
// bypass preconditions or publication validation.
//
// v1 does not implement sessions. The unsupported implementation below is
// the v1 contract so adapters surface an explicit error instead of
// approximating live collaboration with polling writes.
type DocumentSession interface {
	Attach(ctx context.Context, actor Principal, space SpaceID, file FileID, mode SessionMode, base Version) (SessionHandle, error)
	// Checkpoint saves the session's durable state as an ordinary changeset.
	Checkpoint(ctx context.Context, actor Principal, session SessionHandle, message, idempotencyKey string) (Workspace, error)
	Close(ctx context.Context, actor Principal, session SessionHandle) error
}

type unsupportedSession struct{}

// UnsupportedDocumentSession returns the v1 placeholder for the session
// boundary. Every method returns ErrUnsupportedCapability.
func UnsupportedDocumentSession() DocumentSession { return unsupportedSession{} }

func (unsupportedSession) Attach(_ context.Context, _ Principal, _ SpaceID, _ FileID, _ SessionMode, _ Version) (SessionHandle, error) {
	return SessionHandle{}, ErrUnsupportedCapability
}

func (unsupportedSession) Checkpoint(_ context.Context, _ Principal, _ SessionHandle, _, _ string) (Workspace, error) {
	return Workspace{}, ErrUnsupportedCapability
}

func (unsupportedSession) Close(_ context.Context, _ Principal, _ SessionHandle) error {
	return ErrUnsupportedCapability
}
