package agentfs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func releaseTestPin(ctx context.Context, t *testing.T, files FileService, actor Principal, pinID string) {
	t.Helper()
	if err := files.ReleasePin(ctx, actor, pinID); err != nil {
		t.Errorf("release test pin: %v", err)
	}
}

func absentVersion() *Version { v := Version{Exists: false}; return &v }

func presentVersion(digest Digest) *Version { return &Version{Exists: true, Digest: digest} }

func versionAtHead(ctx context.Context, t *testing.T, s *coreStack, actor Principal, head ChangesetID, path string) FileStat {
	t.Helper()
	const space SpaceID = "acme"
	stat, err := s.files.Stat(ctx, actor, space, Snapshot{Space: space, Changeset: head}, path)
	if err != nil {
		t.Fatalf("stat %q at %s: %v", path, head, err)
	}
	return stat
}

func newWorkspace(ctx context.Context, t *testing.T, s *coreStack, actor Principal, id string) Workspace {
	t.Helper()
	const space SpaceID = "acme"
	pin, err := s.files.ResolveAndPin(ctx, actor, space, "main", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseTestPin(ctx, t, s.files, actor, pin.ID)
	ws, err := s.files.CreateWorkspace(ctx, actor, space, pin.Snapshot, WorkspaceID(id), 0)
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func TestFileIdentityAcrossEditsMovesAndCopies(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("author")

	ws := newWorkspace(ctx, t, stack, actor, "ws-identity")

	// Create: mints a FileID.
	ws, err := stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
		Path: "docs/spec.md", Expected: absentVersion(), Content: []byte("v1"),
		Message: "create", IdempotencyKey: "k-create",
	})
	if err != nil {
		t.Fatal(err)
	}
	created := versionAtHead(ctx, t, stack, actor, ws.Head, "docs/spec.md")

	// Edit content: preserves the FileID.
	ws, err = stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
		Path: "docs/spec.md", Expected: presentVersion(created.Version.Digest), Content: []byte("v2"),
		Message: "edit", IdempotencyKey: "k-edit",
	})
	if err != nil {
		t.Fatal(err)
	}
	edited := versionAtHead(ctx, t, stack, actor, ws.Head, "docs/spec.md")
	if edited.File != created.File {
		t.Fatalf("edit changed the FileID: %s -> %s", created.File, edited.File)
	}

	// Move/rename: preserves the FileID; the old path disappears.
	ws, err = stack.files.MoveFile(ctx, actor, "acme", ws.ID, MoveFileRequest{
		From: "docs/spec.md", To: "docs/spec-v2.md",
		ExpectedFrom: presentVersion(edited.Version.Digest), ExpectedTo: absentVersion(),
		Message: "rename", IdempotencyKey: "k-move",
	})
	if err != nil {
		t.Fatal(err)
	}
	moved := versionAtHead(ctx, t, stack, actor, ws.Head, "docs/spec-v2.md")
	if moved.File != created.File {
		t.Fatalf("move changed the FileID: %s -> %s", created.File, moved.File)
	}
	if _, err := stack.files.Stat(ctx, actor, "acme", Snapshot{Space: "acme", Changeset: ws.Head}, "docs/spec.md"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("moved source still resolves: %v", err)
	}

	// Copy: the destination is a NEW logical document.
	ws, err = stack.files.CopyFile(ctx, actor, "acme", ws.ID, CopyFileRequest{
		From: "docs/spec-v2.md", To: "docs/copy.md",
		ExpectedFrom: presentVersion(edited.Version.Digest), ExpectedTo: absentVersion(),
		Message: "copy", IdempotencyKey: "k-copy",
	})
	if err != nil {
		t.Fatal(err)
	}
	copied := versionAtHead(ctx, t, stack, actor, ws.Head, "docs/copy.md")
	if copied.File == moved.File {
		t.Fatal("copy reused the source FileID")
	}
	if copied.Version.Digest != moved.Version.Digest {
		t.Fatal("copy did not preserve content bytes")
	}

	// Delete and recreate the same path: the recreation is a NEW document.
	ws, err = stack.files.DeleteFile(ctx, actor, "acme", ws.ID, DeleteFileRequest{
		Path: "docs/spec-v2.md", Expected: presentVersion(edited.Version.Digest),
		Message: "delete", IdempotencyKey: "k-delete",
	})
	if err != nil {
		t.Fatal(err)
	}
	ws, err = stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
		Path: "docs/spec-v2.md", Expected: absentVersion(), Content: []byte("recreated"),
		Message: "recreate", IdempotencyKey: "k-recreate",
	})
	if err != nil {
		t.Fatal(err)
	}
	recreated := versionAtHead(ctx, t, stack, actor, ws.Head, "docs/spec-v2.md")
	if recreated.File == created.File || recreated.File == copied.File {
		t.Fatalf("recreate reused a retired FileID: %s", recreated.File)
	}
}

func TestPreconditionFailuresAreStructured(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("author")
	ws := newWorkspace(ctx, t, stack, actor, "ws-precond")

	ws, err := stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
		Path: "a.txt", Expected: absentVersion(), Content: []byte("v1"),
		Message: "create", IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	stat := versionAtHead(ctx, t, stack, actor, ws.Head, "a.txt")

	t.Run("nil expected is rejected", func(t *testing.T) {
		if _, err := stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
			Path: "a.txt", Content: []byte("x"), Message: "m", IdempotencyKey: "k2",
		}); !errors.Is(err, ErrPreconditionRequired) {
			t.Fatalf("missing expected version was not rejected: %v", err)
		}
		if _, err := stack.files.DeleteFile(ctx, actor, "acme", ws.ID, DeleteFileRequest{
			Path: "a.txt", Message: "m", IdempotencyKey: "k3",
		}); !errors.Is(err, ErrPreconditionRequired) {
			t.Fatalf("delete without expected version was not rejected: %v", err)
		}
		if _, err := stack.files.MoveFile(ctx, actor, "acme", ws.ID, MoveFileRequest{
			From: "a.txt", To: "b.txt", ExpectedTo: absentVersion(),
			Message: "m", IdempotencyKey: "k4",
		}); !errors.Is(err, ErrPreconditionRequired) {
			t.Fatalf("move without source expectation was not rejected: %v", err)
		}
	})

	t.Run("stale replacement reports expected and actual", func(t *testing.T) {
		_, err := stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
			Path: "a.txt", Expected: presentVersion("stale-digest"), Content: []byte("x"),
			Message: "m", IdempotencyKey: "k5",
		})
		var conflict *Conflict
		if !errors.Is(err, ErrContentConflict) || !errors.As(err, &conflict) {
			t.Fatalf("stale replacement produced %v", err)
		}
		if conflict.Path != "a.txt" || conflict.Actual.Digest != stat.Version.Digest || conflict.Expected.Digest != "stale-digest" {
			t.Fatalf("conflict descriptor is wrong: %+v", conflict)
		}
	})

	t.Run("create collision reports existing content", func(t *testing.T) {
		_, err := stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
			Path: "a.txt", Expected: absentVersion(), Content: []byte("x"),
			Message: "m", IdempotencyKey: "k6",
		})
		var conflict *Conflict
		if !errors.Is(err, ErrContentConflict) || !errors.As(err, &conflict) {
			t.Fatalf("create collision produced %v", err)
		}
		if conflict.Actual.Exists != true {
			t.Fatalf("conflict should report the existing file: %+v", conflict)
		}
	})

	t.Run("move onto an occupied destination is explicit", func(t *testing.T) {
		if _, err := stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
			Path: "b.txt", Expected: absentVersion(), Content: []byte("b"),
			Message: "m", IdempotencyKey: "k7",
		}); err != nil {
			t.Fatal(err)
		}
		headBefore, err := stack.files.Workspace(ctx, actor, "acme", ws.ID)
		if err != nil {
			t.Fatal(err)
		}
		statA := versionAtHead(ctx, t, stack, actor, headBefore.Head, "a.txt")
		_, err = stack.files.MoveFile(ctx, actor, "acme", ws.ID, MoveFileRequest{
			From: "a.txt", To: "b.txt",
			ExpectedFrom: presentVersion(statA.Version.Digest), ExpectedTo: absentVersion(),
			Message: "m", IdempotencyKey: "k8",
		})
		var conflict *Conflict
		if !errors.Is(err, ErrContentConflict) || !errors.As(err, &conflict) || conflict.Path != "b.txt" {
			t.Fatalf("occupied move destination produced %v", err)
		}
		headAfter, err := stack.files.Workspace(ctx, actor, "acme", ws.ID)
		if err != nil {
			t.Fatal(err)
		}
		if headAfter.Head != headBefore.Head || headAfter.Generation != headBefore.Generation {
			t.Fatal("failed move changed the workspace head")
		}
	})
}

func TestAtomicMultiFileCommitIsAllOrNothing(t *testing.T) {
	stack := newCoreStack(t, testCaps(), nil)
	ctx := context.Background()
	actor := testActor("author")
	ws := newWorkspace(ctx, t, stack, actor, "ws-atomic")

	headBefore := ws.Head
	refA, err := stack.files.Stage(ctx, actor, "acme", []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	refB, err := stack.files.Stage(ctx, actor, "acme", []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	ops := []Mutation{
		PutMutation("reports/a.md", Version{Exists: false}, refA),
		PutMutation("reports/b.md", Version{Exists: true, Digest: "not-the-digest"}, refB), // stale precondition
	}
	_, err = stack.files.Commit(ctx, actor, "acme", ws.ID, CommitRequest{
		Ops: ops, Message: "batch", IdempotencyKey: "batch-1",
	})
	if err == nil {
		t.Fatal("commit with a stale precondition succeeded")
	}
	var conflict *Conflict
	if !errors.Is(err, ErrContentConflict) || !errors.As(err, &conflict) || conflict.Path != "reports/b.md" {
		t.Fatalf("expected a structured conflict for reports/b.md: %v", err)
	}
	wsNow, err := stack.files.Workspace(ctx, actor, "acme", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if wsNow.Head != headBefore || wsNow.Generation != ws.Generation {
		t.Fatal("rejected batch partially applied")
	}
	if _, err := stack.files.Stat(ctx, actor, "acme", Snapshot{Space: "acme", Changeset: wsNow.Head}, "reports/a.md"); !errors.Is(err, ErrNotFound) {
		t.Fatal("reports/a.md became visible despite the rejected batch")
	}

	// A valid multi-file commit lands atomically; an older snapshot keeps
	// the previous view.
	refA2, err := stack.files.Stage(ctx, actor, "acme", []byte("a2"))
	if err != nil {
		t.Fatal(err)
	}
	refB2, err := stack.files.Stage(ctx, actor, "acme", []byte("b2"))
	if err != nil {
		t.Fatal(err)
	}
	oldMain, err := stack.files.ResolveAndPin(ctx, actor, "acme", "main", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseTestPin(ctx, t, stack.files, actor, oldMain.ID)
	wsNow, err = stack.files.Commit(ctx, actor, "acme", ws.ID, CommitRequest{
		Ops: []Mutation{
			PutMutation("reports/a.md", Version{Exists: false}, refA2),
			PutMutation("reports/b.md", Version{Exists: false}, refB2),
		},
		Message: "batch", IdempotencyKey: "batch-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"reports/a.md", "reports/b.md"} {
		if _, err := stack.files.Stat(ctx, actor, "acme", Snapshot{Space: "acme", Changeset: wsNow.Head}, path); err != nil {
			t.Fatalf("%s missing from the committed snapshot: %v", path, err)
		}
		if _, err := stack.files.Stat(ctx, actor, "acme", oldMain.Snapshot, path); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s leaked into an older snapshot: %v", path, err)
		}
	}
}

func TestFileServiceRejectsMissingCapabilities(t *testing.T) {
	ctx := context.Background()
	caps := NewCapabilities("weak", "v1", TransferLimits{}, CapSnapshots) // no conditional writes
	weak := &coreStack{blobs: newMemBlobs(), trees: newMemTrees()}
	if _, err := NewFileService(weak.blobs, weak.trees, newMemDrafts(weak.blobs), newMemSnapshots(newMemRefs()), allowAll{}, caps); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("service started without conditional writes: %v", err)
	}
	full, err := NewFileService(newMemBlobs(), newMemTrees(), newMemDrafts(newMemBlobs()), newMemSnapshots(newMemRefs()), allowAll{}, testCaps())
	if err != nil {
		t.Fatal(err)
	}
	discovered, err := full.DiscoverCapabilities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []Capability{CapConditionalWrites, CapStableFileIDs, CapSnapshots, CapAtomicMultiFile} {
		if !discovered.Has(want) {
			t.Fatalf("discovery does not report %s", want)
		}
	}
	if err := RequireCapability(discovered, CapSemanticSearch); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("semantic search should be explicitly unsupported: %v", err)
	}
}

func TestFileServiceEnforcesTransferLimitsAndAuthz(t *testing.T) {
	denying := prefixDeny{deny: "contracts/"}
	stack := newCoreStack(t, testCaps(), denying)
	ctx := context.Background()
	actor := testActor("author")
	ws := newWorkspace(ctx, t, stack, actor, "ws-limits")

	if _, err := stack.files.Stage(ctx, actor, "acme", make([]byte, testCaps().Limits.MaxFileSize+1)); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("oversized content was accepted: %v", err)
	}

	// Authorization applies per path through the shared application service.
	if _, err := stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
		Path: "contracts/secret.md", Expected: absentVersion(), Content: []byte("x"),
		Message: "m", IdempotencyKey: "k-denied",
	}); !errors.Is(err, ErrDenied) {
		t.Fatalf("denied path was writable: %v", err)
	}

	// Read authorization: the file exists in a snapshot the authorizer
	// guards, so reads are denied even though the caller names a snapshot.
	ws2, err := stack.files.WriteFile(ctx, actor, "acme", ws.ID, WriteFileRequest{
		Path: "public/ok.md", Expected: absentVersion(), Content: []byte("ok"),
		Message: "m", IdempotencyKey: "k-ok",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := stack.files.ReadFile(ctx, actor, "acme", Snapshot{Space: "acme", Changeset: ws2.Head}, "public/ok.md"); err != nil {
		t.Fatal(err)
	}
	reader := denyReader{auth: denying, actor: actor, space: "acme", snap: Snapshot{Space: "acme", Changeset: ws2.Head}, files: stack.files}
	if _, _, err := reader.read("contracts/x.md"); !errors.Is(err, ErrDenied) {
		t.Fatalf("denied path was readable: %v", err)
	}
}

// denyReader mirrors what an MCP/REST adapter must do: every read goes
// through the same application service, so per-path authorization holds.
type denyReader struct {
	auth  Authorizer
	actor Principal
	space SpaceID
	snap  Snapshot
	files FileService
}

func (d denyReader) read(path string) ([]byte, FileStat, error) {
	if err := d.auth.CheckAccess(context.Background(), d.actor, d.space, path, AccessRead); err != nil {
		return nil, FileStat{}, err
	}
	return d.files.ReadFile(context.Background(), d.actor, d.space, d.snap, path)
}
