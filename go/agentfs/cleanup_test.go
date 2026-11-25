package agentfs

import (
	"context"
	"errors"
	"io"
	"testing"
)

type failingReadCloser struct {
	io.ReadCloser
	readErr  error
	closeErr error
	closed   bool
}

func (r *failingReadCloser) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return r.ReadCloser.Read(p)
}

func (r *failingReadCloser) Close() error {
	r.closed = true
	return errors.Join(r.ReadCloser.Close(), r.closeErr)
}

type failingReadBlobStore struct {
	BlobStore
	reader *failingReadCloser
}

func (b *failingReadBlobStore) Open(ctx context.Context, ref BlobRef) (io.ReadCloser, error) {
	r, err := b.BlobStore.Open(ctx, ref)
	if err != nil {
		return nil, err
	}
	b.reader.ReadCloser = r
	return b.reader, nil
}

func TestReadFileReportsReadAndCloseErrors(t *testing.T) {
	for _, failRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "read-and-close"}[failRead], func(t *testing.T) {
			ctx := context.Background()
			stack := newCoreStack(t, testCaps(), nil)
			actor := testActor("reader")
			seedRemoteFiles(ctx, t, stack, map[string]string{"a.txt": "content"})
			head, err := stack.pubs.Head(ctx, actor, "acme", "main")
			if err != nil {
				t.Fatal(err)
			}
			closeErr := errors.New("stream close failed")
			reader := &failingReadCloser{closeErr: closeErr}
			if failRead {
				reader.readErr = errors.New("stream read failed")
			}
			blobs := &failingReadBlobStore{BlobStore: stack.blobs, reader: reader}
			files, err := NewFileService(blobs, stack.trees, stack.drafts, stack.snaps, allowAll{}, testCaps())
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = files.ReadFile(ctx, actor, "acme", Snapshot{Space: "acme", Changeset: head.Version.Head}, "a.txt")
			if !reader.closed || !errors.Is(err, closeErr) || (failRead && !errors.Is(err, reader.readErr)) {
				t.Fatalf("closed=%t, error=%v; want both applicable stream errors", reader.closed, err)
			}
		})
	}
}

type failingReleaseService struct {
	FileService
	err error
}

func (s failingReleaseService) ReleasePin(context.Context, Principal, string) error {
	return s.err
}

func TestStatusReportsPinReleaseError(t *testing.T) {
	ctx := context.Background()
	stack := newCoreStack(t, testCaps(), nil)
	actor := testActor("editor")
	journal, cache := newMemJournal(), newMemCache()
	manager := newManager(t, stack, journal, cache)
	wc, err := manager.Checkout(ctx, actor, "acme", "main", t.TempDir(), "status-copy")
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("pin release failed")
	manager.files = failingReleaseService{FileService: stack.files, err: injected}
	if _, err := manager.Status(ctx, actor, wc, "main"); !errors.Is(err, injected) {
		t.Fatalf("Status error = %v, want pin release failure", err)
	}
}
