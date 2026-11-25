package workspace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The fake below exercises the coordinator contract. It is not a production
// metadata backend: a real adapter must use a durable database transaction.
type operationScope struct {
	space     SpaceID
	actor     PrincipalID
	operation string
	key       string
}

type storedReceipt struct {
	hash    Digest
	receipt Receipt
}

type fakePublicationStore struct {
	mu          sync.Mutex
	ref         Ref
	records     map[operationScope]storedReceipt
	attempts    int
	events      int
	moveOnce    bool
	alwaysMoved bool
}

func newFakeStore() *fakePublicationStore {
	return &fakePublicationStore{
		ref:     Ref{Space: "acme", Name: "main", Version: RefVersion{Head: "base", Generation: 1}},
		records: make(map[operationScope]storedReceipt),
	}
}

func scope(op OperationKey) operationScope {
	return operationScope{op.Space, op.Actor, op.Operation, op.Key}
}

func (s *fakePublicationStore) lookupLocked(op OperationKey) (Receipt, bool, error) {
	record, found := s.records[scope(op)]
	if found && record.hash != op.RequestHash {
		return Receipt{}, false, ErrIdempotencyConflict
	}
	return record.receipt, found, nil
}

func (s *fakePublicationStore) LookupPublication(ctx context.Context, op OperationKey) (Receipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookupLocked(op)
}

func (s *fakePublicationStore) Head(ctx context.Context, _ Principal, space SpaceID, name RefName) (Ref, error) {
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if space != s.ref.Space || name != s.ref.Name {
		return Ref{}, ErrNotFound
	}
	return s.ref, nil
}

func (s *fakePublicationStore) TryPublish(ctx context.Context, p Publication) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if receipt, found, err := s.lookupLocked(p.Operation); found || err != nil {
		return receipt, err
	}
	if s.moveOnce {
		s.moveOnce = false
		s.ref.Version = RefVersion{Head: "other-writer", Generation: s.ref.Version.Generation + 1}
		return Receipt{}, ErrHeadMoved
	}
	if s.alwaysMoved || s.ref != p.Expected {
		return Receipt{}, ErrHeadMoved
	}
	if p.Candidate.Parent != s.ref.Version.Head || p.Candidate.Snapshot.Space != s.ref.Space {
		return Receipt{}, ErrInvalidCandidate
	}
	if p.Permit.Actor != p.Request.Actor.ID || p.Permit.Target != p.Request.Target ||
		p.Permit.Candidate != p.Candidate.Snapshot || p.Permit.InputDigest != p.Candidate.InputDigest {
		return Receipt{}, ErrInvalidPermit
	}
	s.ref.Version.Head = p.Candidate.Snapshot.Changeset
	s.ref.Version.Generation++
	receipt := Receipt{
		Ref: s.ref, Proposal: p.Request.Proposal, Published: p.Candidate.Snapshot,
		PublishedAt: time.Now().UTC(),
	}
	s.events++
	s.records[scope(p.Operation)] = storedReceipt{hash: p.Operation.RequestHash, receipt: receipt}
	return receipt, nil
}

type builderFunc func(context.Context, PublishRequest, Ref) (Candidate, error)

func (f builderFunc) Prepare(ctx context.Context, request PublishRequest, head Ref) (Candidate, error) {
	return f(ctx, request, head)
}

func buildCandidate(_ context.Context, request PublishRequest, head Ref) (Candidate, error) {
	// Opaque test IDs; the production builder uses verified content addresses.
	id := ChangesetID(fmt.Sprintf("%s-on-%s-%d", request.Proposal, head.Version.Head, head.Version.Generation))
	return Candidate{
		Snapshot: Snapshot{Space: request.Space, Changeset: id},
		Parent:   head.Version.Head, Tree: TreeID("tree-" + string(id)), InputDigest: "inputs",
	}, nil
}

type policyFunc func(context.Context, PublishRequest, Candidate) (Permit, error)

func (f policyFunc) Evaluate(ctx context.Context, request PublishRequest, candidate Candidate) (Permit, error) {
	return f(ctx, request, candidate)
}

func allowCandidate(_ context.Context, request PublishRequest, candidate Candidate) (Permit, error) {
	return Permit{
		Actor: request.Actor.ID, Target: request.Target,
		Candidate: candidate.Snapshot, InputDigest: candidate.InputDigest,
		PolicyRevision: "policy-v1", Evidence: "test-evidence",
	}, nil
}

func requestFor(proposal ChangesetID) PublishRequest {
	return PublishRequest{
		Space: "acme", Target: "main", Proposal: proposal,
		Actor: Principal{ID: "agent-1", Owner: "team", Kind: "agent", RunID: "run-1"},
		Mode:  PublishRebaseIfSafe, IdempotencyKey: "publish-" + string(proposal),
	}
}

func publisherFor(t *testing.T, store PublicationStore, builder CandidateBuilder, policy PublicationPolicy, attempts int) *Publisher {
	t.Helper()
	publisher, err := NewPublisher(store, builder, policy, attempts)
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func TestPublisherRebuildsAndRevalidatesAfterHeadMoves(t *testing.T) {
	store := newFakeStore()
	store.moveOnce = true
	var validated []ChangesetID
	policy := policyFunc(func(ctx context.Context, request PublishRequest, candidate Candidate) (Permit, error) {
		validated = append(validated, candidate.Snapshot.Changeset)
		return allowCandidate(ctx, request, candidate)
	})
	publisher := publisherFor(t, store, builderFunc(buildCandidate), policy, 3)
	receipt, err := publisher.Publish(context.Background(), requestFor("proposal-a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(validated) != 2 || validated[0] == validated[1] {
		t.Fatalf("expected a new candidate and new policy check, got %v", validated)
	}
	if receipt.Ref.Version.Generation != 3 || store.events != 1 || store.attempts != 2 {
		t.Fatalf("unexpected result: receipt=%+v events=%d attempts=%d", receipt, store.events, store.attempts)
	}
}

func TestPublisherReplaysOriginalReceiptAfterFurtherPublications(t *testing.T) {
	store := newFakeStore()
	publisher := publisherFor(t, store, builderFunc(buildCandidate), policyFunc(allowCandidate), 3)
	request := requestFor("proposal-a")
	first, err := publisher.Publish(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), requestFor("proposal-b")); err != nil {
		t.Fatal(err)
	}
	replayed, err := publisher.Publish(context.Background(), request)
	if err != nil || replayed != first || store.events != 2 {
		t.Fatalf("retry did not return original receipt: first=%+v replay=%+v err=%v", first, replayed, err)
	}
	request.Proposal = "different-proposal"
	if _, err := publisher.Publish(context.Background(), request); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed request reused key: %v", err)
	}
}

func TestPublisherStrictModeDoesNotRebase(t *testing.T) {
	store := newFakeStore()
	store.moveOnce = true
	publisher := publisherFor(t, store, builderFunc(buildCandidate), policyFunc(allowCandidate), 4)
	request := requestFor("proposal-a")
	expected := store.ref.Version
	request.Mode, request.Expected = PublishStrict, &expected
	_, err := publisher.Publish(context.Background(), request)
	if !errors.Is(err, ErrHeadMoved) || store.attempts != 1 || store.events != 0 {
		t.Fatalf("strict request retried or published: err=%v attempts=%d events=%d", err, store.attempts, store.events)
	}
}

func TestPublisherRejectsStaleDependenciesBeforePublication(t *testing.T) {
	store := newFakeStore()
	builder := builderFunc(func(context.Context, PublishRequest, Ref) (Candidate, error) {
		return Candidate{}, &Conflict{
			Kind: ErrDependencyChanged, Path: "pricing.json",
			Expected: Version{Exists: true, Digest: "old"}, Actual: Version{Exists: true, Digest: "new"},
		}
	})
	publisher := publisherFor(t, store, builder, policyFunc(allowCandidate), 3)
	_, err := publisher.Publish(context.Background(), requestFor("forecast"))
	var conflict *Conflict
	if !errors.Is(err, ErrDependencyChanged) || !errors.As(err, &conflict) || conflict.Path != "pricing.json" {
		t.Fatalf("missing structured input conflict: %v", err)
	}
	if store.attempts != 0 || store.events != 0 {
		t.Fatal("stale proposal reached publication")
	}
}

func TestPublisherStopsForRequiredApproval(t *testing.T) {
	store := newFakeStore()
	policy := policyFunc(func(context.Context, PublishRequest, Candidate) (Permit, error) {
		return Permit{}, ErrApprovalRequired
	})
	publisher := publisherFor(t, store, builderFunc(buildCandidate), policy, 3)
	_, err := publisher.Publish(context.Background(), requestFor("contract"))
	if !errors.Is(err, ErrApprovalRequired) || store.attempts != 0 || store.events != 0 {
		t.Fatalf("approval requirement was bypassed: %v", err)
	}
}

func TestPublisherRejectsPermitForDifferentTarget(t *testing.T) {
	store := newFakeStore()
	policy := policyFunc(func(ctx context.Context, request PublishRequest, candidate Candidate) (Permit, error) {
		permit, err := allowCandidate(ctx, request, candidate)
		permit.Target = "unprotected-branch"
		return permit, err
	})
	publisher := publisherFor(t, store, builderFunc(buildCandidate), policy, 3)
	_, err := publisher.Publish(context.Background(), requestFor("contract"))
	if !errors.Is(err, ErrInvalidPermit) || store.attempts != 0 {
		t.Fatalf("permit for another target reached publication: %v", err)
	}
}

func TestPublisherBoundsRetriesAndHonorsCancellation(t *testing.T) {
	store := newFakeStore()
	store.alwaysMoved = true
	publisher := publisherFor(t, store, builderFunc(buildCandidate), policyFunc(allowCandidate), 2)
	_, err := publisher.Publish(context.Background(), requestFor("proposal-a"))
	if !errors.Is(err, ErrContended) || store.attempts != 2 {
		t.Fatalf("unbounded retry: err=%v attempts=%d", err, store.attempts)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = publisher.Publish(ctx, requestFor("proposal-b"))
	if !errors.Is(err, context.Canceled) || store.attempts != 2 {
		t.Fatalf("canceled call reached adapters: err=%v attempts=%d", err, store.attempts)
	}
}

// Force the first two calls to prepare from the same head, so CAS contention
// happens deterministically rather than relying on scheduler timing.
func barrierBuilder() CandidateBuilder {
	barrier := make(chan struct{})
	var arrivals int32
	return builderFunc(func(ctx context.Context, request PublishRequest, head Ref) (Candidate, error) {
		arrival := atomic.AddInt32(&arrivals, 1)
		if arrival <= 2 {
			if arrival == 2 {
				close(barrier)
			}
			select {
			case <-barrier:
			case <-ctx.Done():
				return Candidate{}, ctx.Err()
			}
		}
		return buildCandidate(ctx, request, head)
	})
}

func TestPublisherConcurrentRequests(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprintf("duplicate=%v", duplicate), func(t *testing.T) {
			store := newFakeStore()
			publisher := publisherFor(t, store, barrierBuilder(), policyFunc(allowCandidate), 4)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type outcome struct {
				receipt Receipt
				err     error
			}
			outcomes := make(chan outcome, 2)
			requests := []PublishRequest{requestFor("proposal-a"), requestFor("proposal-b")}
			if duplicate {
				requests[1] = requests[0]
			}
			for _, request := range requests {
				go func(request PublishRequest) {
					receipt, err := publisher.Publish(ctx, request)
					outcomes <- outcome{receipt: receipt, err: err}
				}(request)
			}
			var receipts []Receipt
			for range requests {
				select {
				case result := <-outcomes:
					if result.err != nil {
						t.Fatal(result.err)
					}
					receipts = append(receipts, result.receipt)
				case <-ctx.Done():
					t.Fatal("concurrent publication did not terminate")
				}
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if duplicate {
				if store.events != 1 || store.ref.Version.Generation != 2 || receipts[0] != receipts[1] {
					t.Fatal("duplicate concurrent calls must produce one publication and identical receipts")
				}
			} else if store.events != 2 || store.ref.Version.Generation != 3 || receipts[0].Published == receipts[1].Published {
				t.Fatal("independent concurrent requests must each publish after a safe retry")
			}
		})
	}
}
