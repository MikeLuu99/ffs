package agentfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"unicode/utf8"
)

// Publisher coordinates preparation and publication. It contains immutable
// configuration and is safe to share across goroutines if its adapters are.
// Each call does bounded work; it starts no background goroutines and holds no
// process-wide lock. Distributed correctness belongs to TryPublish's database
// transaction, not a mutex or channel in this process.
type Publisher struct {
	store       PublicationStore
	builder     CandidateBuilder
	policy      PublicationPolicy
	maxAttempts int
}

// NewPublisher wires publication adapters with a positive retry budget.
func NewPublisher(store PublicationStore, builder CandidateBuilder, policy PublicationPolicy, maxAttempts int) (*Publisher, error) {
	if store == nil || builder == nil || policy == nil || maxAttempts < 1 {
		return nil, fmt.Errorf("%w: adapters and a positive attempt budget are required", ErrInvalidRequest)
	}
	return &Publisher{store: store, builder: builder, policy: policy, maxAttempts: maxAttempts}, nil
}

// Publish preserves saved proposals on every failure. A caller receiving a
// transport/cancellation error must treat the outcome as uncertain and retry
// the SAME request/key: the store may already have committed its transaction.
func (p *Publisher) Publish(ctx context.Context, request PublishRequest) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	if err := validatePublishRequest(request); err != nil {
		return Receipt{}, err
	}
	if request.Expected != nil {
		expected := *request.Expected
		request.Expected = &expected
	}
	op, err := publicationKey(request)
	if err != nil {
		return Receipt{}, err
	}

	for attempt := 0; attempt < p.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Receipt{}, err
		}
		if receipt, found, err := p.store.LookupPublication(ctx, op); err != nil {
			return Receipt{}, err
		} else if found {
			return receipt, nil
		}

		head, err := p.store.Head(ctx, request.Actor, request.Space, request.Target)
		if err != nil {
			return Receipt{}, err
		}
		if head.Space != request.Space || head.Name != request.Target || head.Version.Head == "" {
			return Receipt{}, fmt.Errorf("%w: metadata returned a different ref", ErrInvalidCandidate)
		}
		if request.Mode == PublishStrict && head.Version != *request.Expected {
			return p.replayOrError(ctx, op, ErrHeadMoved)
		}

		candidate, err := p.builder.Prepare(ctx, request, head)
		if err != nil {
			return p.replayOrError(ctx, op, err)
		}
		if candidate.Snapshot.Space != request.Space || candidate.Snapshot.Changeset == "" ||
			candidate.Parent != head.Version.Head || candidate.Tree == "" || candidate.InputDigest == "" {
			return Receipt{}, ErrInvalidCandidate
		}

		permit, err := p.policy.Evaluate(ctx, request, candidate)
		if err != nil {
			return p.replayOrError(ctx, op, err)
		}
		if permit.Actor != request.Actor.ID || permit.Target != request.Target ||
			permit.Candidate != candidate.Snapshot || permit.InputDigest != candidate.InputDigest ||
			permit.PolicyRevision == "" || permit.Evidence == "" {
			return Receipt{}, ErrInvalidPermit
		}

		receipt, err := p.store.TryPublish(ctx, Publication{
			Operation: op,
			Request:   request,
			Expected:  head,
			Candidate: candidate,
			Permit:    permit,
		})
		if err == nil {
			return receipt, nil
		}
		if !errors.Is(err, ErrHeadMoved) || request.Mode == PublishStrict {
			return p.replayOrError(ctx, op, err)
		}
		// Main moved while preparation ran. Rebuild AND reevaluate policy against
		// the new head; never reuse a permit for a previous candidate.
	}
	return p.replayOrError(ctx, op, ErrContended)
}

// A concurrent copy of the same request may have published while this caller
// prepared a now-stale candidate. Prefer the original durable receipt.
func (p *Publisher) replayOrError(ctx context.Context, op OperationKey, original error) (Receipt, error) {
	receipt, found, err := p.store.LookupPublication(ctx, op)
	if err != nil {
		return Receipt{}, err
	}
	if found {
		return receipt, nil
	}
	return Receipt{}, original
}

func validatePublishRequest(request PublishRequest) error {
	for _, value := range []string{
		string(request.Space), string(request.Target), string(request.Proposal),
		string(request.Actor.ID), string(request.Actor.Owner), request.Actor.Kind,
		request.Actor.Tool, request.Actor.RunID, request.IdempotencyKey,
	} {
		if !utf8.ValidString(value) {
			return fmt.Errorf("%w: request strings must be valid UTF-8", ErrInvalidRequest)
		}
	}
	if strings.TrimSpace(string(request.Space)) == "" || strings.TrimSpace(string(request.Actor.ID)) == "" ||
		strings.TrimSpace(string(request.Proposal)) == "" || strings.TrimSpace(request.IdempotencyKey) == "" {
		return fmt.Errorf("%w: space, actor, proposal, and idempotency key are required", ErrInvalidRequest)
	}
	name := string(request.Target)
	if !fs.ValidPath(name) || name == "." || strings.ContainsAny(name, "\\\x00") {
		return fmt.Errorf("%w: target must be a canonical relative ref name", ErrInvalidRequest)
	}
	switch request.Mode {
	case PublishStrict:
		if request.Expected == nil || request.Expected.Head == "" || !utf8.ValidString(string(request.Expected.Head)) {
			return fmt.Errorf("%w: strict mode requires an expected ref version", ErrInvalidRequest)
		}
	case PublishRebaseIfSafe:
		if request.Expected != nil {
			return fmt.Errorf("%w: use strict mode to require an exact ref version", ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: choose strict or rebase_if_safe", ErrInvalidRequest)
	}
	return nil
}

func publicationKey(request PublishRequest) (OperationKey, error) {
	// This map-free wire struct is versioned independently of changeset storage.
	// Retrying against a new head does not change the original request identity.
	wire := struct {
		Version int            `json:"version"`
		Request PublishRequest `json:"request"`
	}{Version: 1, Request: request}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return OperationKey{}, fmt.Errorf("%w: encode publication request: %w", ErrInvalidRequest, err)
	}
	sum := sha256.Sum256(encoded)
	return OperationKey{
		Space: request.Space, Actor: request.Actor.ID, Operation: "publish",
		Key: request.IdempotencyKey, RequestHash: Digest(hex.EncodeToString(sum[:])),
	}, nil
}
