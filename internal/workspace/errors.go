package workspace

import "errors"

// Domain errors describe invalid requests, concurrency conflicts, and access failures.
var (
	ErrInvalidRequest      = errors.New("agentfs: invalid request")
	ErrNotFound            = errors.New("agentfs: not found")
	ErrHeadMoved           = errors.New("agentfs: ref head moved")
	ErrContended           = errors.New("agentfs: publication retry budget exhausted")
	ErrContentConflict     = errors.New("agentfs: content conflict")
	ErrDependencyChanged   = errors.New("agentfs: input dependency changed")
	ErrIdempotencyConflict = errors.New("agentfs: idempotency key reused for a different request")
	ErrDenied              = errors.New("agentfs: access denied")
	ErrApprovalRequired    = errors.New("agentfs: publication requires approval")
	ErrInvalidCandidate    = errors.New("agentfs: invalid prepared candidate")
	ErrInvalidPermit       = errors.New("agentfs: policy evidence does not match candidate")
	// ErrUnsupportedCapability must be returned when a caller requests a
	// guarantee or feature the adapter does not provide. Silently dropping a
	// requested precondition or guarantee is a correctness bug, never a
	// fallback.
	ErrUnsupportedCapability = errors.New("agentfs: capability not supported")
	// ErrPreconditionRequired is returned when a write arrives without the
	// explicit expected version (or expected absence) the API contract
	// requires. Callers must state what they believe is true.
	ErrPreconditionRequired = errors.New("agentfs: expected version required")
	// ErrGenerationConflict is returned when a workspace head update loses
	// its own CAS race.
	ErrGenerationConflict = errors.New("agentfs: workspace generation conflict")
	// ErrLimitExceeded is returned when a request would violate a published
	// transfer limit (file size, operations per commit, upload concurrency).
	ErrLimitExceeded = errors.New("agentfs: transfer limit exceeded")
)

// Conflict carries machine-readable resolution inputs. Callers use errors.Is
// for Kind and errors.As to inspect the path and versions. Do not expose these
// fields through an API without current authorization checks.
type Conflict struct {
	Kind     error
	Path     string
	Expected Version
	Actual   Version
}

func (e *Conflict) Error() string {
	return e.Kind.Error() + ": " + e.Path
}

func (e *Conflict) Unwrap() error { return e.Kind }
