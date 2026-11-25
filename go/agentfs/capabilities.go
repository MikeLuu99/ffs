package agentfs

import (
	"context"
	"fmt"
	"sort"
)

// Capability names a guarantee or feature an adapter may or may not provide.
// Adapters must enforce a requested guarantee or fail with
// ErrUnsupportedCapability; they must never silently drop a precondition
// (for example, applying a write without its expected version because the
// backend lacks conditional writes).
type Capability string

const (
	// CapConditionalWrites guarantees Before/BeforeTarget preconditions on every
	// mutation are validated atomically with the change.
	CapConditionalWrites Capability = "conditional_writes"
	// CapStableFileIDs guarantees FileID identity semantics (§identity).
	CapStableFileIDs Capability = "stable_file_ids"
	// CapSnapshots guarantees pinned, consistent multi-file read handles are
	// protected against garbage collection for the pin lifetime.
	CapSnapshots Capability = "snapshots"
	// CapAtomicMultiFile guarantees a commit with several mutations becomes visible
	// atomically or not at all.
	CapAtomicMultiFile Capability = "atomic_multi_file_commits"
	// CapWatchPolling allows clients to poll for changes to a scope.
	CapWatchPolling Capability = "watch_polling"
	// CapWatchResumable allows clients to hold a resumable event subscription
	// with a durable cursor (distinct from polling: delivery resumes after
	// disconnects without loss).
	CapWatchResumable Capability = "watch_resumable"
	// CapFilenameSearch enables filename queries against a derived SearchIndex.
	CapFilenameSearch Capability = "search_filename"
	// CapFullTextSearch enables content queries against a derived SearchIndex.
	CapFullTextSearch Capability = "search_full_text"
	// CapSemanticSearch enables embedding-based relevance. Added later than the
	// lexical modes; capability discovery keeps clients honest about it.
	CapSemanticSearch Capability = "search_semantic"
)

// TransferLimits bound request shapes so adapters can reject early and
// explicitly. Zero means "no explicit limit" for local test stacks; adapters
// must publish real values.
type TransferLimits struct {
	MaxFileSize            int64 `json:"max_file_size,omitempty"`
	MaxOperationsPerCommit int   `json:"max_operations_per_commit,omitempty"`
	MaxConcurrentUploads   int   `json:"max_concurrent_uploads,omitempty"`
}

// Capabilities is the negotiated contract between a client adapter and a
// service deployment. Discovery precedes feature use; requesting an
// unsupported capability surfaces ErrUnsupportedCapability.
type Capabilities struct {
	Service    string         `json:"service"`
	APIVersion string         `json:"api_version"`
	Limits     TransferLimits `json:"limits"`

	supported map[Capability]struct{}
}

// NewCapabilities builds a Capabilities value from the enabled set.
func NewCapabilities(service, apiVersion string, limits TransferLimits, caps ...Capability) Capabilities {
	supported := make(map[Capability]struct{}, len(caps))
	for _, c := range caps {
		supported[c] = struct{}{}
	}
	return Capabilities{Service: service, APIVersion: apiVersion, Limits: limits, supported: supported}
}

// Has reports whether the service supports capability.
func (c Capabilities) Has(capability Capability) bool {
	_, ok := c.supported[capability]
	return ok
}

// Enabled returns the supported capabilities in sorted order.
func (c Capabilities) Enabled() []Capability {
	out := make([]Capability, 0, len(c.supported))
	for c := range c.supported {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// RequireCapability returns ErrUnsupportedCapability unless the capability
// is present. Application services call this before honoring a request that
// depends on a negotiable guarantee.
func RequireCapability(caps Capabilities, capability Capability) error {
	if !caps.Has(capability) {
		return fmt.Errorf("%w: %s", ErrUnsupportedCapability, capability)
	}
	return nil
}

// CapabilityDiscoverer exposes the contract to adapters (REST, CLI, MCP,
// mount) before they commit to one. Discovery output must match enforced
// behavior exactly: listing a capability the service does not enforce is a
// correctness bug.
type CapabilityDiscoverer interface {
	DiscoverCapabilities(ctx context.Context) (Capabilities, error)
}
