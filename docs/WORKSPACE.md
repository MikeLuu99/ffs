# Workspace domain reference

Go abstractions for a versioned organizational workspace shared by humans and agents.

The central rules are **save work independently; publish shared state atomically**, and **stable file identity**: a `FileID` follows a logical document across edits and moves; a blob digest identifies exact bytes.

The `internal/workspace` package contains domain types, adapter contracts, the publication coordinator, filesystem-facing application services, working-copy synchronization, and capability/search/session contracts. It uses the standard library and targets Go 1.22 or later. `example.com/agentfs` is a local placeholder module path.

## Main abstractions

| Abstraction | Responsibility | Location |
|---|---|---|
| `BlobStore` | Verify and store immutable, space-scoped file bytes. | [store.go](../internal/workspace/store.go) |
| `TreeStore` | Apply deltas to persistent trees; resolve paths at an exact root; enforce FileID identity rules. | [store.go](../internal/workspace/store.go), [model.go](../internal/workspace/model.go) |
| `DraftStore` / `SnapshotStore` | Save private progress, resolve snapshots, protect retention. | [store.go](../internal/workspace/store.go) |
| `CandidateBuilder` / `PublicationPolicy` | Prepare a safe candidate (rebased onto the current head, dependencies validated) and obtain checks bound to it. | [store.go](../internal/workspace/store.go) |
| `PublicationStore` | Atomically move a ref, record audit/outbox events, and store the retry receipt. | [store.go](../internal/workspace/store.go) |
| `Publisher` | Coordinate preparation, validation, bounded CAS retries, and result replay. | [publisher.go](../internal/workspace/publisher.go) |
| `FileService` | Filesystem-facing API (`ReadFile`, `Stat`, `List`, `WriteFile`, `Move`, `Copy`, `Delete`, `Commit`): verified staging, mandatory preconditions, atomic multi-file changesets, snapshot-consistent reads. | [service.go](../internal/workspace/service.go) |
| `WorkingCopyManager` | Local checkout and synchronization: durable sync journal, crash recovery, idempotent retries, reconnect conflict detection. | [workingcopy.go](../internal/workspace/workingcopy.go) |
| `SearchIndex` / `FileMetaStore` | Derived read models: filename/full-text search with freshness, FileID history and annotations. | [search.go](../internal/workspace/search.go), [identity.go](../internal/workspace/identity.go) |
| `Capabilities` / `CapabilityDiscoverer` | Negotiated guarantees and transfer limits; unsupported features fail explicitly. | [capabilities.go](../internal/workspace/capabilities.go) |
| `DocumentSession` | Future live-collaboration boundary (explicitly unsupported in v1). | [session.go](../internal/workspace/session.go) |

See [ARCHITECTURE.md](ARCHITECTURE.md) for diagrams and worked examples.

## Concurrency contract

- A `Publisher` can serve many goroutines at once. Its configuration is immutable; adapters must support concurrent calls.
- Slow work runs outside the publication transaction. Every CAS retry rebuilds the candidate and reruns policy checks.
- Shared-state correctness comes from the metadata transaction. A Go mutex or channel cannot protect a ref across multiple servers.
- Strict publication compares both commit ID and generation. Safe-rebase mode retries only head movement, up to the configured budget; dependency/content conflicts remain explicit.
- The original publication request determines idempotency identity. Retry-generated candidates do not change that identity.
- A matching duplicate returns the original receipt, even if the ref has advanced since then.
- Every I/O-facing method accepts `context.Context`. Coordinators start no hidden goroutines and respect cancellation between attempts. Background staging in sync is bounded by a configured concurrency limit.
- A canceled request can have an uncertain outcome if the database already committed. Retry the exact request and key to retrieve its durable receipt.

## Identity rules

| Operation | FileID |
|---|---|
| Edit content (`OpPut` on an existing path with its expected digest) | preserved |
| Move/rename (`OpMove`) | preserved |
| Copy (`OpCopy`) | new at the destination |
| Delete, then recreate the path | new (creation expects absence) |

Comments, references, and logical history attach to the FileID through the derived `FileMetaStore`.

## What exists now

- `Publisher.Publish`: validates requests, replays completed retries, prepares candidates (rebase onto the current head with declared-dependency validation), evaluates policy, validates candidate/permit binding, attempts atomic publication, and performs bounded safe retries.
- `FileService`: filesystem operations over the ports — authorization per path, verified ingest, explicit preconditions (nil expectations are rejected, never defaulted), atomic multi-file commits, snapshot-consistent reads, transfer limits.
- `WorkingCopyManager`: checkout of a pinned snapshot, durable journal of pending edits and base versions, bounded concurrent staging, atomic batch save, idempotent publication with receipt replay, reconnect conflict detection, and eviction of only re-fetchable content.
- `Capabilities`, `SearchIndex` (+ an authorization wrapper), `FileMetaStore`, and the `DocumentSession` boundary as contracts.

Local moves require both paths to be free of other unconfirmed edits. Publish
or resolve those edits before moving. Move intent is journaled before cached
paths change; storage failures are returned, and retries preserve saved batch
identity. Working-copy copy operations remain explicitly unsupported.

The storage, candidate-building, snapshot, and policy interfaces are contracts. There is no PostgreSQL/S3 adapter, persistent tree implementation, full merge engine, HTTP/MCP server, or production authorization layer yet. The in-memory implementations in the test files exercise the contracts but do not establish database durability, distributed locking, real content merging, or production outbox delivery.

## Integration requirements for the next adapters

1. Bootstrap a space, its genesis changeset/ref, and a workspace through authorized administration code.
2. Implement verified ingest plus metadata registration. A blob must be marked verified and retained before a draft references it; `BlobStore.Put` alone does not publish a file.
3. Implement persistent, structurally shared tree nodes (bounded-size directory pages, no full-namespace rewrites) and canonical changeset hashing. Validate paths, expected old values, cross-space references, and mutation/input consistency; mint FileIDs per the identity rules.
4. Implement draft save, resolve-and-pin, and workspace transactions with GC barriers.
5. Implement `TryPublish` exactly as documented in [store.go](../internal/workspace/store.go), including current authorization and policy checks inside the atomic update.
6. Implement the builder to check both file and directory dependencies against the candidate base and to rebase saved deltas onto the current head before publication. File digests alone cannot catch directory phantom reads.
7. Implement authenticated, snapshot-bound policy evidence and an outbox dispatcher with at-least-once delivery and deduplication.
8. Expose capability discovery in every transport (REST, CLI, MCP, mount); a transport must enforce a requested guarantee or return `ErrUnsupportedCapability` — never silently drop a precondition.
9. Implement a file-backed `SyncJournal` and `LocalCache` (write-ahead: journal before acting; never evict content with unconfirmed entries), and a search worker feeding from outbox events.

## Check from the repository root

```sh
gofmt -w internal/workspace/*.go
go vet ./...
go test -race ./...
```

With golangci-lint v2.15.0 installed, run the expanded audit profile from this
repository root:

```sh
golangci-lint run ./...
```

The profile checks production and test files, including discarded errors and
formatting. Regression tests also cover move failures during cache reads,
writes, deletion, and journaling; preservation of unpublished edits; saved-move
retries; stream read/close errors; and pin-release failures.

Tests cover: concurrent independent publications, duplicate concurrent requests converging on one receipt, rebuilding and revalidating after head movement, original-receipt replay, idempotency key misuse, strict CAS, stale inputs, required approval, rejecting evidence for another target, bounded retries, cancellation, file-identity preservation across edits/moves/copies/recreations, structured precondition failures, all-or-nothing multi-file commits, capability and transfer-limit enforcement, authorized search filtering, checkout/sync publication, crash recovery at every sync stage, idempotent duplicate syncs, reconnect conflict detection with resolution, bounded upload concurrency, and eviction that preserves unconfirmed edits.
