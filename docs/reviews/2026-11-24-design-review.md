# Org-wide workspace for humans and AI agents: design review and proposed revision

Date: 2026-11-24  
Input: “Design Doc: Org-Wide Distributed File System for Humans + AI Agents”  
Status: Proposed amendments for review. The supplied draft is preserved.

## Recommendation

Keep S3-compatible object storage, transactional metadata, immutable versions, explicit conflicts, agent identity, and MCP access. Reorganize the system around **saving immutable changes independently of publishing them to shared state**. Make ordinary work proportional to the files changed or requested.

The product is an online, versioned organizational workspace with distributed storage and caches. A space is its transaction and administration boundary. There is one authoritative `main` **per space**; an organization-wide view combines those spaces.

The recommendations below are architecture proposals for this project. They are not claims that Cursor or Anthropic implemented these exact APIs or policies.

## 1. What the research establishes

| System and primary source | Relevant published design |
|---|---|
| Cursor Origin / Continuity, [“Git at any scale”](https://cursor.com/blog/git-at-any-scale), August 18, 2026 | Origin retains Git. Its storage system, Continuity, persists pushes in an S3 WAL, then publishes them through an atomic index update. Local Git repositories are reconstructable caches. Replicas check freshness against the authoritative index; compaction results are reused. |
| Anthropic Scion, [“Redesigning source control at Anthropic”](https://www.anthropic.com/research/redesigning-source-control), October 13, updated October 15, 2026 | Scion uses server-created changesets containing file deltas. Creating a changeset does not require moving a named ref. Derived indexes serve history and other queries. Clients request sparse data through server APIs and caches. Anthropic describes an unusual workload and does not recommend Scion as a general Git replacement. |

These are materially different architectures. The draft’s dismissal of Git as inherently limited by a single pack mutex is too broad. Reframe §15 around workload fit: standard Git hosting remains a credible option for a code-centric product; arbitrary documents, document policies, and the proposed API favor a custom workspace service.

Do not transplant Continuity’s S3 WAL/index into this SQL design as a second authority. The useful principle is a clearly identified durable publication point. Scion supplies useful ideas for reducing shared coordination and unnecessary data movement.

## 2. Main amendments, in priority order

| Priority | Draft sections | Proposed change | Result |
|---|---|---|---|
| P0 | §7–8, §9 | Separate changeset creation from publication to `main`. | Agents can save progress without competing for a shared ref. |
| P0 | §7, §14 | Replace full snapshot manifests with structurally shared, sharded trees. | A small edit does not rewrite the entire namespace. |
| P0 | §8.2 | Add server-side rebase with explicit write preconditions and read dependencies. | Independent writes can land automatically; stale assumptions remain detectable. |
| P0 | §8.1, §9 | Require one snapshot handle across related reads and paginated listings. | Consistency extends across requests, not just within one response. |
| P0 | §8.4 | Keep unresolved merges outside protected `main`; bind checks and approvals to exact inputs. | Conflict markers and invalid documents never become published truth by default. |
| P0 | §6, §10, §13 | Specify verified ingest, durable acknowledgement, explicit idempotency, and a transactional outbox. | Uploads, retries, publication, and events have defined failure behavior. |
| P1 | §8.3 | Make reservations optional, short, and fenced. Never bypass publication validation. | Expired or paused writers cannot regain authority accidentally. |
| P1 | §9, §14 | Add batch reads, sparse materialization, server-side diff/history, and bounded caches. | Agents fetch the subset their task requires. |
| P1 | §8.5 | Treat journals as versioned streams with explicit ordering and watermarks. | Concurrent append behavior becomes implementable and testable. |
| P1 | §9.3, §11–12 | Tighten gateway, authorization, audit, and garbage-collection contracts. | Compatibility and cleanup preserve the same correctness guarantees. |

## 3. Revised core data model

```text
Changeset {
  id, schema_version, space_id,
  parents: [changeset_id, ...],
  delta: [put | delete | rename, ...],
  author: {principal_id, owner_id, run_id, tool},
  message, created_at,
  provenance: {source_changeset?, input_snapshots?, external_refs?}
}

Ref {
  space_id, name, head_changeset_id, generation
}

Workspace {
  id, owner, visibility, base_changeset_id,
  head_changeset_id, expires_at
}

SnapshotHandle {
  space_id, changeset_id, retention_pin_id, expires_at
}

BlobRegistration {
  security_domain, sha256, size,
  storage_key, storage_version?, verification_state, gc_state
}

DerivedTree {
  changeset_id, index_format_version, root_hash, readiness
}
```

A changeset describes modifications relative to its first parent. For a merge, its delta describes the fully resolved result relative to that parent. Compute its ID from a versioned, canonical serialization; server-assigned time and authenticated attribution are fixed once per idempotent request. User-supplied author strings do not establish identity.

Tree roots, path history, search, and ancestry indexes are derived views. Their encoding can change without changing the public changeset ID. Required snapshot lookup structures must be ready before returning a snapshot as readable; optional indexes can catch up asynchronously.

Named branches remain available for humans and teams. Agents normally work with private workspace handles and immutable changeset IDs. Workspace head updates may use CAS locally to that workspace; saving a standalone changeset does not update a shared branch. Workspace visibility is an additional authorization constraint.

### Incremental tree representation

Use a persistent directory tree with bounded-size, sharded directory pages and structural sharing. Updating a file creates new nodes along affected paths and reuses unchanged subtrees. Bound fanout so one enormous directory does not recreate the flat-manifest problem.

Let `N` be total files, `k` changed files, and `d` index depth. Target approximately `O(k × d)` node work, plus the actual bytes changed, rather than `O(N)` manifest rewriting. Batch shared ancestor work. Normal reads must use the materialized tree, not replay an unbounded delta chain.

Illustration: at 120 bytes per manifest entry, one million files produce a 120 MB uncompressed map. Rewriting it 100 times per second means 12 GB/s of logical manifest processing before content I/O. These are illustrative assumptions, not measured performance. Compression reduces stored bytes but does not eliminate full-map traversal.

Keep flat manifests only as an explicitly bounded prototype option if the first release drops the million-file scale claim.

### Cross-space scope

Atomic commits apply within one space. An organization snapshot can record selected `{space_id, changeset_id}` pairs, but collecting those pairs independently does not create a globally simultaneous snapshot. Cross-space atomic publication requires a separate protocol and is outside v1.

## 4. Revised save and publication protocol

### 4.1 Upload and save

1. Authorize an upload and issue a short-lived URL for a staging object.
2. Complete upload; verify size and the full file digest against the exact staged object/version. Use a trusted backend checksum where it supplies the required digest semantics; otherwise a worker reads and hashes the bytes. A multipart ETag is not the file’s SHA-256.
3. Promote verified bytes into the immutable content namespace and register their durable storage location. Enforce create-only writes and restrict deletion. Clients cannot overwrite finalized blobs.
4. Persist the changeset, its blob references, workspace retention protection, audit/outbox entries, and its idempotency result in one metadata transaction. Return the immutable changeset ID when durable and its required read view is ready.

This saves work without changing `main`. Stage uploads outside the SQL transaction. A failed metadata write can leave an orphan to collect; it must not leave a published reference to an incomplete upload.

S3 objects are not inherently immutable: AWS documents that ordinary writes can overwrite an existing key. Enforce immutability with conditional operations and policies; qualify other S3-compatible backends against the required behavior. See [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

Direct upload keeps bulk transfer off the API frontend. Verification, merging, document extraction, and indexing workers may still read content, correcting §14’s claim that the system only ever touches hashes.

### 4.2 Publish to a shared ref

1. Authenticate the principal and authorize the proposed paths and destination ref.
2. Read destination head `H`. Check the proposal’s write preconditions and declared read dependencies against `H`.
3. If `main` advanced since the proposal’s base, compute a candidate rebased or merged result outside the short publication transaction. Preserve provenance back to the original proposal.
4. Run required document validation and obtain approvals for the candidate. Record exactly which content, dependencies, and policy version each check covers.
5. In one SQL transaction, verify the destination still equals `H`; validate current permissions, policy version, applicable reservation epochs, and GC-safe blob registrations; update the ref; append publication audit/outbox records; and persist the idempotency result.
6. Acknowledge publication after the transaction satisfies the configured durability/failover policy. Return the published snapshot ID and ref generation.

If the head changed during preparation, retry within a bounded budget. If validation must examine the entire candidate snapshot, changing that snapshot requires rerunning validation. Reuse narrower checks only where their declared input hashes still match.

A hot shared ref still serializes publication. This design reduces the duration and frequency of shared coordination; it does not eliminate it. Retain a strict `expected_head` mode for callers that explicitly require rejection after any head movement.

### 4.3 Distinguish conflicts from stale assumptions

Consider two agents starting at `main@41`:

| Concurrent work | Publication behavior |
|---|---|
| A edits `marketing/launch.md`; B edits `finance/budget.md`, with no overlapping declared dependencies | Publish A, then rebase and publish B automatically if checks pass. |
| A and B replace the same file incompatibly | Preserve both saved changesets; return a structured content conflict. |
| B reads `pricing.json` and writes `forecast.md`; A changes `pricing.json` | If B declared that input dependency, require recomputation or explicit resolution. Different output paths do not prove independence. |
| B lists a directory to establish that no contract exists; A adds one | A directory/subtree fingerprint can detect this phantom dependency. |

The API must support both expected old values on writes, including expected absence, and declared read sets. Clients should record dependencies as they read. The service cannot infer every fact an agent relied on, especially from external tools. Do not claim application-level serializability beyond the dependencies actually validated.

Use distinct errors such as `CONTENT_CONFLICT`, `DEPENDENCY_CHANGED`, `HEAD_MOVED`, and `LEASE_EXPIRED`, with retryability and resolution inputs. Avoid sending an entire commit diff in every error; provide bounded conflict descriptors and authorized retrieval handles.

## 5. Reads, APIs, and derived data

Two separate calls with `ref=main` can resolve to different commits. An immutable snapshot only provides a stable multi-file view when the caller reuses its ID.

```text
dfs_snapshot(space, ref="main", ttl) -> snapshot_handle
dfs_batch_read(snapshot_handle, paths[]) -> contents + hashes
dfs_list(snapshot_handle, prefix, cursor, limit) -> page
dfs_diff(base_snapshot, target_snapshot, prefix?, cursor?)
dfs_history(snapshot, path, cursor?, limit?)

dfs_create_changeset(workspace, parent, ops, message, idempotency_key)
dfs_publish(changeset, target_ref, dependencies, mode, idempotency_key)
dfs_conflicts(operation_id) -> authorized conflict descriptors
```

Make base snapshots/preconditions mandatory for replacement writes. Convenience APIs may combine save and publish but must retain the same semantics. Return snapshot identity on every response; continuation tokens bind to the snapshot, query, and visibility scope.

History, ancestry, merge-base, and diff operations execute on the server. Offer sparse export/materialization for tools that require local files; bound disk caches. Batch immutable node/blob requests to avoid one remote round trip for every graph edge.

Cache immutable content by security domain and hash. Resolve `latest` from the authoritative metadata primary or a replica proven to satisfy the required freshness boundary. TTL-only ref caching does not provide linearizable latest reads. Do not serve stale `main` silently during a partition.

Start with one service region and an ordinary shared cache. Add node-local and regional cache tiers only after measured demand justifies them. If added, limit cache-miss traffic so a cold or failed tier cannot overload its upstream.

Search returns its indexed snapshot or coverage watermark and reports lag. Current authorization applies even to old snapshots, cached results, historical diffs, conflict descriptors, and event replay. A content hash is never an access credential.

## 6. Authority, recovery, events, and retries

For v1, SQL is authoritative for changesets, refs, policies, publication records, and operation deduplication; the object store is authoritative for registered immutable bytes. Refs, policies, and identities cannot be recovered from blobs alone. Define database backup, point-in-time recovery, and a tested restore procedure alongside object retention.

State the linearization point: a publication becomes visible when its SQL transaction commits. Configure database durability to match the accepted-write loss objective under the supported failover scenarios. Search, notifications, and disposable caches are rebuildable views.

Use a transactional outbox for mutation events: ref update, audit record, and outbox insertion commit together. Delivery is at least once. Consumers deduplicate by `event_id` and can resume from an opaque committed-log cursor. Preserve ref-generation order for publications to the same ref; do not promise a total order across independent workspaces. A database sequence allocated before transaction commit is insufficient by itself as a lossless replay cursor. See [AWS transactional outbox guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html).

An explicit idempotency key is scoped to organization, principal, operation, and request digest. Persist the accepted result atomically with the mutation. Same key and same request returns the original result; same key and different request is an error. Retain keys for a documented retry window. Hashing only `(base, ops, blob hashes)` neither captures all intent nor distinguishes every legitimate repeat operation.

Verifying an object before publication prevents publishing unverified uploads. It does not prove that data can never be deleted or corrupted later. Restrict destructive storage access, coordinate GC, detect corruption, and specify repair/recovery behavior.

## 7. Reservations and merge policy

### Reservations

Long agent tasks belong in private workspaces. Introduce prefix reservations only for workflows that need temporary exclusion at publication time, and prefer short durations.

If enforced, a reservation is a lock with expiry, not merely advisory. Scope it to `(space, target_ref, normalized_prefix)` and return a monotonically increasing fencing epoch. At the mutation boundary, atomically check the current reservation, epoch, ownership, and validity. Coordinate ancestor/descendant prefix acquisition so overlapping reservations cannot race through separate row checks. Validate both sides of renames and their affected descendants.

A reservation never skips shared-ref validation or permits overwriting unrelated changes. A paused worker resuming after expiry must fail even if it still holds its old token. See the [etcd fencing example](https://github.com/etcd-io/etcd/blob/main/contrib/lock/README.md).

### Merges and approvals

The default destination policy rejects unresolved conflict markers, broken structured data, and unresolved binary conflicts. Store resolution work and all input versions in the proposal workspace; publish the resolved result explicitly.

| Type | Default behavior |
|---|---|
| Plain text / Markdown | Deterministic three-way merge; unresolved regions require resolution. |
| JSON | Merge only where the schema defines compatible key/collection behavior; validate the result. |
| YAML | Conservative handling; reject ambiguous duplicate keys and unsupported constructs. |
| CSV | Require a configured stable row key for semantic merging; otherwise treat conservatively. |
| PDF, Office files, other binaries | Preserve input versions and request explicit selection or a format-specific merge. |

A syntactically clean merge can still be semantically wrong. Apply path/type policies: generated low-risk material may auto-publish after checks; sensitive contracts or financial documents may require authorized approval. Bind approvals to exact approved content and dependencies, and reevaluate after changes.

## 8. Journals are streams with snapshot semantics

Replace the claim “no CAS conflict, ever; concatenate by timestamp” with a defined stream contract:

- Each append has a stable record ID, writer identity, and writer-local sequence; retries deduplicate.
- Define whether ordering is per writer, causal, or total. Wall-clock timestamps alone do not establish a reliable total order. Total stream ordering requires coordination.
- A snapshot records a stream watermark or segment manifest so later appends do not change an old snapshot’s logical file.
- Compaction preserves record IDs, chosen order, and snapshot-visible boundaries.
- Journals do not silently inherit arbitrary branch/merge semantics; define those separately.

Appends can avoid the document branch-head CAS, but they still require authorization, durable indexing, ordering where promised, and deduplication. Defer journals until a measured workload justifies this separate storage abstraction.

## 9. Compatibility, access, and retention

**S3 gateway.** Expose logical paths through a virtual gateway; the underlying hash-keyed bucket is not itself a user-facing filesystem. Make snapshot IDs part of gateway addressing, or bind a mount to a snapshot. Ordinary repeated `GET`/`LIST` calls against `latest` do not provide a stable session automatically. Start read-only; do not infer multi-file commit boundaries from arbitrary raw S3 writes.

**Authorization and deduplication.** Authorize path and snapshot context before issuing blob capabilities. Scope physical deduplication to compatible encryption, retention, and access domains, with space-level isolation as a conservative default. Prevent hash existence probes from becoming a cross-domain information channel. Account for classification and approval rules when moving files into less restricted paths.

**Audit.** Distinguish issuing a download URL, observing object requests, and a person actually consuming content. A presigned URL issuance record does not prove who downloaded the bytes. Use a mediated download path when strict per-request identity attribution is required; otherwise state the capability’s lifetime and audit limitations. Record mutations with authenticated run identity and input snapshot provenance. Restrict audit-log mutation and define any stronger tamper-evidence requirements explicitly.

**GC.** Roots include retained history, tags, workspace heads, active snapshot handles, proposals under review, in-flight verified uploads, legal holds, and any journal watermarks. Creating or extending a root and marking an object for deletion must coordinate through a generation/epoch barrier. A grace period alone is not sufficient: a new snapshot can otherwise pin an old object after mark and before sweep. Publication/pinning must reject or restore objects already fenced for deletion.

Deleting a branch does not immediately delete its retained ancestors. Likewise, a 30-day retention policy cannot simply erase old parent deltas if current state still depends on them. History truncation requires a self-contained checkpoint and an explicit policy for inaccessible older revisions.

## 10. Revised MVP and validation

### MVP

1. One service region, HA Postgres with tested recovery, and one qualified object backend.
2. Verified immutable blobs; incremental trees; saved changesets; workspace retention pins.
3. Snapshot/batch/list APIs; explicit idempotency; save versus publish; strict CAS and conservative rebase.
4. Write preconditions and declared input dependencies; text conflicts kept in workspaces; schema validation where configured.
5. Authenticated agent identity, current ACL checks, protected publication policy, audit, transactional outbox, and resumable events.
6. Retention-safe GC before operating beyond a capped pilot; operational quotas and storage limits.

Defer shared-prefix reservations, general structured merges, journals, an S3 gateway, and regional cache fleets until specific usage requires them. This concentrates the first release on snapshot and publication correctness.

### Required correctness scenarios

| Scenario | Expected evidence |
|---|---|
| Concurrent edits to different paths | Both publish without lost changes when declared dependencies allow it. |
| Concurrent edits to one path | Incompatible replacements produce a conflict and preserve both proposals. |
| Changed input or directory membership | Publication rejects a stale declared dependency. |
| Lost response after successful mutation | Retrying one idempotency key returns the original accepted result. |
| Crash before/after publication transaction | No partially visible multi-file state; committed events remain deliverable. |
| GC races with pinning/publication | A live acknowledged snapshot never loses objects to the collector. |
| Permission changes during work | New reads and publication apply the current authorization contract. |
| Snapshot used over many pages/reads | Every response resolves within the same snapshot. |
| Expired reservation holder resumes, if reservations ship | Old epoch cannot publish. |
| Restore after supported database failure | Registered bytes, refs, publication records, and retention roots remain coherent. |

Measure increasing namespace size with a fixed tiny edit, many agents saving private work, publication to one hot ref, and sparse reads with cold/warm caches. Report throughput, p95/p99 latency, retry/conflict rate, bytes transferred, index lag, and cost. Specify file sizes, changed paths, and per-space concurrency.

Keep 10³ users / 10⁴ agents / 10⁷–10⁹ files as an eventual workload hypothesis until benchmarked. Replace the single “commit <150 ms” objective with separate measurements for metadata-only saves, verified-upload completion, publication without checks, publication with checks, and event delivery. Do not copy either vendor’s published performance claims into this project’s capacity plan.

## Sources

1. Cursor, Vicent Martí, [Git at any scale](https://cursor.com/blog/git-at-any-scale), August 18, 2026.
2. Anthropic engineering, [Redesigning source control at Anthropic](https://www.anthropic.com/research/redesigning-source-control), October 13, updated October 15, 2026.
3. AWS, [How to prevent object overwrites with conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html), accessed November 24, 2026.
4. AWS Prescriptive Guidance, [Transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html), accessed November 24, 2026.
5. etcd maintainers, [Lease expiry and fencing example](https://github.com/etcd-io/etcd/blob/main/contrib/lock/README.md), accessed November 24, 2026.

Research boundary: this review uses published primary engineering descriptions and official documentation. It is not an audit of proprietary implementations or independent validation of vendor benchmarks.
