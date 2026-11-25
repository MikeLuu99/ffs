# Design Doc: Org-Wide Distributed File System for Humans + AI Agents

Status: **Draft — for review**
Author: —
Date: 2026-11-24

---

## 1. Summary

A single, org-wide distributed file system ("the DFS") that acts as the **ground truth** for documents and artifacts across every team. Its two primary user classes are:

1. **Humans** — via CLI, web UI, and existing tools (S3-compatible clients).
2. **AI agents** — via a first-class API/MCP tool surface, which will rapidly become the dominant consumer.

The core technical problems this doc focuses on are:

- **Versioning**: every change is tracked, attributable, and recoverable.
- **Concurrency**: many humans and agents read/write simultaneously without silent data loss.

The data plane is **S3-compatible object storage**; a **metadata service** provides filesystem semantics (paths, snapshots, branches, merges, leases) on top of it.

---

## 2. Background & Motivation

- Agents are no longer confined to engineering roles. Marketing, legal, finance, ops teams all run agents that need to produce and consume shared documents.
- Cross-team document exchange today happens through email, chat, and ad-hoc shares — there is no machine-readable source of truth, so agents hallucinate, stale-copy, or duplicate work.
- Agents collaborate with each other. A shared, versioned, observable workspace turns "two agents in a chat thread" into "two agents committing to a shared repo with visible history."
- "Ground truth with freedom": the org needs one authoritative state (`main`), while teams and agents need room to experiment (`branches`/`workspaces`) without stepping on each other.

### Why S3-like storage is a good fit (and what it doesn't give us)

| S3 gives us | S3 does NOT give us |
|---|---|
| Cheap, durable, elastic capacity | Directory semantics (rename, atomic move, listing at scale) |
| Native object versioning & lifecycle rules | Cross-file atomicity (multi-file commits) |
| Immutable PUT semantics — great for content addressing | Conflict detection/merging between concurrent writers |
| Conditional writes (ETag / If-Match) on a single key | Efficient metadata queries ("files changed by agent X today") |
| Presigned URLs — safe delegated access | Access control finer than bucket/prefix |
| Standard tooling (R2, MinIO, on-prem gateways) | Eventing, watching, schemas |

**Conclusion**: S3 is the right *data plane*, but the hard problems — versioning, conflicts, concurrent reads/writes — live in a **metadata layer** we build. That layer is the heart of this design.

---

## 3. Goals / Non-Goals

### Goals

- G1. Every mutation is versioned, attributed (human/agent/service), and auditable.
- G2. Readers always observe a **consistent snapshot**; no torn reads across multi-file changes.
- G3. Concurrent writers never silently overwrite each other; conflicts are detected and resolvable (including by agents).
- G4. Branching model: `main` = org ground truth; teams/agents branch freely and merge explicitly.
- G5. Agent-first API: simple, deterministic, self-describing, MCP-compatible.
- G6. Human tool compatibility: S3-compatible read path, mountable via existing S3 FUSE drivers where useful.
- G7. Scales to org size: ~10³ users, ~10⁴ concurrent agents, ~10⁷–10⁹ files, files from bytes to multi-GB.
- G8. Full audit trail: who/what read or wrote what, when — this is what makes it "ground truth" trustworthy.

### Non-Goals (v1)

- N1. Real-time collaborative rich-text editing (Google-Docs-style CRDT editing). We provide coarse versioning + merge; fine-grained co-editing can come later per doc type.
- G2/N2. POSIX semantics (byte-range file locking, O_APPEND, mmap). Not a kernel filesystem.
- N3. Replacing the data warehouse / blob CDNs. This is documents and artifacts, not analytics tables or video delivery.
- N4. End-to-end encryption at v1 (design leaves room; see §13).

---

## 4. Key Design Drivers

Because **agents are first-class users**, several decisions differ from a classic DFS:

1. **Deterministic, inspectable concurrency.** Agents handle explicit `409 Conflict` responses far better than hidden last-write-wins. Every response carries version metadata so an agent can reason about it.
2. **Structured metadata on every write.** Authors, intent messages, and tool identity are mandatory, not optional. This powers attribution and search.
3. **Leases, not locks, for long tasks.** Agents crash, hang, and get killed mid-task. TTL-based leases expire automatically; no deadlock cleanup odyssey.
4. **Append-friendly high-contention mode.** Many agents will want to write to the *same* file (shared logs, status boards). We provide append-only journal paths where concurrent writes are trivially concurrent (concatenation), instead of forcing everyone through merge.
5. **Event stream.** Agents coordinate by reacting to events (`file.committed`, `lease.acquired`, `merge.completed`), not by polling.
6. **Everything addressable by stable IDs.** Snapshots and blobs are content-addressed and immutable — an agent can pin "the spec as of yesterday 3pm" safely.

---

## 5. High-Level Architecture

```
                ┌────────────────────────────────────────────┐
                │              Clients                       │
                │  AI agents (MCP) │ CLI │ Web UI │ S3 tools │
                └───────┬───────────────┬────────────┬──────┘
                        │ API (REST/WS) │            │ S3 API
                        ▼               │            │ (read-mostly)
        ┌───────────────────────────┐   │            │
        │      API Gateway / Auth    │◄──┘            │
        │  (identity, ACL, rate limit)│                │
        └───────────┬────────────────┘                │
                    │                                 │
                    ▼                                 │
        ┌───────────────────────────┐        ┌────────┴────────┐
        │      Metadata Service     │        │  Data Plane     │
        │  (branches, commits, trees│        │  S3-compatible  │
        │   leases, merge, events)  │        │  object store   │
        │   Transactional DB (SQL)  │        │  (R2/S3/MinIO)  │
        └───────┬──────────┬────────┘        └────────┬────────┘
                │          │                          │
                │          ▼                          │
        ┌───────┴───┐  ┌──────────────┐   presigned │
        │ Event Log │  │ Merge/Verify │    uploads  │
        │ (pub/sub) │  │   Workers    │◄────────────┘
        └───────────┘  └──────────────┘
```

**The golden rule**: *the data plane stores immutable content; the metadata layer owns all mutable state (paths, branch pointers, leases).*

- Clients **never** talk to S3 with real credentials; the metadata service issues **presigned URLs** for upload/download.
- S3 keys are content addresses; nothing in S3 is ever mutated in place. All "mutation" is committing new metadata.

---

## 6. Storage Layout (Data Plane)

All objects are **content-addressed by SHA-256**. Key layout:

```
s3://{bucket}/
  blobs/ab/cdef1234...            # file content (immutable), deduped by hash
  trees/{space}/{commit_id}.json.zst  # manifest: path -> blob ref for that snapshot
  tmp/{upload_id}                 # multipart upload staging, lifecycle-expired
```

- `blobs/` is globally deduplicated: two teams storing the same PDF store it once.
- Writes are `PUT` + integrity check (hash verified on ingest). Immutability means cacheability everywhere (CDN-friendly reads later).
- S3-native bucket versioning is enabled as a **belt-and-suspenders** safety net, but our real versioning lives in the metadata layer (S3 versioning alone can't express multi-file snapshots or merges).
- `tmp/` has a lifecycle rule (e.g., expire after 3 days) to garbage-collect abandoned uploads.

---

## 7. Versioning Model

A **Git-inspired model, simplified for document workloads**:

```
Commit {
  id:          hash (content-addressed)
  parents:     [commit_id, ...]        // 1 parent = normal, 2 = merge
  space_id:    which space this belongs to
  tree_ref:    pointer to manifest (S3 trees/ key)
  author:      {principal_type: human|agent|service, id, tool}
  message:     required human/agent-readable intent
  ts:          timestamp
}

Branch = named mutable pointer: { name -> commit_id, version }   // `version` enables CAS
```

- **Namespace**: `spaces` are the unit of administration and ACLs (e.g., `marketing`, `platform`, `company-wide`). Paths inside a space are normal-looking: `specs/api-v2.md`.
- **Snapshots are multi-file**: one commit can change any number of paths atomically. Agents doing "write report + update index" do it in one commit — no torn state ever visible.
- **History is a DAG per space.** `main` is the ground truth; branches are cheap pointers.
- **Immutability**: commits and blobs are immutable. Branch pointers are the only mutable handles, and they move only via compare-and-swap (§8).
- **Tags/Pins**: immutable named pointers (`releases/q3-report`) + pinned snapshot IDs. Agents can pin a snapshot to guarantee stable input across a long task.

### Tree storage strategy

- MVP: each commit's manifest is a full `path → blob` map (compressed) in `trees/`. Simple, debuggable, fine to ~10⁶ files/space.
- Later (if needed): prefix/merkle tree chunks (Git-style) with delta storage; the API doesn't change.

---

## 8. Concurrency Control (the core problem)

### 8.1 Reads — trivially consistent

Any snapshot (commit ID) is immutable, so a reader that pins `ref=main@<commit>` sees a perfectly consistent multi-file view for any duration. Reads of `main` "latest" resolve a pointer and read that snapshot — a reader never sees half a commit.

### 8.2 Writes — optimistic concurrency (OCC) by default

Every branch pointer carries a monotonic `version`. Moving a pointer (committing) is a **compare-and-swap**:

```
POST /spaces/{s}/commits
  base: { ref: "main", version: 41 }      // "I built on main@v41"
  ops:  [ {put: path, blob_sha}, {delete: path}, ... ]

→ 200 { commit: "c9f2...", new_version: 42 }        // won the race
→ 409 { current_version: 42, current_commit: "e77a...", diff: {...} }  // lost the race
```

- Single-writer-small-file fast path: a one-file PUT is sugar for a one-op commit (same CAS machinery).
- **On 409, the client (usually an agent) re-reads, merges or re-bases, retries.** The 409 body includes the conflicting commit's diff so an agent can decide autonomously.
- Retries are safe because commits are idempotent per (base, ops, blob shas) — a retried commit that already landed returns the original result.

### 8.3 Leases — for long-running multi-step work

OCC retries are wasteful for a 20-minute agent task writing many files. Writers can take a **lease** on a path prefix:

```
POST /spaces/{s}/leases { path: "reports/q3/", ttl: 900s, holder: agent-123 }
→ 200 { lease_id, expires_at }      // exclusive write access under that prefix
→ 409 { holder }                    // someone else holds it
```

- Leases are **TTL-bound and must be heartbeated**; expiry is automatic → crashed agents can't deadlock the system.
- Lease holders commit with their lease token (skipping CAS conflict on that prefix); others get a clean `423 Locked` with expiry info instead of racing blindly.
- Leases are advisory *within our API* — the metadata service enforces them at commit time.

### 8.4 Conflict resolution at merge time

Merging branch → `main` uses the common ancestor as the merge base:

| Content | Default strategy |
|---|---|
| Text (incl. markdown, code, csv) | Three-way line merge; clean merge auto-commits, real conflict → merge commit with conflict markers + `conflict` flag |
| Structured (JSON/YAML) | Three-way merge at key level; type collisions surface as conflicts |
| Binary | No auto-merge: both versions preserved, conflict artifact created (`name.conflict.{a,b}.ext`) — a human or agent resolves explicitly |
| Append-only journals (§8.5) | Trivial: concatenation, order by commit timestamp |

Agents resolve conflicts programmatically: the merge API returns structured conflict descriptors (paths, base/theirs/ours blob refs), so an agent can fetch all three versions, reason, and commit the resolution — or escalate to a human via the conflict flag + event.

### 8.5 High-contention paths: journal mode

For paths where many agents write simultaneously (shared status files, scratchpads, aggregation logs), users opt into **journal mode**:

- Writes become **appends** of length-prefixed records (JSONL) into per-writer segments under the hood.
- Concurrency = O(1): no CAS conflict, ever; readers get a merged logical view (concatenation + periodic compaction).
- This converts the hardest contention case (N agents → 1 file) into the easiest.

### 8.6 Metadata transactionality

All pointer/lease/commit state lives in a transactional SQL database (e.g., Postgres — `Bun.sql`). Commit = one ACID transaction: validate base version → insert commit row → update pointer. This is what makes CAS correct. The DB stores metadata only (small, hot, cacheable); bytes live in S3.

---

## 9. API Surface

### 9.1 REST (client-facing)

```
# Content
GET    /spaces/{s}/file?path=&ref=main          → bytes + {version, commit, sha} headers
PUT    /spaces/{s}/file?path=   (If-Match)      → 200 | 409
POST   /spaces/{s}/commits                      → multi-op atomic commit (§8.2)
GET    /spaces/{s}/log?ref=&limit=              → history
GET    /spaces/{s}/tree?ref=                    → listing w/ metadata

# Branching & merging
POST   /spaces/{s}/branches {name, from}
POST   /spaces/{s}/merge {from, to, strategy}   → structured conflicts if any (§8.4)

# Coordination
POST   /spaces/{s}/leases                       → §8.3
GET    /spaces/{s}/events (SSE) / webhook reg   → §10
```

### 9.2 MCP tool surface (agent-facing) — same semantics, tool-shaped

```
dfs_read(path, ref?)            → content + version metadata
dfs_write(path, content, base_version?, message)   → new_version | conflict details
dfs_append(path, record)        → journal-mode paths (§8.5)
dfs_list(path_prefix, ref?)
dfs_history(path, limit?)
dfs_branch / dfs_merge
dfs_lease(path, ttl) / dfs_release(lease_id)
dfs_watch(path_prefix)          → event subscription
dfs_search(query, ref?)         → metadata + content search
```

Agents discover semantics through tool descriptions + structured error payloads (e.g., a 409 includes actionable fields). **No undocumented magic** — agents can't ask a colleague.

### 9.3 S3 compatibility

Read-mostly S3 API gateway (same bucket, presigned or proxied) so existing S3 tooling/mounts work for humans. Writes through raw S3 go to a quarantine prefix and are ingested as commits by the metadata service (so versioning is never bypassed).

---

## 10. Events & Coordination

Append-only event log (backed by the metadata DB / a queue), fanned out via SSE + webhooks:

```
{type: "file.committed", space, path, commit, author: {type: agent, id}, ts}
{type: "merge.completed" | "conflict.raised" | "lease.expired" | ...}
```

- Agents subscribe to prefixes; a reviewer agent can watch `contracts/` and act on every commit.
- Every event carries commit IDs → events are verifiable against ground truth, not just notifications.

---

## 11. Identity, Access Control & Audit

- **Principals**: humans (SSO/OIDC), agents (service identity bound to an owning human/team), services. Agent identity is mandatory — every write says *which tool/agent* did it.
- **ACLs** at space + path-prefix granularity, evaluated in the metadata service on every API call; data-plane access only via short-lived presigned URLs scoped to a single blob.
- **Audit log**: every read and write is recorded (append-only). "Ground truth" requires provable provenance: *who* (principal + tool) wrote *what* (commit) *when*, and who has read what (for sensitive spaces).
- Per-space retention & legal-hold policies are metadata, not lifecycle hacks.

---

## 12. Garbage Collection

- Blobs/commits unreachable from any branch head, tag, pin, or active lease → collectable.
- Mark-and-sweep: periodically mark from live pointers (+ grace window, e.g., keep everything ≥ 30 days for `main` per policy), sweep unreferenced blobs from S3.
- Abandoned `tmp/` uploads expire via S3 lifecycle. Pinned snapshots and branches under legal hold are exempt.

---

## 13. Failure Modes & Consistency

| Failure | Behavior |
|---|---|
| Client crashes mid-upload | `tmp/` multipart orphan → lifecycle GC; nothing committed |
| Client crashes mid-commit | Commit is a single DB transaction → all-or-nothing |
| Lease holder dies | Lease TTL expires → prefix free again; partial work visible on their branch, not `main` |
| CAS race | Clean 409 with conflict details (§8.2) — safe by construction |
| Blob committed but S3 write lost | Commit records blob SHA; ingest verifies existence + hash before pointer flips → pointer never references missing/corrupt data |
| Metadata DB down | Fully unavailable (fail closed); data plane still readable via last-known presigned URLs — availability of metadata is our SLO focus (single-writer DB + standby; quorum replicaton later) |
| Duplicate agent retries | Idempotent commit keys → same result returned |

Consistency stance: **strong consistency for metadata** (SQL transactions), **immutable snapshots for content** — together this yields snapshot isolation for readers and serializable pointer updates for writers. We accept metadata-layer write unavailability over any inconsistency; agents can retry, but they cannot trust a system that lies.

---

## 14. Scalability Notes

- **Metadata DB**: the only serialization point. Commit rate is the limit, not bytes. Mitigations: hot-path caching of branch pointers and tree listings; per-space partitioning when needed; trees stored as S3 manifests keeps DB rows small.
- **Throughput**: all byte I/O is client ↔ S3 (presigned, direct) — the metadata service touches only hashes, never bytes. Scales with the object store.
- **Hot spaces** (thousands of agents on one space): journal-mode paths (§8.5) + read replicas for log/tree queries.
- Target SLOs (v1): p99 read-metadata < 50 ms, commit < 150 ms, event fan-out < 1 s.

---

## 15. Alternatives Considered

| Option | Verdict |
|---|---|
| **Git server as backend** (Gitea etc.) | Right model, wrong substrate: poor large-binary handling, weak auth granularity, concurrency bottlenecks (single pack mutex), not API-first for agents. We borrowed the model (branches, three-way merge, CAS), not the implementation. |
| **CRDT doc store** (Yjs/Automerge everywhere) | Beautiful for fine-grained co-editing of rich docs; heavy and awkward for arbitrary binaries and big trees. Keep as a per-doc-type add-on later (§2 N1). |
| **LWW (last-write-wins) on S3 alone** | Fails G1–G3 outright: silent overwrites, no multi-file atomicity, no provenance. Rejected as primary, but LWW is available *opt-in* for scratch spaces. |
| **POSIX/parallel FS (JuiceFS, Lustre over S3)** | Built for compute I/O, not collaboration: locks are transient, no meaningful version/merge/provenance semantics. |
| **Box/Drive/SharePoint APIs** | Human-document-centric, weak versioning semantics, rate limits hostile to agent workloads, no snapshot isolation. |

---

## 16. MVP Scope & Milestones

**MVP (prove the core):**
1. Metadata service (Postgres) + S3 data plane, presigned I/O, hash-verified ingest
2. Commits (single + multi-op), CAS branch pointers, snapshots, history
3. OCC + 409 conflict payloads; leases with TTL
4. Branch + three-way merge (text/JSON), binary conflict artifacts
5. MCP tool surface (`dfs_read/write/list/history/branch/merge/lease`)
6. Audit log + events (SSE)

**M2:** journal-mode paths, webhooks, web UI, S3-compat read gateway, GC daemon
**M3:** metadata search, agent-assisted merge suggestions, quota/reporting, per-space replication policies

**Deliberately deferred:** E2EE (encrypt-at-rest now, field-level E2EE later), CRDT doc types, POSIX mount (via S3 FUSE as stopgap).

---

## 17. Open Questions (for review)

1. **Scale reality check**: expected concurrent agents per space? (Decides how aggressively we need §8.5 at MVP.)
2. **Merge autonomy**: should agents be allowed to auto-merge into `main`, or is human approval mandatory for protected paths? (Proposal: per-path protection rules, like CODEOWNERS.)
3. **Storage choice**: cloud S3 vs R2 vs self-hosted MinIO for on-prem requirements?
4. **Metadata DB**: single Postgres with standby acceptable for v1, or do we need multi-region from day one?
5. **Retention**: default retention for `main` history (forever? 7 years? per-space?)
6. **Naming**: spaces map 1:1 to org teams, or free-form with ACL groups?

---

## Appendix A: Example — two agents, one file, zero lost writes

```
Agent-A: dfs_read("specs/api.md")         → v41 (sha X, commit c900)
Agent-B: dfs_read("specs/api.md")         → v41 (sha X, commit c900)
Agent-A: dfs_write("specs/api.md", A', base_version=41)   → 200, v42
Agent-B: dfs_write("specs/api.md", B', base_version=41)   → 409 {current: v42, diff}
Agent-B: dfs_read("specs/api.md")         → v42
Agent-B: merges A' + B' → B''; dfs_write(..., base_version=42) → 200, v43
```

No configuration, no lost bytes, and the audit log shows the full story — the "ground truth" property in one example.
