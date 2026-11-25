# golangci-lint audit

Date: 2026-11-24. Scope: all production and test Go files in `internal/workspace`.

## Resolution

The findings below describe the initial audit. They have now been addressed:
the unchanged expanded audit profile reports **0 issues**, including test
files, and `go vet ./...` passes. The race-enabled suite passed after every
logical change batch.

- Moves journal intent before changing cached paths, propagate storage errors,
  preserve source bytes after failed destination writes, and preserve saved
  batch identity on retries. Paths with other unconfirmed edits are rejected
  until those edits are published or resolved, preventing pending uploads from
  losing their content.
- File reads report stream close errors as well as read errors, and status
  calls report pin-release failures.
- Working-copy switches explicitly reject unsupported copy entries.
- Documentation, builtin shadowing, helper naming and signatures, discarded
  test errors, error wrapping, and test-only timestamp conversion were fixed.
- Permanent regression coverage lives in `workingcopy_move_test.go` and
  `cleanup_test.go`. It covers cache read/write/delete and journal failures,
  protection of unpublished edits at either move path, saved move replay,
  stream errors, and pin-release failures.

The original diagnostic output and standalone reproduction are retained as
historical evidence; the reproduction uses the pre-cleanup test-helper
signatures. Use the permanent tests with the updated module.

Tool: official golangci-lint v2.15.0 Darwin ARM64 release, built with Go 1.27.0;
project toolchain: Go 1.26.1, module language version: Go 1.22.
The downloaded archive's SHA-256 matched the GitHub release asset digest:
`d1e409481a8ca846c4bc97e781eb5cb1bd5c23353a0b3977a1cbda1b1b6447d5`.

## Initial results

The standard profile reports **4 diagnostics** (3 errcheck, 1 staticcheck).
The expanded [audit profile](../../.golangci.yml), including explicit checks of
errors assigned to `_`, reports **110 diagnostics**: 83 in production files
and 27 in test files. These are diagnostic counts, not distinct bug counts;
some rules overlap. The intermediate expanded run without strict errcheck
reported 95 diagnostics. gofmt and goimports reported no formatting issues.

| Analyzer | Diagnostics | Assessment |
|---|---:|---|
| errcheck | 18 | Includes a reproduced loss of unpublished content; also cleanup and test-only findings |
| revive | 79 | 53 documentation, 20 unused parameters, 3 context argument ordering, 2 builtin shadowing, 1 variable naming |
| unparam | 5 | Test helper parameters/results that are unused or always receive the same value |
| predeclared | 3 | `cap` shadows the builtin; two overlap with revive |
| exhaustive | 2 | Working-copy switches omit `OpCopy`; current local editing API does not expose copy |
| staticcheck | 1 | S1011: replace an append loop with one variadic append |
| errorlint | 1 | An underlying encoding error is formatted with `%v`, losing its chain |
| gosec | 1 | G115 uint64-to-int64 conversion in a test-only fake timestamp; no production vulnerability established |

Full diagnostic output: [2026-11-24-golangci-lint-findings.txt](../../testdata/lint/2026-11-24-golangci-lint-findings.txt).

## Prioritized findings

### P1: Move can silently lose unpublished local content

Location: [workingcopy.go:347](../../internal/workspace/workingcopy.go#L347).

`Move` ignores an error writing destination content, then deletes the source
and records the move as successful. If destination storage fails (for example,
disk full), the only cached copy of an unpublished edit can disappear. The
original pending put remains journaled but its bytes are unavailable, so it
cannot be staged on a later sync. The ignored source-delete error can also
leave local state inconsistent with the reported move.

This was reproduced in an isolated copy of the module using its existing
in-memory stack and a cache wrapper that fails writes to `b.txt`:

1. Checkout remote `a.txt`.
2. Write unpublished replacement bytes to local `a.txt`.
3. Move `a.txt` to `b.txt` while injecting a destination cache write failure.
4. Observe nil move error and both source and destination content absent.

The regression test fails with:

```text
Move reported success after destination cache write failed
source unpublished content lost: data="" err=agentfs: not found
destination also absent: agentfs: not found
```

The reproduction is saved as [2026-11-24-golangci-lint-repro.go.txt](../../testdata/lint/2026-11-24-golangci-lint-repro.go.txt).
It is not part of the normal test suite. Copy the module to a temporary
directory, place its contents in `lint_regression_test.go`, and run:

```sh
go test -run '^TestAuditMovePreservesUnpublishedContentOnCacheFailure$' -count=1 -v ./...
```

Recommended fix: propagate cache errors, preserve source bytes until the
destination is durable, and make the journal/cache transition recoverable
across partial failures. Add this scenario to the permanent regression suite.

### Error-handling improvements

- [service.go:259](../../internal/workspace/service.go#L259): `ReadFile` ignores `reader.Close`.
  Decide whether the blob adapter's close error can indicate failed stream
  completion; propagate it when relevant, or explicitly document best-effort
  cleanup. This finding alone does not establish corrupt reads.
- [workingcopy.go:376](../../internal/workspace/workingcopy.go#L376): `Status` discards
  pin-release errors. A failed release can hold retention resources until
  expiry. Define an observable best-effort cleanup policy or return the error.
- Several tests ignore staging, head lookup, and journal lookup errors. These
  should be checked so setup failures cannot feed zero values into assertions.
  The `fmt.Fprintf` findings target in-memory hash writers, which do not fail;
  these have lower priority and can use an explicitly documented discard.

### Go conventions and maintainability

- [workingcopy.go:503](../../internal/workspace/workingcopy.go#L503): simplify the loop
  to `result.Outcomes = append(result.Outcomes, conflicts...)`.
- [capabilities.go:72](../../internal/workspace/capabilities.go#L72),
  [capabilities.go:90](../../internal/workspace/capabilities.go#L90), and
  [service.go:155](../../internal/workspace/service.go#L155): rename `cap` to `capability`.
- [fixtures_test.go:112](../../internal/workspace/fixtures_test.go#L112): rename
  `space_trees` to `spaceTrees`.
- Add or correct exported API comments and mark intentionally unused parameters
  explicitly. The unsupported session implementation accounts for many unused
  parameter warnings; these do not establish missing session functionality.
- [workingcopy.go:532](../../internal/workspace/workingcopy.go#L532) and
  [workingcopy.go:710](../../internal/workspace/workingcopy.go#L710): make unsupported
  `OpCopy` handling explicit. The first switch already rejects unsupported
  values through `default`; the second silently skips them. Current public
  working-copy methods cannot create copy entries, so these findings are
  defensive completeness warnings rather than reproduced copy failures.
- [publisher.go:167](../../internal/workspace/publisher.go#L167): consider `%w` for the
  nested encoding error if it is intended to remain inspectable. The current
  wire struct contains supported JSON types, making this low priority.

## Reproduction of the audit

Official installation guidance: <https://golangci-lint.run/docs/welcome/install/local/>.
CLI reference: <https://golangci-lint.run/docs/configuration/cli/>.

From the module directory, using golangci-lint v2.15.0 on PATH:

```sh
golangci-lint run --no-config --timeout=3m --max-issues-per-linter=0 --max-same-issues=0 ./...
golangci-lint config verify --config=.golangci.yml
golangci-lint run --config=.golangci.yml ./...
```

The config validates successfully. Finding exit status is 1, as expected.
The existing module suite passes `go test -race ./...`; the isolated new
regression fails as expected and demonstrates a gap in that suite.
The sandbox required writable temporary `GOCACHE` and `GOLANGCI_LINT_CACHE`
directories; no `go mod tidy` or dependency changes were needed. The linter
binary was installed only under `/private/tmp`.

The initial audit did not change production code; the subsequent fixes are
summarized under Resolution. These checks do not establish database durability
or eliminate domain-level bugs outside the analyzers' scope.
