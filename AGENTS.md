# agents.md

This document defines how agents—human and automated—operate within this codebase.  
It follows **Extreme Programming (XP)** principles (communication, simplicity, feedback, courage, respect) and adopts **Conventional Commits** for clear traceability.

## Code Style

- Prefer Elixir functions over Erlang's
- Prefer multiple clauses over long functions
- Low cyclometric complexity
- `{:error, reason}` over plain `:error` for error handling
- Do not redefine types. If `Dot` defines `Dot.t() :: {Replica.id(), counter()}`, do not redefine `dot` type.

## Roles

- **Feature Agent (developer or pair):**  
  Breaks stories into smallest testable increments. Practices TDD. Keeps work in progress minimal.

- **Review Agent:**  
  Verifies Definition of Done: runnable demo, passing tests, updated docs. Requests scope reduction if PR grows.

- **Maintainer Agent:**  
  Curates backlog, merges only green builds, enforces API stability, plans refactors.

- **CI Agent (automation):**  
  Executes linting, unit/property tests, coverage, security and license scans.

- **Release Agent (automation):**  
  Generates changelog, semantic version bump, tags, and artifact publishing after successful merge.

- **Docs Agent:**  
  Keeps guides, tutorials, and API references synchronized with implementation.

## Testing

- All tests reside under `test/{unit,property,fuzz,integration,conformance}/`.
- Each suite uses a corresponding module namespace: `EdgeCrdtTest.<Category>`.
- Common generators, models, and case templates live in `test/support/`.

| Category        | Purpose                                                                         |
|-----------------|---------------------------------------------------------------------------------|
| **unit**        | Fast, deterministic, pure logic (CRDT ops, codec, store primitives).            |
| **property**    | Short property-based tests (`StreamData`/`PropCheck`) verifying algebraic laws. |
| **fuzz**        | Long-running randomized or stateful tests, usually excluded from default CI.    |
| **integration** | Cross-module, networking, or persistence behavior.                              |
| **conformance** | Spec and contract validation for CRDT behaviours and sync protocols.            |

## Definition of Done

- **Tests:** Appropriate coverage (unit/property/fuzz/conformance/integration as relevant).
- **Documentation:** README or guide reflects current design and usage.
- **Observability:** Metrics/logging hooks instrumented where applicable.
- **Demo:** `mix demo` or equivalent executable verifies end-to-end behavior.
- **Debt:** All remaining TODOs are converted into tracked issues before merge.

## Conventional Commits

Format:  
`<type>(<scope>): <message>`

Common types:

- `feat:` user-visible feature  
- `fix:` bug fix  
- `refactor:` behavior-preserving change  
- `perf:` performance improvement  
- `test:` test-only change  
- `docs:` documentation change  
- `build:` CI/build system  
- `chore:` maintenance work  
- `revert:` revert of previous commit  

Add `BREAKING CHANGE:` footer when relevant.

Examples:

- `feat(sync): add digest-first handshake`
- `fix(store-sqlite): handle WAL rotation`
- `refactor(crdt): extract dvv context module`
- `docs(guide): expand replay walkthrough`

## Review Policy

- Prefer **small, frequent merges** (<300 lines diff).  
- Every change must be demonstrable (test or demo).  
- Reject complexity not required by a failing test or explicit story.  
- Keep trunk green — integration tests must always pass.
