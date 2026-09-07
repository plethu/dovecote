# Maintainability review

This review covers the core, all three SQL adapters, their public and durable
boundaries, the migration fixture runner, and the commands that enforce them.
It follows the relation-lifecycle integration work; a successful integration
example alone was not treated as a repository-wide audit.

| Criterion | Reviewed or enforced boundary |
| --- | --- |
| Setup and commands | Pinned mise tools; familiar just recipes; one canonical gate shared with CI |
| Models and ownership | Private validated events; immutable payloads separate from mutable delivery state; one hydration owner per backend |
| Effects and errors | Caller-owned enqueue transactions; adapter-owned claim transactions; typed conflicts and lost claims; documented rollback obligations |
| Cohesion | Candidate selection and claim preparation have named owners; migration inputs and modes replace positional bundles |
| Ecosystem reuse | Serde, SQLx, time and maintained protocol parsers; narrow catalog validation retained for physical schema guarantees |
| Quality coverage | Inherited lints, strict public docs, dependency gates, fixture-runner checks and live conformance; deliberate differences listed below |
| Compatibility | Compatible patch; unchanged public behavior and migration bytes; archive and complete-history checks |

## Findings and changes

- The private migration runner accepted an unknown mode after a numeric batch
  limit and continued importing. Its fixed positional protocol now produces a
  named invocation and an exclusive execution mode. Raw legacy audit/export
  rows have named models instead of positional argument bundles. Unknown modes fail before
  opening a database; a bounded rollback is represented explicitly. Parser
  regression tests cover both cases. Top-level failures report fixed categories
  rather than formatting driver errors that can contain URLs or source data.
- Enqueue replay validation duplicated each backend's event hydration used for
  paging and claiming. These paths now use the same validating hydration owner.
  Original event bytes remain authoritative; no table or payload format changed.
- MySQL claim acquisition mixed candidate discovery, locking, validation,
  entropy, persistence and transaction completion in one large function.
  Candidate selection and preparation now have private owners. The transaction
  still owns all row locks, samples operation time after acquiring them, prepares
  the entire batch before writes, and retains the original commit/rollback path.
- SQLite claim retries repeated the claimant, tenant, lease and limit alongside
  database configuration and injected effects. A private claim request retains
  the operation's scope across retries. Existing argument-count suppressions
  are removed.
- Catalog expression normalization duplicated binary-length alias handling and
  used indexed byte traversal with three loosely related quote-state booleans.
  One alias normalizer and an explicit quote state now own that behavior.
- Public fallible APIs lacked discoverable error contracts. Their documentation
  now describes validation, conflicts, lost claims, database failures and the
  caller's rollback responsibility. Shared workspace lints enforce error docs,
  documentation formatting, unreachable-public-item and exception hygiene checks.
- The migration runner's blanket lint suppressions and unused SQLite fixture
  bridge methods are removed. Its formatting, Clippy, tests and dependency scan
  have a named check. Canonical checks explicitly report when its required
  sibling checkout is unavailable.
- Tool setup now pins the supported Rust baseline as well as the existing tools.
  CI explicitly selects each Rust matrix member so the local pin cannot silently
  replace the stable compiler lane. `just clippy`, filtered `just test`, and
  `just check-migration-runner` expose focused work through familiar commands.
- Repeated MySQL conformance runs tried to recreate an existing installation.
  Setup now installs only into an empty owned table footprint and performs the
  full schema check before reuse. Partial installations fail; retained history
  is not deleted. TOML discovery covers the same maintained files without
  recursively traversing build artifacts and sibling checkout links.
- The history harness used `cargo run`, which echoed its database URL in process
  diagnostics. It now builds separately and invokes the runner directly;
  injected failures retain the runner's fixed diagnostic categories.

## Contracts retained

All published migration SQL remains immutable. There is no replacement lifecycle
store, extra audit table, change to claim-token width, or new delivery guarantee.
Immutable events and mutable delivery rows retain their separate ownership.
Tenant binding, consumer deduplication, lease fencing and `LostClaim` remain the
application's observable contracts.

The public API changes in this pass are additive documentation and const/must-use
annotations; the internal refactors require no new major version or database
migration. The changes are staged as the compatible 0.2.2 patch release, after package
and release verification. No publication is implied by
local workspace checks.

The deprecated PostgreSQL `V1_TENANT_ACTIVATE_SQL` re-export retains its published
bytes. Its existing deprecation allowance is narrowed to that symbol and explains
why it remains. Callers use the replacement activation artifact for upgrades.

## Default differences reviewed

The stricter toolkit was run as an exploratory profile, separately from the
repository's acceptance gate. Its output was read by category and in substantive
source paths; counts of grep hits or warnings are not defect counts.

The repository does not claim that every pedantic, nursery and cargo lint is
enabled. In particular:

- Snapshot pagers deliberately retain their published non-`Send` contract and
  compile-fail documentation. Enabling `future_not_send` indiscriminately would
  reject that contract. A pager's transaction remains connection-bound across
  all pages; an ordinary cached page does not claim snapshot authority.
- The SQLx dependency graph includes multiple versions of transitive packages.
  Replacing or patching SQLx internals solely to satisfy `multiple_crate_versions`
  would create a maintained dependency fork. Cargo-deny checks actual advisories,
  licenses, sources and bans; duplicate versions are reviewed there.
- Large catalog contract tables and complete database scenario tests are reviewed
  for cohesion rather than split to satisfy a function-line threshold. They retain
  one visible schema contract or one complete behavior. No size baseline is raised.
- A few infallible methods assert invariants established by private validated
  fields, such as parsing retained validated JSON or formatting a validated time.
  These are inspected programmer invariants, not recovery paths for external
  input. Test assertions may panic. A separate library/binary lint lane preserves
  existing test-fixture acceptance while denying production unwrap and panic macros.

No production lint suppression was introduced to silence the exploratory profile. The safe core, PostgreSQL and MySQL crates forbid unsafe
code. SQLite retains its existing native-handle transaction-state query, with a
local safety argument and SQLx's locked handle guarding the FFI call.

## Reuse and ownership

Serde JSON owns JSON parsing and encoding, `fluent-uri` owns URI syntax, `mime`
owns media types, `time` owns timestamp representation, SQLx owns database I/O,
and `getrandom` supplies claim entropy. CloudEvents SDK and golden fixtures
provide independent projection evidence. Dovecote retains its strict immutable
projection and schema validation contracts rather than adopting an event SDK's
mutable builder as the durable authority.

Catalog validators compare live physical constraints and backend-specific catalog
renderings. SQLx migration receipts alone cannot establish those properties;
retaining this narrow validation is justified. The fixed test-runner protocol is
not a growing application CLI: its shell harness supplies positional values and
one mode, and the parser has no public release surface. A future interactive CLI
should use a maintained argument parser rather than extend this grammar.

## Verification

The reproducible acceptance owner is `mise exec -- just check`. It includes
formatting, structure rules, Clippy, strict rustdoc, dependency checks, tests,
SQLite migration and CDC reference fixtures, package checks and whitespace checks.
See [CONTRIBUTING.md](../CONTRIBUTING.md) for required live database flags and
[the support matrix](support-matrix.md) for exact backend release evidence.

The final Rust 1.94.0 and Rust 1.96.0 stable canonical gates passed with PostgreSQL and MySQL required
and `DOVECOTE_VERIFY_PUBLISHED_ADAPTERS=1`. All four 0.2.2 archives were verified;
the adapter archives resolved the actual registry core 0.2.1, not a local patch.
The source workspace separately exercised the staged 0.2.2 core.

Additional evidence:

- PostgreSQL 17.11: 44 reported passing conformance cases; the optional RLS
  case returned early because its role-creation configuration was absent.
- MySQL 8.4.11, Innovation 26.7.0 and MariaDB 11.8.6: 25 live conformance
  tests each, plus the separately enabled v1-to-v2 tenant activation test on
  each backend. Repeated runs reused and validated the existing fixture schema.
- SQLite: 37 conformance, 11 import and five finalization tests; linked-runtime
  migration smoke tests and the executable enqueue/retry/delivery example.
- `tests/complete-history-migration.sh sqlite` and
  `tests/run-complete-history-containers.sh`: complete history on SQLite,
  PostgreSQL 17.11, MySQL 8.4.11 and 26.7.0, and MariaDB's retained-data
  maintenance upgrade from 10.3.17 to 11.8.6. Rollback, post-commit crash,
  restart, immutable payloads, terminal delivery history and retry passed.
- Core `--no-default-features`, strict documentation and compile-fail pager
  examples, fixture-runner tests, ShellCheck, advisory/license/source checks,
  unused dependencies and unchanged historical migration bytes.

Fresh independent source review found no unresolved material issue
after corrections and re-review, including the fixture reuse and diagnostic
changes. This is a scoped source and runtime assessment, not a guarantee that
future maintenance will require no changes.

The optional 10,000-tenant stress tests, live RLS role/grant exercise, live CDC
connector and remote CI were not run. The CDC fixture is reference evidence.
Nothing was committed or published; package verification does not authorize a
release.
