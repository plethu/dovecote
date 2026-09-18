# Publishing a Dovecote release

## Evidence before publication

Run the canonical checks and the backend matrix defined in
`.github/workflows/check.yml` on the release revision, including the declared
minimum Rust version, stable Rust and complete-history fixtures. The
[support matrix](support-matrix.md) describes backend settings and migration
constraints. Keep run results with the release, rather than copying them into
these instructions.

Stable jobs require two repository variables containing reviewed, reachable
40-hex commit SHAs:

- `DOVECOTE_KEEPSAKE_BRIDGE_REF`, checked out from `plethu/keepsake`;
- `DOVECOTE_GATEKEEP_BRIDGE_REF`, checked out from `plethu/gatekeep`.

These identify the migration fixture's sibling sources. They are separate from
crates.io versions and from a decision to release either sibling project. The
harness validates historical migration hashes before touching its database;
a source checkout cannot replace the published-artifact compatibility proof.
Keep existing reviewed pins unless the fixture needs a different revision.

Record the independent review and verify the private reporting route as required
by [SECURITY.md](../SECURITY.md). Inspect the package archives and preserved
migration bytes. Local database, source-dependency and archive results remain
separate from CI and registry evidence. CDC remains unadvertised without its
separate live-connector release evidence.

## Publication order

After the final revision has green CI and the release evidence is complete:

1. Publish `dovecote` and wait until crates.io serves the new version.
2. Run `DOVECOTE_VERIFY_PUBLISHED_ADAPTERS=1 mise run check` with the documented
   backend configuration. This verifies each adapter archive normally against
   the registry core; the ordinary pre-publication gate only constructs those
   adapter archives with `--no-verify`.
3. Publish `dovecote-sqlx-postgres`, `dovecote-sqlx-mysql` and
   `dovecote-sqlx-sqlite`. They depend on the core and may be published in any
   order after their verified packages pass.
4. Confirm that all four versions resolve from the registry, and record the
   final revision, CI run and package results with the release tag and notes.

Keepsake and Gatekeep publication follows their own dependency order. A local
path dependency on Dovecote is not proof that their normalized packages resolve
from crates.io. Do not recreate a package or tag that already exists; inspect
its identity and publication outcome before recovering an interrupted release.

## Compatibility

Crate APIs, minimum Rust versions, SQL schemas and CloudEvents encodings are
separate contracts. Describe breaking changes and migration requirements in the
changelog. Published SQL artifacts are immutable; schema changes need forward
migrations and a recovery path. Wire changes need updated deterministic vectors
and compatibility tests. Backend and CDC claims require their respective live
fixtures.

`just check-public-api` compares public APIs with the published baselines in
`scripts/check-public-api.sh`; update them after publication. After 1.0,
raising the minimum Rust version requires at least a minor release and checks on
the previous and new toolchains. The declared versions live in Cargo manifests
and CI, not in a second status table.
