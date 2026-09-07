#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
if [[ ! -f tests/sibling-worktrees/carrier/crates/dovecote/Cargo.toml ]]; then
  echo "Migration runner dependencies are unavailable; follow CONTRIBUTING.md to prepare the fixture checkout." >&2
  exit 2
fi
manifest=tests/fixture-runner/Cargo.toml
cargo fmt --manifest-path "$manifest" -- --check
cargo clippy --manifest-path "$manifest" --all-targets -- -D warnings
cargo test --manifest-path "$manifest"
cargo machete --with-metadata tests/fixture-runner
