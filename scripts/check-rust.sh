#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
cargo clippy --workspace --all-targets --all-features -- -D warnings
# Test fixture construction and assertions may panic; production may not use
# unchecked unwraps or panic macros. This preserves the existing test policy.
cargo clippy --workspace --lib --bins --all-features -- \
  -D warnings -D clippy::unwrap_used -D clippy::panic -D clippy::panic_in_result_fn
