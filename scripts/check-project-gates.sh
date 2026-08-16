#!/usr/bin/env bash
set -euo pipefail

if [[ "${CARRIER_GATES_MISE_REEXEC:-0}" != "1" ]] && {
  ! command -v ast-grep >/dev/null 2>&1 ||
  ! command -v taplo >/dev/null 2>&1 ||
  ! command -v typos >/dev/null 2>&1 ||
  ! command -v just >/dev/null 2>&1
}; then
  if command -v mise >/dev/null 2>&1; then
    export CARRIER_GATES_MISE_REEXEC=1
    exec mise exec -- "$0" "$@"
  fi
fi

repo_root="$(git rev-parse --show-toplevel)"

echo "== cargo fmt --all --check =="
(
  cd "$repo_root"
  cargo fmt --all --check
)

echo "== structural Rust checks =="
if command -v ast-grep >/dev/null 2>&1; then
  MISE_PROJECT_ROOT="$repo_root" "$repo_root/.config/mise/tasks/lint-structure"
elif command -v mise >/dev/null 2>&1; then
  (
    cd "$repo_root"
    MISE_PROJECT_ROOT="$repo_root" mise exec -- .config/mise/tasks/lint-structure
  )
else
  echo "ast-grep is unavailable; install the pinned tools with 'mise install'" >&2
  exit 2
fi

echo "== cargo clippy =="
(
  cd "$repo_root"
  cargo clippy --workspace --all-targets --all-features -- -D warnings
)

echo "== cargo test =="
(
  cd "$repo_root"
  cargo test --workspace --all-features
)

echo "== TOML and spelling checks =="
(
  cd "$repo_root"
  taplo fmt --check
  taplo lint
  typos
)

echo "== SQLite migration smoke test =="
(
  cd "$repo_root"
  tests/sqlite-migration.sh
)

echo "== git diff check =="
(
  cd "$repo_root"
  git diff --check
)

echo "Carrier project gates passed."
