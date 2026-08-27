#!/usr/bin/env bash
set -euo pipefail

if [[ "${DOVECOTE_GATES_MISE_REEXEC:-0}" != "1" ]] && {
  ! command -v ast-grep >/dev/null 2>&1 ||
  ! command -v taplo >/dev/null 2>&1 ||
  ! command -v typos >/dev/null 2>&1 ||
  ! command -v just >/dev/null 2>&1
}; then
  if command -v mise >/dev/null 2>&1; then
    export DOVECOTE_GATES_MISE_REEXEC=1
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

echo "== CDC reference fixture (not a live CDC release gate) =="
(
  cd "$repo_root"
  tests/debezium-config.sh
)

echo "== Cargo package archives =="
(
  cd "$repo_root"
  echo "-- verifying the runtime-free core archive --"
  cargo package --package dovecote --allow-dirty --locked
  echo "-- constructing workspace archives (adapters not locally verified) --"
  cargo package --workspace --allow-dirty --no-verify --locked
  cat <<'EOF'
Adapter archives were constructed with --no-verify because their normalized
manifests depend on the unpublished dovecote crate. Before publishing any
adapter, publish dovecote first, wait for it to be available from the registry,
then run `cargo package --package <adapter> --locked` without --no-verify for
each adapter. Only after those verification runs pass may the adapters be
published.
EOF
)

echo "== git diff check =="
(
  cd "$repo_root"
  git diff --check
)

echo "Dovecote project gates passed."
