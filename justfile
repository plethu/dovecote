set shell := ["bash", "-euo", "pipefail", "-c"]

fmt:
    cargo fmt --all
    taplo fmt

check:
    scripts/check-project-gates.sh

verify:
    just check

supply-chain:
    if ! command -v cargo-deny >/dev/null 2>&1; then echo "cargo-deny is unavailable; run 'mise install'" >&2; exit 2; fi
    cargo deny check advisories bans licenses sources
