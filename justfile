set shell := ["bash", "-euo", "pipefail", "-c"]

fmt:
    cargo fmt --all
    taplo fmt

check:
    scripts/check-project-gates.sh

verify:
    just check
