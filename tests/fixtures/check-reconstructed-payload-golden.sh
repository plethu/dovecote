#!/usr/bin/env bash
set -euo pipefail

# Re-run the retired codecs against the exact four normalized inputs recorded
# by the fixture.  This deliberately uses detached source archives and a
# Dovecote 0.1.0 checkout from the reviewed bridge-era carrier commit; the
# current 3.0 worktrees do not contain these removed encoder APIs.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
keepsake_root="${KEEPSAKE_ROOT:-${repo_root}/../keepsake-rs}"
gatekeep_root="${GATEKEEP_ROOT:-${repo_root}/../gatekeep-rs}"
keepsake_commit="b5d1c1fdebb19164c0c569c75f3a2e21c1c667fc"
gatekeep_commit="d7450f2c02e2510da38c5e66e5e55954c3005bd6"
carrier_commit="de7ea8535fa5e7c76a75b22e9bc4fcf56e6e8a4a"

require_commit() {
    local repository="$1"
    local commit="$2"
    git -C "${repository}" cat-file -e "${commit}^{commit}"
}

require_commit "${keepsake_root}" "${keepsake_commit}"
require_commit "${gatekeep_root}" "${gatekeep_commit}"
require_commit "${repo_root}" "${carrier_commit}"

work_root="$(mktemp -d "${TMPDIR:-/tmp}/dovecote-retired-codecs.XXXXXX")"
cleanup() {
    rm -rf -- "${work_root}"
}
trap cleanup EXIT
mkdir -p "${work_root}/keepsake-rs" "${work_root}/gatekeep-rs" "${work_root}/carrier"
git -C "${keepsake_root}" archive "${keepsake_commit}" | tar -x -C "${work_root}/keepsake-rs"
git -C "${gatekeep_root}" archive "${gatekeep_commit}" | tar -x -C "${work_root}/gatekeep-rs"
git -C "${repo_root}" archive "${carrier_commit}" | tar -x -C "${work_root}/carrier"

rg -q 'encode_reconstructed_audit_v1' \
    "${work_root}/keepsake-rs/crates/keepsake-sqlx/src/repository/dovecote_bridge.rs"
rg -q 'encode_reconstructed_audit_v1' \
    "${work_root}/gatekeep-rs/crates/gatekeep-sqlx/src/audit/bridge.rs"

mkdir -p "${work_root}/keepsake-rs/crates/keepsake-sqlx/tests"
cat > "${work_root}/keepsake-rs/crates/keepsake-sqlx/tests/reconstructed_golden.rs" <<'EOF'
use chrono::{DateTime, Utc};
use keepsake_sqlx::{encode_reconstructed_audit_v1, LegacyAuditEventV1};
use std::{collections::BTreeMap, env, fs};
use uuid::Uuid;

#[test]
fn emit_keepsake_100_payload() {
    let mut context = BTreeMap::new();
    let payload = encode_reconstructed_audit_v1(LegacyAuditEventV1 {
        audit_id: 100,
        event_type: "apply".to_owned(),
        occurred_at: "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
        actor_kind: "service".to_owned(),
        actor_id: "writer-zero".to_owned(),
        keepsake_id: Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap(),
        subject_kind: "user".to_owned(),
        subject_id: "münchen".to_owned(),
        relation_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
        decision: serde_json::json!({"type":"applied","duplicate_prevented":false}),
        context_attributes: std::mem::take(&mut context),
    }).unwrap();
    fs::write(env::var_os("KEEPSAKE_PAYLOAD").unwrap(), payload).unwrap();
}
EOF

keepsake_payload="${work_root}/keepsake-100.json"
KEEPSAKE_PAYLOAD="${keepsake_payload}" \
    CARGO_TARGET_DIR="${work_root}/target" \
    cargo test --manifest-path "${work_root}/keepsake-rs/crates/keepsake-sqlx/Cargo.toml" \
        --no-default-features --features 'dovecote-sqlite,migrations' --test reconstructed_golden --offline -- \
        --exact emit_keepsake_100_payload
printf 'keepsake/100 payload sha256: '
sha256sum "${keepsake_payload}" | cut -d' ' -f1

mkdir -p "${work_root}/gatekeep-rs/crates/gatekeep-sqlx/tests"
cat > "${work_root}/gatekeep-rs/crates/gatekeep-sqlx/tests/reconstructed_golden.rs" <<'EOF'
use gatekeep_sqlx::encode_reconstructed_audit_v1;
use std::{env, fs};

#[test]
fn emit_gatekeep_100_200_500_payloads() {
    let values = [
        ("100", serde_json::json!({"effect":"permit","context":{"tenant":"東京","optional":[]}})),
        ("200", serde_json::json!({"effect":"permit","context":{}})),
        ("500", serde_json::json!({"effect":"permit","context":{"audit":"only-after-outbox"}})),
    ];
    let directory = env::var_os("GATEKEEP_PAYLOAD_DIR").unwrap();
    for (id, value) in values {
        fs::write(
            std::path::Path::new(&directory).join(format!("{id}.json")),
            encode_reconstructed_audit_v1(&value).unwrap(),
        ).unwrap();
    }
}
EOF

gatekeep_payload_dir="${work_root}/gatekeep-payloads"
mkdir -p "${gatekeep_payload_dir}"
GATEKEEP_PAYLOAD_DIR="${gatekeep_payload_dir}" \
    CARGO_TARGET_DIR="${work_root}/target" \
    cargo test --manifest-path "${work_root}/gatekeep-rs/crates/gatekeep-sqlx/Cargo.toml" \
        --no-default-features --features dovecote-sqlite --test reconstructed_golden --offline -- \
        --exact emit_gatekeep_100_200_500_payloads
for id in 100 200 500; do
    printf 'gatekeep/%s payload sha256: ' "${id}"
    sha256sum "${gatekeep_payload_dir}/${id}.json" | cut -d' ' -f1
done
