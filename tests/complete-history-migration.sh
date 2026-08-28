#!/usr/bin/env bash
set -euo pipefail

# Complete-history migration fixture harness.  It is intentionally separate
# from the normal unit-test gate: the sibling schemas are real release
# artifacts, and a missing or altered artifact must fail before a database is
# touched.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_root="${repo_root}/tests/fixtures"
keepsake_root="${KEEPSAKE_ROOT:-${repo_root}/tests/sibling-worktrees/keepsake}"
gatekeep_root="${GATEKEEP_ROOT:-${repo_root}/tests/sibling-worktrees/gatekeep}"
runner_manifest="${repo_root}/tests/fixture-runner/Cargo.toml"
fixture="${fixture_root}/complete-history-v1.json"

ensure_codec_source() {
    local project="$1"
    local path="$2"
    local default_source="$3"
    if [[ -f "${path}/crates/${project}-sqlx/Cargo.toml" ]]; then
        return
    fi
    if [[ -e "${path}" || -L "${path}" ]]; then
        echo "configured ${project} codec source is not a usable checkout: ${path}" >&2
        exit 1
    fi
    if [[ ! -f "${default_source}/crates/${project}-sqlx/Cargo.toml" ]]; then
        echo "missing ${project} bridge checkout; set ${project^^}_ROOT to a reviewed 1.x bridge source" >&2
        exit 1
    fi
    mkdir -p "$(dirname "${path}")"
    ln -s "${default_source}" "${path}"
}

# CI supplies reviewed bridge checkouts at the paths above.  For a local
# release-candidate proof, the sibling 3.0 checkouts are the reproducible
# fallback; their historical migration files are still checked against the
# vendored SHA-256 manifest below.
ensure_codec_source keepsake "${keepsake_root}" "${repo_root}/../keepsake-rs"
ensure_codec_source gatekeep "${gatekeep_root}" "${repo_root}/../gatekeep-rs"

# Gatekeep's release-candidate workspace names Keepsake through a sibling
# `../keepsake-rs` path. CI checks out both projects under the fixture's
# `sibling-worktrees` directory, so provide that exact ignored alias when the
# lexical Gatekeep checkout does not already have its expected sibling.
temporary_keepsake_alias=""
# Resolve this lexically: Cargo resolves Gatekeep's `../keepsake-rs` relative
# to the manifest path supplied to it, even when that manifest is reached via
# a symlink.  Using pwd -P here would create the alias beside the real source
# checkout and leave the lexical fixture layout broken.
keepsake_expected_by_gatekeep="$(dirname "${gatekeep_root}")/keepsake-rs"
if [[ ! -f "${keepsake_expected_by_gatekeep}/crates/keepsake/Cargo.toml" ]]; then
    if [[ -e "${keepsake_expected_by_gatekeep}" || -L "${keepsake_expected_by_gatekeep}" ]]; then
        echo "Gatekeep's expected Keepsake sibling is not a usable checkout: ${keepsake_expected_by_gatekeep}" >&2
        exit 1
    fi
    ln -s "${keepsake_root}" "${keepsake_expected_by_gatekeep}"
    temporary_keepsake_alias="${keepsake_expected_by_gatekeep}"
fi

cleanup_fixture_alias() {
    if [[ -n "${temporary_keepsake_alias}" && -L "${temporary_keepsake_alias}" ]]; then
        rm -f -- "${temporary_keepsake_alias}"
    fi
}
trap cleanup_fixture_alias EXIT

# The bridge manifests resolve their local Dovecote dependency through a
# sibling named `carrier`. Keep that path valid both for local worktrees and
# for CI's fixture-local checkouts without adding the fixture runner to the
# normal Dovecote workspace.
codec_worktree_root="${repo_root}/tests/sibling-worktrees"
if [[ ! -f "${codec_worktree_root}/carrier/crates/dovecote/Cargo.toml" ]]; then
    if [[ -e "${codec_worktree_root}/carrier" || -L "${codec_worktree_root}/carrier" ]]; then
        echo "fixture sibling carrier path is not a usable checkout" >&2
        exit 1
    fi
    ln -s "${repo_root}" "${codec_worktree_root}/carrier"
fi

usage() {
    cat <<'EOF'
usage: tests/complete-history-migration.sh [sqlite|postgres|mysql|mysql-innovation|mariadb]

SQLite runs locally with SQLx's linked SQLite runtime.  Other backends require
DATABASE_URL and the matching client.  Supported release evidence is:
PostgreSQL 17.11, MySQL LTS 8.4.11, MySQL Innovation 26.7.0, and MariaDB
11.8.6.  Set DOVECOTE_ALLOW_UNVERIFIED_VERSION=1 only for a local exploratory
run; CI and release evidence must use the exact advertised server version.

The MariaDB maintenance-window wrapper invokes this harness once with
DOVECOTE_MARIADB_SOURCE_ONLY=1 on MariaDB 10.3.17 and once with
DOVECOTE_MARIADB_SOURCE_READY=1 after the same database is upgraded to 11.8.
Invoke tests/run-mariadb-history-upgrade.sh for MariaDB; a direct MariaDB run
is rejected so it cannot replay a historical migration on the 11.8 target.
The source-ready invocation does not apply any historical migration.

For MySQL-family clients, set MYSQL_ARGS to the normal mysql CLI connection
arguments (for example: --host=127.0.0.1 --user=test --password=test test).
DATABASE_URL is passed separately to the SQLx runner.

For local release-candidate checkouts, keep the defaults (../keepsake-rs and
../gatekeep-rs) or set KEEPSAKE_ROOT and GATEKEEP_ROOT explicitly. CI retains
its reviewed 40-hex bridge-SHA requirement and is not satisfied by these
local fallbacks.
EOF
}

backend="${1:-sqlite}"
if [[ "${backend}" == "-h" || "${backend}" == "--help" ]]; then
    usage
    exit 0
fi
case "${backend}" in
    sqlite|postgres|mysql|mysql-innovation|mariadb) ;;
    *) usage >&2; exit 2 ;;
esac

mariadb_source_only="${DOVECOTE_MARIADB_SOURCE_ONLY:-0}"
mariadb_source_ready="${DOVECOTE_MARIADB_SOURCE_READY:-0}"
if [[ ("${mariadb_source_only}" == 1 || "${mariadb_source_ready}" == 1) && "${backend}" != mariadb ]]; then
    echo "MariaDB source modes are supported only with the mariadb backend" >&2
    exit 2
fi
if [[ "${mariadb_source_only}" == 1 && "${mariadb_source_ready}" == 1 ]]; then
    echo "MariaDB source-only and source-ready modes are mutually exclusive" >&2
    exit 2
fi
if [[ "${backend}" == mariadb && "${mariadb_source_only}" != 1 && "${mariadb_source_ready}" != 1 ]]; then
    echo "MariaDB complete-history runs must use tests/run-mariadb-history-upgrade.sh; direct migration replay is unsupported" >&2
    exit 2
fi

require_file() {
    if [[ ! -f "$1" ]]; then
        echo "missing migration fixture artifact: $1" >&2
        exit 1
    fi
}

migration_file() {
    local project="$1"
    local backend_name="$2"
    local filename="$3"
    local sibling
    case "${project}" in
        keepsake) sibling="${keepsake_root}/crates/keepsake-sqlx/migrations/${backend_name}/${filename}" ;;
        gatekeep) sibling="${gatekeep_root}/crates/gatekeep-sqlx/migrations/${backend_name}/${filename}" ;;
        *) echo "invalid migration project: ${project}" >&2; exit 1 ;;
    esac
    if [[ -f "${sibling}" ]]; then
        printf '%s\n' "${sibling}"
        return
    fi
    local vendored="${fixture_root}/published/${project}/${backend_name}/${filename}"
    require_file "${vendored}"
    printf '%s\n' "${vendored}"
}

mysql_cli() {
    # The legacy fixtures intentionally contain UTF-8 content. Set the client
    # character set explicitly so JSON text is not converted through a
    # connection default before it reaches the real MySQL JSON column.
    if [[ -n "${MYSQL_CLI_CONTAINER:-}" ]]; then
        "${CONTAINER_RUNTIME:-docker}" exec -i "${MYSQL_CLI_CONTAINER}" \
            "${MYSQL_CLI_PROGRAM:-mysql}" --user=root --password=test fixture \
            --default-character-set=utf8mb4 "$@"
    else
        # shellcheck disable=SC2086
        command "${MYSQL_CLI_PROGRAM:-mysql}" ${MYSQL_ARGS:-} \
            --default-character-set=utf8mb4 "$@"
    fi
}

verify_historical_bytes() {
    local digest relative path actual vendored vendor_actual
    while read -r digest relative; do
        [[ -z "${digest}" || "${digest}" == \#* ]] && continue
        case "${relative}" in
            keepsake/*) path="${keepsake_root}/crates/keepsake-sqlx/migrations/${relative#keepsake/}" ;;
            gatekeep/*) path="${gatekeep_root}/crates/gatekeep-sqlx/migrations/${relative#gatekeep/}" ;;
            *) echo "invalid historical migration manifest path: ${relative}" >&2; exit 1 ;;
        esac
        vendored="${fixture_root}/published/${relative}"
        require_file "${vendored}"
        vendor_actual="$(sha256sum "${vendored}" | awk '{print $1}')"
        if [[ "${vendor_actual}" != "${digest}" ]]; then
            echo "vendored historical migration changed: ${vendored}" >&2
            echo "expected ${digest}, got ${vendor_actual}" >&2
            exit 1
        fi
        if [[ -f "${path}" ]]; then
            actual="$(sha256sum "${path}" | awk '{print $1}')"
            if [[ "${actual}" != "${digest}" ]]; then
                echo "historical migration changed: ${path}" >&2
                echo "expected ${digest}, got ${actual}" >&2
                exit 1
            fi
        fi
    done < "${fixture_root}/historical-migrations.sha256"
}

verify_historical_bytes
require_file "${fixture}"

run_fixture_runner() {
    local url="$1"
    local keepsake_audit_high_water="$2"
    local keepsake_outbox_high_water="$3"
    local gatekeep_audit_high_water="$4"
    local gatekeep_outbox_high_water="$5"
    local stop_after="${6:-}"
    local action="${7:-}"
    local target_dir="${CARGO_TARGET_DIR:-${repo_root}/tests/fixture-runner/target}"
    local ledger="${DOVECOTE_FIXTURE_LEDGER:-${repo_root}/tests/fixture-ledger.jsonl}"
    if [[ -n "${stop_after}" ]]; then
        if [[ -n "${action}" ]]; then
            DOVECOTE_FIXTURE_LEDGER="${ledger}" CARGO_TARGET_DIR="${target_dir}" cargo run --manifest-path "${runner_manifest}" \
                --locked --offline -- "${backend}" "${url}" "${fixture}" "${keepsake_audit_high_water}" "${keepsake_outbox_high_water}" "${gatekeep_audit_high_water}" "${gatekeep_outbox_high_water}" "${stop_after}" "${action}"
        else
            DOVECOTE_FIXTURE_LEDGER="${ledger}" CARGO_TARGET_DIR="${target_dir}" cargo run --manifest-path "${runner_manifest}" \
                --locked --offline -- "${backend}" "${url}" "${fixture}" "${keepsake_audit_high_water}" "${keepsake_outbox_high_water}" "${gatekeep_audit_high_water}" "${gatekeep_outbox_high_water}" "${stop_after}"
        fi
    else
        if [[ "${action}" == "verify" ]]; then
            DOVECOTE_FIXTURE_LEDGER="${ledger}" CARGO_TARGET_DIR="${target_dir}" cargo run --manifest-path "${runner_manifest}" \
                --locked --offline -- "${backend}" "${url}" "${fixture}" "${keepsake_audit_high_water}" "${keepsake_outbox_high_water}" "${gatekeep_audit_high_water}" "${gatekeep_outbox_high_water}" verify
        else
            DOVECOTE_FIXTURE_LEDGER="${ledger}" CARGO_TARGET_DIR="${target_dir}" cargo run --manifest-path "${runner_manifest}" \
                --locked --offline -- "${backend}" "${url}" "${fixture}" "${keepsake_audit_high_water}" "${keepsake_outbox_high_water}" "${gatekeep_audit_high_water}" "${gatekeep_outbox_high_water}"
        fi
    fi
}

sqlite_assertions() {
    local db="$1"
    local state_counts
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM dovecote_events;')" == 16 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM dovecote_deliveries;')" == 16 ]]
    state_counts="$(sqlite3 "${db}" "SELECT state || ':' || count(*) FROM dovecote_deliveries GROUP BY state ORDER BY state;")"
    [[ "${state_counts}" == $'delivered:2\npending:14' ]]
    [[ "$(sqlite3 "${db}" "SELECT count(*) FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id=d.event_row_id WHERE d.state='delivered' AND d.delivered_at IS NULL;")" == 0 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM keepsake_audit_events;')" == 6 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM keepsake_audit_outbox;')" == 5 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM gatekeep_audit_decisions;')" == 11 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM gatekeep_audit_outbox;')" == 8 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM gatekeep_audit_consulted_facts;')" == 3 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM gatekeep_audit_obligations;')" == 1 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM gatekeep_audit_request_subjects;')" == 2 ]]
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM gatekeep_audit_reason_params;')" == 2 ]]
    [[ "$(sqlite3 "${db}" "SELECT count(*) FROM keepsake_audit_outbox WHERE claimed_by IS NOT NULL AND claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now');")" == 0 ]]
    [[ "$(sqlite3 "${db}" "SELECT count(*) FROM gatekeep_audit_outbox WHERE claimed_by IS NOT NULL AND claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now');")" == 0 ]]
    [[ "$(sqlite3 "${db}" "SELECT count(*) FROM dovecote_events WHERE stream='keepsake-audit';")" == 6 ]]
    [[ "$(sqlite3 "${db}" "SELECT count(*) FROM dovecote_events WHERE stream='gatekeep-audit';")" == 10 ]]
    [[ "$(sqlite3 "${db}" "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-1';")" == 1 ]]
    [[ "$(sqlite3 "${db}" "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-207';")" == 0 ]]
    echo "complete-history fixture passes for SQLite linked runtime"
}

assert_independent_progress() {
    local progress_path="$1"
    local last
    last="$(tail -n 1 "${progress_path}")"
    [[ "${last}" == *'"keepsake_audit_cursor":100'* ]]
    [[ "${last}" == *'"keepsake_outbox_cursor":106'* ]]
    # The outbox-backed decision with ID 1000 must not move the audit-only
    # cursor past the later audit-only decision 500.
    [[ "${last}" == *'"gatekeep_audit_cursor":500'* ]]
    [[ "${last}" == *'"gatekeep_outbox_cursor":206'* ]]
    [[ "${last}" == *'"gatekeep_audit_high_water":1000'* ]]
    [[ "${last}" == *'"gatekeep_outbox_high_water":206'* ]]
}

assert_sqlite_writer_fence() {
    local db="$1"
    sqlite3 "${db}" <<'SQL'
CREATE TRIGGER fixture_keepsake_legacy_writer_fence
BEFORE INSERT ON keepsake_audit_outbox
BEGIN
  SELECT RAISE(ABORT, 'legacy writer fenced');
END;
CREATE TRIGGER fixture_gatekeep_legacy_writer_fence
BEFORE INSERT ON gatekeep_audit_outbox
BEGIN
  SELECT RAISE(ABORT, 'legacy writer fenced');
END;
SQL
    if sqlite3 "${db}" "INSERT INTO keepsake_audit_outbox (id, audit_event_id, event_type, payload, created_at) VALUES (107, 101, 'keepsake.audit_event_recorded', '{}', '2026-01-01T00:00:07.000000Z');" >/dev/null 2>&1; then
        echo "legacy Keepsake writer bypassed the fence" >&2
        return 1
    fi
    if sqlite3 "${db}" "INSERT INTO gatekeep_audit_outbox (id, decision_id, event_type, payload, created_at) VALUES (207, 201, 'gatekeep.decision_audit_recorded', '{}', '2026-01-01T00:01:07.000000Z');" >/dev/null 2>&1; then
        echo "legacy Gatekeep writer bypassed the fence" >&2
        return 1
    fi
}

run_sqlite() {
    db="$(mktemp "${TMPDIR:-/tmp}/dovecote-history.XXXXXX.db")"
    trap 'rm -f -- "${db}" "${db}.ledger.jsonl" "${db}.ledger.jsonl.progress"; cleanup_fixture_alias' EXIT
    DOVECOTE_FIXTURE_LEDGER="${db}.ledger.jsonl"

    # Install the actual published schemas in order.  None of these files is
    # copied into the fixture, so a sibling schema change fails the hash gate.
    for migration in \
        0001_init.sql 0002_lifecycle_invariants.sql 0003_fulfillment_expiry_index.sql \
        0004_fulfillment_checklist.sql 0005_audit_outbox.sql; do
        sqlite3 "${db}" < "$(migration_file keepsake sqlite "${migration}")"
    done
    sqlite3 "${db}" < "$(migration_file gatekeep sqlite 0001_audit.sql)"
    sqlite3 "${db}" < "${fixture_root}/seed-sqlite-initial-v1.sql"
    sqlite3 "${db}" < "${repo_root}/crates/dovecote-sqlx-sqlite/migrations/0002_dovecote_tenant_baseline.sql"

    # Active claims are observed before cutover and explicitly fenced.  The
    # importer only receives portable Pending/Delivered states; no old token
    # or lease is ever copied into Dovecote.
    keepsake_claim="$(sqlite3 "${db}" "SELECT claimed_by || '|' || claimed_until FROM keepsake_audit_outbox WHERE id = 102 AND claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now');")"
    gatekeep_claim="$(sqlite3 "${db}" "SELECT claimed_by || '|' || claimed_until FROM gatekeep_audit_outbox WHERE id = 202 AND claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now');")"
    [[ "${keepsake_claim}" == "legacy-worker|2037-01-01T00:00:00.000000Z" ]]
    [[ "${gatekeep_claim}" == "legacy-worker|2037-01-01T00:00:00.000000Z" ]]
    keepsake_owner="${keepsake_claim%%|*}"
    keepsake_lease="${keepsake_claim#*|}"
    gatekeep_owner="${gatekeep_claim%%|*}"
    gatekeep_lease="${gatekeep_claim#*|}"
    wrong_fence="$(sqlite3 "${db}" "UPDATE keepsake_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 102 AND claimed_by = 'wrong-owner' AND claimed_until = '${keepsake_lease}'; SELECT changes();")"
    [[ "${wrong_fence}" == 0 ]]
    fence_result="$(sqlite3 "${db}" "UPDATE keepsake_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 102 AND claimed_by = '${keepsake_owner}' AND claimed_until = '${keepsake_lease}' AND claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now'); SELECT changes();")"
    [[ "${fence_result}" == 1 ]]
    stale_ack="$(sqlite3 "${db}" "UPDATE keepsake_audit_outbox SET delivered_at = '2037-01-01T00:00:00.000000Z' WHERE id = 102 AND claimed_by = '${keepsake_owner}' AND claimed_until = '${keepsake_lease}' AND delivered_at IS NULL; SELECT changes();")"
    [[ "${stale_ack}" == 0 ]]
    wrong_fence="$(sqlite3 "${db}" "UPDATE gatekeep_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 202 AND claimed_by = 'wrong-owner' AND claimed_until = '${gatekeep_lease}'; SELECT changes();")"
    [[ "${wrong_fence}" == 0 ]]
    fence_result="$(sqlite3 "${db}" "UPDATE gatekeep_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 202 AND claimed_by = '${gatekeep_owner}' AND claimed_until = '${gatekeep_lease}' AND claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now'); SELECT changes();")"
    [[ "${fence_result}" == 1 ]]
    stale_ack="$(sqlite3 "${db}" "UPDATE gatekeep_audit_outbox SET delivered_at = '2037-01-01T00:00:00.000000Z' WHERE id = 202 AND claimed_by = '${gatekeep_owner}' AND claimed_until = '${gatekeep_lease}' AND delivered_at IS NULL; SELECT changes();")"
    [[ "${stale_ack}" == 0 ]]

    # A deliberately rolled-back batch leaves neither event nor provenance
    # ledger row. The first committed checkpoint imports decision 1000 through
    # outbox row 1. It must advance only Gatekeep's outbox cursor: advancing
    # the independent decision cursor to 1000 would skip decision-only row 500.
    run_fixture_runner "sqlite://${db}" 104 104 1000 104 rollback
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM dovecote_events;')" == 0 ]]
    run_fixture_runner "sqlite://${db}" 104 104 1000 104 1
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM dovecote_events;')" == 1 ]]
    [[ "$(sqlite3 "${db}" "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-1';")" == 1 ]]
    checkpoint="$(tail -n 1 "${db}.ledger.jsonl.progress")"
    [[ "${checkpoint}" == *'"gatekeep_audit_cursor":0'* ]]
    [[ "${checkpoint}" == *'"gatekeep_outbox_cursor":1'* ]]
    if run_fixture_runner "sqlite://${db}" 104 104 1000 104 2 crash; then
        echo "fixture runner did not crash after its committed batch" >&2
        return 1
    fi
    [[ "$(sqlite3 "${db}" 'SELECT count(*) FROM dovecote_events;')" == 3 ]]
    run_fixture_runner "sqlite://${db}" 104 104 1000 104
    sqlite3 "${db}" < "${fixture_root}/seed-sqlite-late-v1.sql"
    # Fence legacy writers before taking the final source high-water snapshot.
    assert_sqlite_writer_fence "${db}"
    run_fixture_runner "sqlite://${db}" 206 206 1000 206 verify
    assert_independent_progress "${db}.ledger.jsonl.progress"
    sqlite_assertions "${db}"
}

sql_apply_postgres() {
    local url="$1"
    local migration
    for migration in \
        0001_init.sql 0002_lifecycle_invariants.sql 0003_schema_metadata.sql \
        0004_fulfillment_expiry_index.sql 0005_fulfillment_checklist.sql 0006_audit_outbox.sql; do
        psql "${url}" --set ON_ERROR_STOP=1 --file "$(migration_file keepsake postgres "${migration}")" >/dev/null
    done
    psql "${url}" --set ON_ERROR_STOP=1 --file "$(migration_file gatekeep postgres 0001_audit.sql)" >/dev/null
    psql "${url}" --set ON_ERROR_STOP=1 --file "${fixture_root}/seed-postgres-v1.sql" >/dev/null
    psql "${url}" --set ON_ERROR_STOP=1 --file "${repo_root}/crates/dovecote-sqlx-postgres/migrations/0002_dovecote_tenant_baseline.sql" >/dev/null
    keepsake_claim="$(psql "${url}" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "SELECT claimed_by || '|' || to_char(claimed_until AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM keepsake_audit_outbox WHERE id = 102 AND claimed_until > clock_timestamp();")"
    gatekeep_claim="$(psql "${url}" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "SELECT claimed_by || '|' || to_char(claimed_until AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM gatekeep_audit_outbox WHERE id = 202 AND claimed_until > clock_timestamp();")"
    [[ "${keepsake_claim}" == "legacy-worker|2037-01-01T00:00:00.000000Z" ]]
    [[ "${gatekeep_claim}" == "legacy-worker|2037-01-01T00:00:00.000000Z" ]]
    keepsake_owner="${keepsake_claim%%|*}"
    keepsake_lease="${keepsake_claim#*|}"
    gatekeep_owner="${gatekeep_claim%%|*}"
    gatekeep_lease="${gatekeep_claim#*|}"
    wrong_fence="$(psql "${url}" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "WITH changed AS (UPDATE keepsake_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 102 AND claimed_by = 'wrong-owner' AND claimed_until = '${keepsake_lease}'::timestamptz RETURNING id) SELECT count(*) FROM changed;")"
    [[ "${wrong_fence}" == 0 ]]
    fence_result="$(psql "${url}" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "WITH changed AS (UPDATE keepsake_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 102 AND claimed_by = '${keepsake_owner}' AND claimed_until = '${keepsake_lease}'::timestamptz AND claimed_until > clock_timestamp() RETURNING id) SELECT count(*) FROM changed;")"
    [[ "${fence_result}" == 1 ]]
    stale_ack="$(psql "${url}" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "WITH changed AS (UPDATE keepsake_audit_outbox SET delivered_at = '2037-01-01T00:00:00Z'::timestamptz WHERE id = 102 AND claimed_by = '${keepsake_owner}' AND claimed_until = '${keepsake_lease}'::timestamptz AND delivered_at IS NULL RETURNING id) SELECT count(*) FROM changed;")"
    [[ "${stale_ack}" == 0 ]]
    wrong_fence="$(psql "${url}" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "WITH changed AS (UPDATE gatekeep_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 202 AND claimed_by = 'wrong-owner' AND claimed_until = '${gatekeep_lease}'::timestamptz RETURNING id) SELECT count(*) FROM changed;")"
    [[ "${wrong_fence}" == 0 ]]
    fence_result="$(psql "${url}" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "WITH changed AS (UPDATE gatekeep_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 202 AND claimed_by = '${gatekeep_owner}' AND claimed_until = '${gatekeep_lease}'::timestamptz AND claimed_until > clock_timestamp() RETURNING id) SELECT count(*) FROM changed;")"
    [[ "${fence_result}" == 1 ]]
    stale_ack="$(psql "${url}" --tuples-only --no-align --set ON_ERROR_STOP=1 --command "WITH changed AS (UPDATE gatekeep_audit_outbox SET delivered_at = '2037-01-01T00:00:00Z'::timestamptz WHERE id = 202 AND claimed_by = '${gatekeep_owner}' AND claimed_until = '${gatekeep_lease}'::timestamptz AND delivered_at IS NULL RETURNING id) SELECT count(*) FROM changed;")"
    [[ "${stale_ack}" == 0 ]]
}

sql_apply_mysql() {
    local migration
    # MYSQL_ARGS is deliberately shell-word-split so callers can use the
    # native client's normal connection flags without a second abstraction.
    # The Dovecote migration is applied by the adapter's SQLx migration gate
    # in CI because its trigger bodies require SQLx's statement splitter.
    # Legacy artifacts and fixture rows are still applied directly here.
    for migration in \
        0001_init.sql 0002_lifecycle_invariants.sql 0003_fulfillment_expiry_index.sql \
        0004_fulfillment_checklist.sql 0005_audit_outbox.sql; do
        # shellcheck disable=SC2086
        mysql_cli < "$(migration_file keepsake mysql "${migration}")"
    done
    # shellcheck disable=SC2086
    mysql_cli < "$(migration_file gatekeep mysql 0001_audit.sql)"
    # shellcheck disable=SC2086
    mysql_cli < "${fixture_root}/seed-mysql-v1.sql"
}

assert_postgres_writer_fence() {
    local url="$1"
    psql "${url}" --set ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE FUNCTION fixture_legacy_writer_fence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'legacy writer fenced';
END;
$$;
CREATE TRIGGER fixture_keepsake_legacy_writer_fence
BEFORE INSERT ON keepsake_audit_outbox
FOR EACH ROW EXECUTE FUNCTION fixture_legacy_writer_fence();
CREATE TRIGGER fixture_gatekeep_legacy_writer_fence
BEFORE INSERT ON gatekeep_audit_outbox
FOR EACH ROW EXECUTE FUNCTION fixture_legacy_writer_fence();
SQL
    if psql "${url}" --set ON_ERROR_STOP=1 >/dev/null 2>&1 --command "INSERT INTO keepsake_audit_outbox (audit_event_id, event_type, payload) VALUES (101, 'keepsake.audit_event_recorded', '{}'::jsonb);"; then
        echo "legacy Keepsake writer bypassed the fence" >&2
        return 1
    fi
    if psql "${url}" --set ON_ERROR_STOP=1 >/dev/null 2>&1 --command "INSERT INTO gatekeep_audit_outbox (decision_id, event_type, payload) VALUES (201, 'gatekeep.decision_audit_recorded', '{}'::jsonb);"; then
        echo "legacy Gatekeep writer bypassed the fence" >&2
        return 1
    fi
}

assert_mysql_writer_fence() {
    # shellcheck disable=SC2086
    mysql_cli >/dev/null <<'SQL'
DELIMITER //
CREATE TRIGGER fixture_keepsake_legacy_writer_fence
BEFORE INSERT ON keepsake_audit_outbox
FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'legacy writer fenced';
END//
CREATE TRIGGER fixture_gatekeep_legacy_writer_fence
BEFORE INSERT ON gatekeep_audit_outbox
FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'legacy writer fenced';
END//
DELIMITER ;
SQL
    # shellcheck disable=SC2086
    if mysql_cli >/dev/null 2>&1 --execute "INSERT INTO keepsake_audit_outbox (audit_event_id, event_type, payload) VALUES (101, 'keepsake.audit_event_recorded', '{}');"; then
        echo "legacy Keepsake writer bypassed the fence" >&2
        return 1
    fi
    # shellcheck disable=SC2086
    if mysql_cli >/dev/null 2>&1 --execute "INSERT INTO gatekeep_audit_outbox (decision_id, event_type, payload) VALUES (201, 'gatekeep.decision_audit_recorded', '{}');"; then
        echo "legacy Gatekeep writer bypassed the fence" >&2
        return 1
    fi
}

if [[ "${backend}" == "sqlite" ]]; then
    run_sqlite
    exit 0
fi

if [[ "${mariadb_source_only}" != 1 ]]; then
    : "${DATABASE_URL:?DATABASE_URL is required for a live backend fixture run}"
fi
case "${backend}" in
    postgres)
        actual_version="$(psql "${DATABASE_URL}" --tuples-only --no-align --command 'SHOW server_version;')"
        expected_version="17.11"
        ;;
    mysql|mysql-innovation|mariadb)
        # shellcheck disable=SC2086
        actual_version="$(mysql_cli --batch --skip-column-names --execute 'SELECT VERSION();')"
        case "${backend}" in
            mysql) expected_version="8.4.11" ;;
            mysql-innovation) expected_version="26.7.0" ;;
            mariadb)
                if [[ "${mariadb_source_only}" == 1 ]]; then
                    expected_version="${DOVECOTE_MARIADB_SOURCE_VERSION:-10.3.17}"
                else
                    expected_version="${DOVECOTE_MARIADB_TARGET_VERSION:-11.8.6}"
                fi
                ;;
        esac
        ;;
esac
if [[ "${actual_version}" != *"${expected_version}"* && "${DOVECOTE_ALLOW_UNVERIFIED_VERSION:-0}" != 1 ]]; then
    echo "fixture requires ${expected_version}; server reported ${actual_version@Q}" >&2
    exit 1
fi

if [[ "${backend}" == "postgres" ]]; then
    sql_apply_postgres "${DATABASE_URL}"
else
    if [[ "${mariadb_source_ready}" == 1 ]]; then
        echo "using the existing MariaDB source schema; no historical migration is replayed"
    else
        sql_apply_mysql
    fi
    if [[ "${mariadb_source_only}" == 1 ]]; then
        echo "MariaDB ${actual_version} source schema prepared for a maintenance-window upgrade"
        exit 0
    fi
    # The same fence is required on MySQL and MariaDB. Dovecote's own schema
    # is installed by the SQLx runner, whose splitter handles trigger bodies.
    # shellcheck disable=SC2086
    keepsake_claim="$(mysql_cli --batch --skip-column-names --execute "SELECT CONCAT(claimed_by, '|', DATE_FORMAT(claimed_until, '%Y-%m-%dT%H:%i:%s.%fZ')) FROM keepsake_audit_outbox WHERE id = 102 AND claimed_until > UTC_TIMESTAMP(6);")"
    gatekeep_claim="$(mysql_cli --batch --skip-column-names --execute "SELECT CONCAT(claimed_by, '|', DATE_FORMAT(claimed_until, '%Y-%m-%dT%H:%i:%s.%fZ')) FROM gatekeep_audit_outbox WHERE id = 202 AND claimed_until > UTC_TIMESTAMP(6);")"
    [[ "${keepsake_claim}" == "legacy-worker|2037-01-01T00:00:00.000000Z" ]]
    [[ "${gatekeep_claim}" == "legacy-worker|2037-01-01T00:00:00.000000Z" ]]
    keepsake_owner="${keepsake_claim%%|*}"
    keepsake_lease="${keepsake_claim#*|}"
    gatekeep_owner="${gatekeep_claim%%|*}"
    gatekeep_lease="${gatekeep_claim#*|}"
    # shellcheck disable=SC2086
    wrong_fence="$(mysql_cli --batch --skip-column-names --execute "UPDATE keepsake_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 102 AND claimed_by = 'wrong-owner' AND claimed_until = STR_TO_DATE('${keepsake_lease}', '%Y-%m-%dT%H:%i:%s.%fZ'); SELECT ROW_COUNT();")"
    [[ "${wrong_fence}" == 0 ]]
    # shellcheck disable=SC2086
    fence_result="$(mysql_cli --batch --skip-column-names --execute "UPDATE keepsake_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 102 AND claimed_by = '${keepsake_owner}' AND claimed_until = STR_TO_DATE('${keepsake_lease}', '%Y-%m-%dT%H:%i:%s.%fZ') AND claimed_until > UTC_TIMESTAMP(6); SELECT ROW_COUNT();")"
    [[ "${fence_result}" == 1 ]]
    # shellcheck disable=SC2086
    stale_ack="$(mysql_cli --batch --skip-column-names --execute "UPDATE keepsake_audit_outbox SET delivered_at = '2037-01-01 00:00:00.000000' WHERE id = 102 AND claimed_by = '${keepsake_owner}' AND claimed_until = STR_TO_DATE('${keepsake_lease}', '%Y-%m-%dT%H:%i:%s.%fZ') AND delivered_at IS NULL; SELECT ROW_COUNT();")"
    [[ "${stale_ack}" == 0 ]]
    # shellcheck disable=SC2086
    wrong_fence="$(mysql_cli --batch --skip-column-names --execute "UPDATE gatekeep_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 202 AND claimed_by = 'wrong-owner' AND claimed_until = STR_TO_DATE('${gatekeep_lease}', '%Y-%m-%dT%H:%i:%s.%fZ'); SELECT ROW_COUNT();")"
    [[ "${wrong_fence}" == 0 ]]
    # shellcheck disable=SC2086
    fence_result="$(mysql_cli --batch --skip-column-names --execute "UPDATE gatekeep_audit_outbox SET claimed_by = NULL, claimed_until = NULL WHERE id = 202 AND claimed_by = '${gatekeep_owner}' AND claimed_until = STR_TO_DATE('${gatekeep_lease}', '%Y-%m-%dT%H:%i:%s.%fZ') AND claimed_until > UTC_TIMESTAMP(6); SELECT ROW_COUNT();")"
    [[ "${fence_result}" == 1 ]]
    # shellcheck disable=SC2086
    stale_ack="$(mysql_cli --batch --skip-column-names --execute "UPDATE gatekeep_audit_outbox SET delivered_at = '2037-01-01 00:00:00.000000' WHERE id = 202 AND claimed_by = '${gatekeep_owner}' AND claimed_until = STR_TO_DATE('${gatekeep_lease}', '%Y-%m-%dT%H:%i:%s.%fZ') AND delivered_at IS NULL; SELECT ROW_COUNT();")"
    [[ "${stale_ack}" == 0 ]]
fi

# A live run gets its own provenance ledger.  The ledger is deliberately not
# part of the source or Dovecote schema and must never make a normal checkout
# dirty (the container wrapper runs all four engines in one checkout).
if [[ -z "${DOVECOTE_FIXTURE_LEDGER:-}" ]]; then
    DOVECOTE_FIXTURE_LEDGER="$(mktemp "${TMPDIR:-/tmp}/dovecote-history-ledger.XXXXXX")"
    trap 'rm -f -- "${DOVECOTE_FIXTURE_LEDGER}" "${DOVECOTE_FIXTURE_LEDGER}.progress"' EXIT
fi

# The live backend job supplies the Dovecote schema via the adapter migration
# gate, then runs the same four-source high-water sequence with the public SQLx importer.
# Checkpoint decision 1000 through outbox row 1 first. The decision-only cursor
# must remain below row 500 while the independent outbox cursor advances to 1.
    run_fixture_runner "${DATABASE_URL}" 104 104 1000 104 1
if [[ "${backend}" == "postgres" ]]; then
    [[ "$(psql "${DATABASE_URL}" --tuples-only --no-align --command 'SELECT count(*) FROM dovecote_events;')" == 1 ]]
    [[ "$(psql "${DATABASE_URL}" --tuples-only --no-align --command "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-1';")" == 1 ]]
else
    [[ "$(mysql_cli --batch --skip-column-names --execute 'SELECT count(*) FROM dovecote_events;')" == 1 ]]
    [[ "$(mysql_cli --batch --skip-column-names --execute "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-1';")" == 1 ]]
fi
checkpoint="$(tail -n 1 "${DOVECOTE_FIXTURE_LEDGER}.progress")"
[[ "${checkpoint}" == *'"gatekeep_audit_cursor":0'* ]]
[[ "${checkpoint}" == *'"gatekeep_outbox_cursor":1'* ]]
    if run_fixture_runner "${DATABASE_URL}" 104 104 1000 104 2 crash; then
    echo "fixture runner did not crash after its committed batch" >&2
    exit 1
fi
    run_fixture_runner "${DATABASE_URL}" 104 104 1000 104
if [[ "${backend}" == "postgres" ]]; then
    psql "${DATABASE_URL}" --set ON_ERROR_STOP=1 --file "${fixture_root}/seed-postgres-late-v1.sql" >/dev/null
    # Fence legacy writers before taking the final source high-water snapshot.
    assert_postgres_writer_fence "${DATABASE_URL}"
else
    # shellcheck disable=SC2086
    mysql_cli < "${fixture_root}/seed-mysql-late-v1.sql"
    # Fence legacy writers before taking the final source high-water snapshot.
    assert_mysql_writer_fence
fi
    run_fixture_runner "${DATABASE_URL}" 206 206 1000 206 verify
    assert_independent_progress "${DOVECOTE_FIXTURE_LEDGER}.progress"
if [[ "${backend}" == "postgres" ]]; then
    counts="$(psql "${DATABASE_URL}" --tuples-only --no-align --command \
        "SELECT (SELECT count(*) FROM dovecote_events), (SELECT count(*) FROM dovecote_deliveries), (SELECT count(*) FROM keepsake_audit_events), (SELECT count(*) FROM keepsake_audit_outbox), (SELECT count(*) FROM gatekeep_audit_decisions), (SELECT count(*) FROM gatekeep_audit_outbox), (SELECT count(*) FROM gatekeep_audit_consulted_facts), (SELECT count(*) FROM gatekeep_audit_obligations), (SELECT count(*) FROM gatekeep_audit_request_subjects), (SELECT count(*) FROM gatekeep_audit_reason_params);")"
    [[ "${counts}" == "16|16|6|5|11|8|3|1|2|2" ]]
    [[ "$(psql "${DATABASE_URL}" --tuples-only --no-align --command "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-1';")" == 1 ]]
    [[ "$(psql "${DATABASE_URL}" --tuples-only --no-align --command "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-207';")" == 0 ]]
else
    # shellcheck disable=SC2086
    counts="$(mysql_cli --batch --skip-column-names --execute \
        "SELECT (SELECT count(*) FROM dovecote_events), (SELECT count(*) FROM dovecote_deliveries), (SELECT count(*) FROM keepsake_audit_events), (SELECT count(*) FROM keepsake_audit_outbox), (SELECT count(*) FROM gatekeep_audit_decisions), (SELECT count(*) FROM gatekeep_audit_outbox), (SELECT count(*) FROM gatekeep_audit_consulted_facts), (SELECT count(*) FROM gatekeep_audit_obligations), (SELECT count(*) FROM gatekeep_audit_request_subjects), (SELECT count(*) FROM gatekeep_audit_reason_params);")"
    [[ "${counts}" == $'16\t16\t6\t5\t11\t8\t3\t1\t2\t2' ]]
    [[ "$(mysql_cli --batch --skip-column-names --execute "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-1';")" == 1 ]]
    [[ "$(mysql_cli --batch --skip-column-names --execute "SELECT count(*) FROM dovecote_events WHERE event_id='gatekeep-outbox-207';")" == 0 ]]
fi
echo "complete-history fixture passes for ${backend} ${actual_version}"
