#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
db="$(mktemp)"
trap 'rm -f -- "${db}"' EXIT

sqlite3 "${db}" < "${repo_root}/crates/carrier-sqlx-sqlite/migrations/0001_carrier.sql"

oversized_id="$(printf 'x%.0s' {1..1500})"
if sqlite3 "${db}" <<SQL
INSERT INTO carrier_events (
    stream, specversion, event_id, source, event_type, extensions, data_kind, data
) VALUES (
    'audit', '1.0', '${oversized_id}', 'https://example.test/source',
    'com.example.audit', '{}', NULL, NULL
);
SQL
then
    echo "expected SQLite migration to reject an oversized event_id" >&2
    exit 1
fi

echo "SQLite migration constraints pass"
