#!/usr/bin/env bash
set -euo pipefail

# Rehearse the supported MariaDB maintenance-window route.  The historical
# Keepsake and Gatekeep artifacts are applied only while creating the source
# database on the compatible server.  The target server reuses that data
# volume, runs mariadb-upgrade, and imports the existing tables; it never
# replays a Keepsake migration on MariaDB 11.8.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_script="${repo_root}/tests/complete-history-migration.sh"
runtime="${CONTAINER_RUNTIME:-docker}"
source_image="${DOVECOTE_MARIADB_SOURCE_IMAGE:-mariadb:10.3.17}"
target_image="${DOVECOTE_MARIADB_TARGET_IMAGE:-mariadb:11.8.6}"
source_version="${DOVECOTE_MARIADB_SOURCE_VERSION:-10.3.17}"
target_version="${DOVECOTE_MARIADB_TARGET_VERSION:-11.8.6}"
run_id="${RANDOM}-${RANDOM}"
network="dovecote-mariadb-upgrade-${run_id}"
volume="dovecote-mariadb-upgrade-${run_id}"
source_name="dovecote-mariadb-source-${run_id}"
target_name="dovecote-mariadb-target-${run_id}"
names=("${source_name}" "${target_name}")

case "${runtime}" in
    docker|podman) ;;
    *) echo "CONTAINER_RUNTIME must be docker or podman" >&2; exit 2 ;;
esac

cleanup() {
    local name
    for name in "${names[@]}"; do
        "${runtime}" rm -f "${name}" >/dev/null 2>&1 || true
    done
    "${runtime}" volume rm "${volume}" >/dev/null 2>&1 || true
    "${runtime}" network rm "${network}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_server() {
    local name="$1"
    local client="$2"
    local attempt
    for attempt in {1..180}; do
        if "${runtime}" exec "${name}" "${client}" \
            --user=root --password=test fixture --execute 'SELECT 1' \
            >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "timed out after ${attempt} attempts waiting for MariaDB container ${name}" >&2
    return 1
}

set_session_defaults() {
    local name="$1"
    local client="$2"
    local isolation_variable="$3"
    "${runtime}" exec "${name}" "${client}" \
        --user=root --password=test fixture --execute \
        "SET GLOBAL time_zone = '+00:00'; SET GLOBAL ${isolation_variable} = 'REPEATABLE-READ'; SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES'; SET GLOBAL character_set_server = 'utf8mb4'; SET GLOBAL collation_server = 'utf8mb4_bin'; SET GLOBAL innodb_lock_wait_timeout = 5; ALTER DATABASE fixture CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;"
}

"${runtime}" network create "${network}" >/dev/null
"${runtime}" volume create "${volume}" >/dev/null

"${runtime}" run -d --name "${source_name}" --network "${network}" \
    -e MYSQL_ROOT_PASSWORD=test -e MYSQL_DATABASE=fixture \
    -e MARIADB_ROOT_PASSWORD=test -e MARIADB_DATABASE=fixture \
    -v "${volume}:/var/lib/mysql" "${source_image}" >/dev/null
wait_for_server "${source_name}" mysql
set_session_defaults "${source_name}" mysql tx_isolation

# This invocation is source-schema preparation only.  It uses the normal
# complete-history harness, whose migration_file/hash checks select reviewed
# sibling artifacts (or the checked-in published copies), rather than adding a
# second handwritten schema.
CONTAINER_RUNTIME="${runtime}" \
MYSQL_CLI_CONTAINER="${source_name}" \
MYSQL_CLI_PROGRAM=mysql \
DOVECOTE_MARIADB_SOURCE_ONLY=1 \
DOVECOTE_MARIADB_SOURCE_VERSION="${source_version}" \
    "${fixture_script}" mariadb

"${runtime}" stop "${source_name}" >/dev/null
"${runtime}" rm -f "${source_name}" >/dev/null

"${runtime}" run -d --name "${target_name}" --network "${network}" \
    -e MARIADB_ROOT_PASSWORD=test -e MARIADB_DATABASE=fixture \
    -v "${volume}:/var/lib/mysql" -p 127.0.0.1::3306 "${target_image}" >/dev/null
wait_for_server "${target_name}" mariadb

# MariaDB's documented major-version route requires the system-table upgrade
# after the server starts on the existing data directory.
"${runtime}" exec "${target_name}" mariadb-upgrade \
    --user=root --password=test --force
set_session_defaults "${target_name}" mariadb transaction_isolation

target_port="$("${runtime}" port "${target_name}" 3306/tcp | sed -E 's/.*:([0-9]+)$/\1/')"
if [[ ! "${target_port}" =~ ^[0-9]+$ ]]; then
    echo "could not determine the published MariaDB port" >&2
    exit 1
fi

# The target phase is explicitly source-ready.  complete-history-migration.sh
# skips every historical application and runs only the importer/reconciliation
# against the schema that survived the server upgrade.
CONTAINER_RUNTIME="${runtime}" \
MYSQL_CLI_CONTAINER="${target_name}" \
MYSQL_CLI_PROGRAM=mariadb \
DOVECOTE_MARIADB_SOURCE_READY=1 \
DOVECOTE_MARIADB_TARGET_VERSION="${target_version}" \
DATABASE_URL="mysql://root:test@127.0.0.1:${target_port}/fixture" \
MYSQL_ARGS="--host=127.0.0.1 --port=${target_port} --user=root --password=test fixture" \
    "${fixture_script}" mariadb

echo "MariaDB maintenance-window fixture passes: ${source_version} -> ${target_version}"
