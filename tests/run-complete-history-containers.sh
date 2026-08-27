#!/usr/bin/env bash
set -euo pipefail

# Environment-gated live matrix.  The fixture itself remains usable against an
# operator-provided DATABASE_URL; this wrapper supplies disposable, explicitly
# versioned servers when Docker/Podman images are available.  MySQL Innovation
# releases are intentionally overridable because vendors may publish that
# channel under a different registry name.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_script="${repo_root}/tests/complete-history-migration.sh"
runtime="${CONTAINER_RUNTIME:-docker}"
network="dovecote-history-${RANDOM}-${RANDOM}"
names=()

case "${runtime}" in
    docker|podman) ;;
    *) echo "CONTAINER_RUNTIME must be docker or podman" >&2; exit 2 ;;
esac

postgres_image="${DOVECOTE_POSTGRES_IMAGE:-postgres:17.11}"
mysql_image="${DOVECOTE_MYSQL_IMAGE:-mysql:8.4.11}"
mysql_innovation_image="${DOVECOTE_MYSQL_INNOVATION_IMAGE:-mysql:26.7.0}"

cleanup() {
    for name in "${names[@]}"; do
        "${runtime}" rm -f "${name}" >/dev/null 2>&1 || true
    done
    "${runtime}" network rm "${network}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${runtime}" network create "${network}" >/dev/null

start_postgres() {
    local name="dovecote-history-postgres"
    names+=("${name}")
    "${runtime}" run -d --name "${name}" --network "${network}" \
        -e POSTGRES_PASSWORD=test -e POSTGRES_DB=fixture \
        -p 127.0.0.1::5432 "${postgres_image}" >/dev/null
    until "${runtime}" exec "${name}" pg_isready -U postgres -d fixture >/dev/null 2>&1; do sleep 1; done
    local port
    port="$("${runtime}" port "${name}" 5432/tcp | sed -E 's/.*:([0-9]+)$/\1/')"
    DATABASE_URL="postgres://postgres:test@127.0.0.1:${port}/fixture" \
        "${fixture_script}" postgres
}

start_mysql() {
    local label="$1" image="$2" backend="$3"
    local name="dovecote-history-${label}"
    local client_program="mysql"
    if [[ "${backend}" == "mariadb" ]]; then
        client_program="mariadb"
    fi
    names+=("${name}")
    "${runtime}" run -d --name "${name}" --network "${network}" \
        -e MYSQL_ROOT_PASSWORD=test -e MYSQL_DATABASE=fixture \
        -p 127.0.0.1::3306 "${image}" >/dev/null
    until "${runtime}" exec "${name}" "${client_program}" \
        --user=root --password=test fixture --execute 'SELECT 1' >/dev/null 2>&1; do
        sleep 1
    done
    local port
    port="$("${runtime}" port "${name}" 3306/tcp | sed -E 's/.*:([0-9]+)$/\1/')"
    MYSQL_ARGS="--host=127.0.0.1 --port=${port} --user=root --password=test fixture" \
        MYSQL_CLI_CONTAINER="${name}" \
        MYSQL_CLI_PROGRAM="${client_program}" \
        CONTAINER_RUNTIME="${runtime}" \
        DATABASE_URL="mysql://root:test@127.0.0.1:${port}/fixture" \
        "${fixture_script}" "${backend}"
}

start_postgres
start_mysql mysql-lts "${mysql_image}" mysql
start_mysql mysql-innovation "${mysql_innovation_image}" mysql-innovation
"${repo_root}/tests/run-mariadb-history-upgrade.sh"

echo "complete-history container matrix passes: PostgreSQL 17.11; MySQL 8.4.11; MySQL Innovation 26.7.0; MariaDB 10.3.17 -> 11.8.6"
