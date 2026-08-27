#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config="${1:-${repo_root}/docs/debezium/dovecote-outbox.properties}"

test -f "${config}"

# Keep the parsed keys and values in indexed arrays. Besides keeping this
# fixture parser easy to inspect, that avoids treating property keys as shell
# expressions when checking duplicates.
property_keys=()
property_values=()

trim_space() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "${value}"
}

parse_properties() {
    local path="$1"
    local raw line key value existing

    property_keys=()
    property_values=()

    while IFS= read -r raw || [[ -n "${raw}" ]]; do
        line="$(trim_space "${raw}")"
        [[ -z "${line}" ]] && continue
        case "${line}" in
            \#*|\!*) continue ;;
        esac

        if [[ "${line}" != *=* ]]; then
            echo "malformed Debezium property (expected key=value): ${raw}" >&2
            return 1
        fi

        key="$(trim_space "${line%%=*}")"
        value="$(trim_space "${line#*=}")"
        if [[ -z "${key}" ]]; then
            echo "Debezium property has an empty key: ${raw}" >&2
            return 1
        fi

        for existing in "${property_keys[@]}"; do
            if [[ "${existing}" == "${key}" ]]; then
                echo "duplicate Debezium property key: ${key}" >&2
                return 1
            fi
        done

        property_keys+=("${key}")
        property_values+=("${value}")
    done < "${path}"
}

expect_property() {
    local expected_key="$1"
    local expected_value="$2"
    local index matches=0 actual=''

    for index in "${!property_keys[@]}"; do
        if [[ "${property_keys[index]}" == "${expected_key}" ]]; then
            matches=$((matches + 1))
            actual="${property_values[index]}"
        fi
    done

    if (( matches != 1 )); then
        echo "expected exactly one Debezium property: ${expected_key}" >&2
        return 1
    fi
    if [[ "${actual}" != "${expected_value}" ]]; then
        echo "wrong Debezium value for ${expected_key}: ${actual}" >&2
        return 1
    fi
}

required_properties=(
    "table.include.list=dovecote_events"
    "transforms=outbox"
    "transforms.outbox.type=io.debezium.transforms.outbox.EventRouter"
    "transforms.outbox.table.op.invalid.behavior=fatal"
    "transforms.outbox.table.field.event.id=event_id"
    "transforms.outbox.table.field.event.type=event_type"
    "transforms.outbox.table.field.event.key=partitionkey"
    "transforms.outbox.table.field.event.timestamp=enqueued_at"
    "transforms.outbox.table.field.event.payload=data"
    "transforms.outbox.table.expand.json.payload=false"
    "transforms.outbox.route.by.field=stream"
    'transforms.outbox.route.topic.replacement=outbox.event.${routedByValue}'
    "transforms.outbox.table.fields.additional.placement=specversion:header:ce_specversion,source:header:ce_source,subject:header:ce_subject,occurred_at:header:ce_time,datacontenttype:header:content-type,dataschema:header:ce_dataschema,partitionkey:header:ce_partitionkey,extensions:envelope:dovecote_extensions,data_kind:envelope:dovecote_data_kind,row_id:envelope:dovecote_row_id,enqueued_at:envelope:dovecote_enqueued_at"
)

check_config() {
    local path="$1"
    local property expected_key expected_value

    parse_properties "${path}"
    for property in "${required_properties[@]}"; do
        expected_key="${property%%=*}"
        expected_value="${property#*=}"
        expect_property "${expected_key}" "${expected_value}"
    done
}

self_test_parser() {
    local temp_dir duplicate commented
    temp_dir="$(mktemp -d)"
    duplicate="${temp_dir}/duplicate.properties"
    commented="${temp_dir}/commented.properties"

    printf '%s\n' \
        'table.include.list=dovecote_events' \
        'table.include.list=dovecote_events' > "${duplicate}"
    if parse_properties "${duplicate}" >/dev/null 2>&1; then
        echo "Debezium parser accepted duplicate keys" >&2
        rm -rf "${temp_dir}"
        return 1
    fi

    printf '%s\n' \
        '# table.include.list=wrong_table' \
        'table.include.list=dovecote_events' > "${commented}"
    if ! parse_properties "${commented}" >/dev/null 2>&1 || \
        ! expect_property "table.include.list" "dovecote_events" >/dev/null 2>&1; then
        echo "Debezium parser mishandled comments or exact values" >&2
        rm -rf "${temp_dir}"
        return 1
    fi

    rm -rf "${temp_dir}"
}

self_test_parser
check_config "${config}"
echo "Debezium outbox configuration fixture passes"
