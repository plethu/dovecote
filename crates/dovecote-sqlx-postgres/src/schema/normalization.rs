/// Normalizes PostgreSQL's version-dependent catalog rendering for comparison.
pub(crate) fn normalize_sql(value: &str) -> String {
    let mut value = value.to_ascii_lowercase();
    for cast in [
        "::character varying[]",
        "::character varying",
        "::timestamp with time zone",
        "::timestamp without time zone",
        "::double precision",
        "::numeric",
        "::bigint",
        "::integer",
        "::smallint",
        "::boolean",
        "::text[]",
        "::text",
    ] {
        value = value.replace(cast, "");
    }
    value
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect()
}
