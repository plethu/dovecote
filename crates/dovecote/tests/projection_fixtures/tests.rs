use super::support::*;

#[test]
fn checked_in_vectors_match_projection_and_local_binding_reference_mappings() {
    for fixture in fixtures() {
        let event = event_for(&fixture.name);
        assert_eq!(
            event.extensions().canonical_json(),
            fixture.durable_extensions
        );
        let portable_size = event.portable_size().expect("fixture size is valid");
        let stored = event.into_stored().expect("fixture event is valid");
        let structured = stored
            .structured_json()
            .expect("structured projection is valid");
        assert_eq!(structured.as_bytes(), fixture.structured_json.as_bytes());

        let structured_value: Value = serde_json::from_slice(structured.as_bytes())
            .expect("structured projection is valid JSON");
        let expected_value: Value = serde_json::from_str(&fixture.structured_json)
            .expect("fixture structured projection is valid JSON");
        assert_eq!(structured_value, expected_value);

        let binary = stored.binary();
        assert_eq!(
            binary.body().map(ToOwned::to_owned),
            expected_bytes(&fixture.binary.body)
        );
        assert_eq!(
            binary
                .datacontenttype()
                .map(|value| value.as_str().to_owned()),
            fixture.binary.datacontenttype
        );
        assert_eq!(
            binary
                .attributes()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect::<Vec<_>>(),
            fixture.binary.attributes
        );

        let (http_body, http_headers) = http_binary(&stored);
        assert_eq!(BASE64.encode(http_body), fixture.http.body);
        assert_eq!(http_headers, fixture.http.headers);
        assert!(
            !http_headers
                .iter()
                .any(|(name, _)| name == "ce-datacontenttype")
        );

        let kafka = kafka_binary(&stored);
        assert_eq!(
            kafka.key.map(|value| BASE64.encode(value)),
            fixture.kafka.key
        );
        assert_eq!(
            kafka.body.map(|value| BASE64.encode(value)),
            fixture.kafka.body
        );
        assert_eq!(kafka.headers, fixture.kafka.headers);

        let (nats_body, nats_headers, nats_msg_id) = nats_binary(&stored);
        assert_eq!(BASE64.encode(nats_body), fixture.nats.body);
        assert_eq!(nats_headers, fixture.nats.headers);
        assert_eq!(nats_msg_id, fixture.nats.msg_id);

        // The fixture's event remains valid at its exact logical-size boundary;
        // one byte below it is rejected before any adapter can insert it.
        assert!(portable_size > 0);
        let exact = event_for(&fixture.name)
            .validate_with_limit(dovecote::EventSizeLimit::new(portable_size).unwrap());
        assert!(exact.is_ok());
        let below = event_for(&fixture.name)
            .validate_with_limit(dovecote::EventSizeLimit::new(portable_size - 1).unwrap());
        assert!(below.is_err());
    }
}

#[test]
fn structured_projection_vectors_are_parsed_by_external_cloudevents_sdk() {
    for fixture in fixtures() {
        let raw: Value = serde_json::from_str(&fixture.structured_json)
            .expect("fixture structured projection is valid JSON");
        let mut sdk_input = raw.clone();

        // cloudevents-sdk 0.9.0 currently decides whether `data` is JSON by
        // checking whether the raw content type ends in `+json`. The schema
        // validation below checks the exact projection, including media-type
        // parameters; normalize only this SDK compatibility probe so that the
        // independent parser can still cover the event and its data.
        if let Some(Value::String(content_type)) = sdk_input.get_mut("datacontenttype") {
            *content_type = content_type.split_once(';').map_or_else(
                || content_type.clone(),
                |(media_type, _)| media_type.to_owned(),
            );
        }

        let event: ExternalCloudEvent = serde_json::from_value(sdk_input).unwrap_or_else(|error| {
            panic!(
                "{} is not accepted by cloudevents-sdk: {error}",
                fixture.name
            )
        });

        assert!(
            matches!(event.specversion(), SpecVersion::V10),
            "{}",
            fixture.name
        );
        assert!(!event.id().is_empty(), "{}", fixture.name);
        assert!(!event.source().as_str().is_empty(), "{}", fixture.name);
        assert!(!event.ty().is_empty(), "{}", fixture.name);

        match event.data() {
            None => assert_eq!(fixture.name, "absent"),
            Some(Data::Binary(data)) => {
                assert_eq!(
                    Some(BASE64.encode(data)),
                    fixture.binary.body,
                    "{}",
                    fixture.name
                )
            }
            Some(Data::Json(data)) => assert_eq!(Some(data), raw.get("data"), "{}", fixture.name),
            Some(Data::String(_)) => panic!(
                "{} was decoded as a string rather than JSON or binary data",
                fixture.name
            ),
        }
    }
}

#[test]
fn structured_projection_validates_against_official_cloudevents_schema() {
    let schema_bytes = include_bytes!("../fixtures/cloudevents-v1.0.2.json");
    assert_eq!(
        format!("{:x}", Sha256::digest(schema_bytes)),
        CLOUDEVENTS_SCHEMA_SHA256,
        "the checked-in schema must remain the official v1.0.2 artifact"
    );
    let schema: Value =
        serde_json::from_slice(schema_bytes).expect("checked-in CloudEvents schema is valid JSON");
    jsonschema::draft7::meta::validate(&schema).expect("CloudEvents schema is Draft 7 JSON");
    let validator = jsonschema::draft7::options()
        .should_validate_formats(true)
        .build(&schema)
        .expect("CloudEvents schema compiles");

    for fixture in fixtures() {
        let value: Value = serde_json::from_str(&fixture.structured_json)
            .expect("fixture structured projection is valid JSON");
        validator.validate(&value).unwrap_or_else(|error| {
            panic!(
                "{} is not valid under the CloudEvents v1.0.2 JSON Schema: {error}",
                fixture.name
            )
        });

        let has_data = value.get("data").is_some();
        let has_binary_data = value.get("data_base64").is_some();
        assert!(
            !(has_data && has_binary_data),
            "{} has both data forms",
            fixture.name
        );
        match fixture.name.as_str() {
            "absent" => assert!(!has_data && !has_binary_data),
            "binary" | "empty" | "text" => assert!(!has_data && has_binary_data),
            "full-json" | "scalar" => assert!(has_data && !has_binary_data),
            unknown => panic!("unknown projection fixture: {unknown}"),
        }
    }
}

fn kafka_binary_value(
    event: &dovecote::StoredEvent,
    compacted: bool,
    allow_compaction_tombstone: bool,
) -> Result<Option<Vec<u8>>, &'static str> {
    let value = event.binary().body().map(ToOwned::to_owned);
    if compacted && value.is_none() && !allow_compaction_tombstone {
        return Err("absent binary data would be a Kafka compaction tombstone");
    }
    Ok(value)
}

#[test]
fn kafka_reference_mapping_keeps_absent_and_empty_values_distinct() {
    let absent = absent_event().into_stored().unwrap();
    assert_eq!(
        kafka_binary_value(&absent, true, false),
        Err("absent binary data would be a Kafka compaction tombstone")
    );
    assert_eq!(kafka_binary_value(&absent, true, true), Ok(None));

    let empty = empty_event().into_stored().unwrap();
    assert_eq!(
        kafka_binary_value(&empty, true, false),
        Ok(Some(Vec::new()))
    );
}

#[test]
fn public_boundary_vectors_cover_base64_percent_encoding_and_both_size_formulas() {
    let binary = binary_event().into_stored().unwrap();
    let structured = binary.structured_json().unwrap();
    assert!(structured.as_bytes().len() > binary.binary().body().unwrap().len());

    let full = full_event();
    let portable_size = full.portable_size().unwrap();
    let stored = full.into_stored().unwrap();
    let structured = stored.structured_json().unwrap();
    let structured_material = structured.as_bytes().len()
        + "Content-Type: ".len()
        + dovecote::StructuredJsonProjection::CONTENT_TYPE.len()
        + "\r\n".len();
    let projection = stored.binary();
    let mut binary_material = projection.body().map_or(0, <[u8]>::len);
    for (name, value) in projection.attributes() {
        binary_material += format!("ce-{name}").len() + 4 + value.len() * 3;
    }

    if let Some(content_type) = projection.datacontenttype() {
        binary_material += "ce-datacontenttype".len() + 4 + content_type.as_str().len() * 3;
    }
    assert_eq!(portable_size, structured_material.max(binary_material));

    assert_eq!(percent_encode("é \"%"), "%C3%A9%20%22%25");
    let (_, headers) = http_binary(&stored);
    assert!(headers.iter().any(|(name, value)| {
        name == "ce-subject" && value == "subject%20%22%20%25%20caf%C3%A9"
    }));

    // The public event-size API can enforce a destination-specific finite
    // logical limit exactly, but lower transport framing is intentionally not
    // represented by Dovecote and remains an integration-owned check.
    assert!(
        full_event()
            .validate_with_limit(dovecote::EventSizeLimit::new(portable_size).unwrap())
            .is_ok()
    );
    assert!(
        full_event()
            .validate_with_limit(dovecote::EventSizeLimit::new(portable_size - 1).unwrap())
            .is_err()
    );
}
