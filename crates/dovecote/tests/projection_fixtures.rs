//! Runtime-free projection vectors and local binding reference mappings.
//!
//! These tests deliberately exercise the public, transport-neutral projection
//! API. HTTP, Kafka, and NATS here are small local reference mappings over
//! BinaryProjection; they are not transport clients or evidence of broker and
//! HTTP-server conformance. The structured JSON vectors are separately checked
//! against the official CloudEvents schema and an external SDK parser below.

#[path = "projection_fixtures/support.rs"]
mod support;
#[path = "projection_fixtures/tests.rs"]
mod tests;
