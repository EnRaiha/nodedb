// SPDX-License-Identifier: BUSL-1.1

//! Row-level-security WRITE enforcement for timeseries ingest.
//!
//! A timeseries row only exists once the line-protocol parser has produced it,
//! so the Control Plane ships the compiled predicate on the plan and the rows
//! are decided here. Every ingest format — line protocol from the raw listener,
//! its canonical MessagePack form, JSON rows — normalizes into parsed ILP lines
//! before anything is appended, so deciding them here covers all of them.
//!
//! The whole batch is decided before the first row is appended: a rejection
//! fails the statement with nothing written, rather than leaving the lines
//! ahead of the offending one durable.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::data::executor::handlers::rls_write_gate::admit_value_row;
use crate::engine::timeseries::ilp::{FieldValue, IlpLine};

/// Nanoseconds per millisecond — line protocol timestamps are nanoseconds, the
/// stored time column is milliseconds.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Decide every parsed line against the compiled write policy.
///
/// `time_key` is the collection's declared `TIME_KEY`, absent only for a
/// measurement with no DDL behind it (raw protocol ingest into a collection
/// that was never created), where no column name can be bound to the line's
/// timestamp. Empty `rls_write_check` admits everything, the same convention
/// every other write gate uses.
pub(super) fn admit_ilp_lines(
    rls_write_check: &[u8],
    lines: &[IlpLine<'_>],
    time_key: Option<&str>,
    default_timestamp_ms: i64,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    if rls_write_check.is_empty() {
        return Ok(());
    }
    for line in lines {
        let image = line_image(line, time_key, default_timestamp_ms);
        admit_value_row(rls_write_check, &image, tid, collection)?;
    }
    Ok(())
}

/// Build the row image a line will be stored as: its tags, its fields, and its
/// timestamp bound to the declared time column.
///
/// The timestamp is written last and unconditionally, because the ingest path
/// gives the designated time column the line's own timestamp regardless of any
/// field that happens to share its name — the image the policy decides has to
/// be the row that will exist, not the row as submitted.
fn line_image(line: &IlpLine<'_>, time_key: Option<&str>, default_timestamp_ms: i64) -> Value {
    let mut map: HashMap<String, Value> =
        HashMap::with_capacity(line.tags.len() + line.fields.len() + 1);
    for (key, value) in &line.tags {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
    for (key, value) in &line.fields {
        map.insert(key.to_string(), field_value(value));
    }
    if let Some(key) = time_key {
        let ts_ms = line
            .timestamp_ns
            .map(|ns| ns / NANOS_PER_MILLI)
            .unwrap_or(default_timestamp_ms);
        map.insert(key.to_string(), Value::Integer(ts_ms));
    }
    Value::Object(map)
}

/// An unsigned field beyond `i64::MAX` has no integer representation the value
/// model can hold, so it widens to a float rather than wrapping negative — a
/// wrapped value would be compared against a policy bound as a different number
/// entirely.
fn field_value(value: &FieldValue<'_>) -> Value {
    match value {
        FieldValue::Float(f) => Value::Float(*f),
        FieldValue::Int(i) => Value::Integer(*i),
        FieldValue::UInt(u) => i64::try_from(*u)
            .map(Value::Integer)
            .unwrap_or_else(|_| Value::Float(*u as f64)),
        FieldValue::Str(s) => Value::String(s.to_string()),
        FieldValue::Bool(b) => Value::Bool(*b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::ScanFilter;
    use crate::engine::timeseries::ilp;

    fn owner_policy(owner: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: "eq".into(),
            value: Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    fn lines(batch: &str) -> Vec<ilp::IlpLine<'_>> {
        ilp::parse_batch(batch).expect("parse ILP").into_lines()
    }

    #[test]
    fn an_empty_check_admits_every_line() {
        let batch = "cpu,owner=mallory value=1i\n";
        assert!(admit_ilp_lines(&[], &lines(batch), Some("ts"), 0, 1, "cpu").is_ok());
    }

    #[test]
    fn a_conforming_line_is_admitted() {
        let batch = "cpu,owner=alice value=1i\n";
        assert!(
            admit_ilp_lines(
                &owner_policy("alice"),
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            )
            .is_ok()
        );
    }

    /// One violating line fails the whole batch — the lines ahead of it must
    /// not become durable.
    #[test]
    fn one_violating_line_rejects_the_batch() {
        let batch = "cpu,owner=alice value=1i\ncpu,owner=mallory value=2i\n";
        assert!(matches!(
            admit_ilp_lines(
                &owner_policy("alice"),
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            ),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A line with no value for the governed column cannot satisfy the
    /// predicate, so it is rejected rather than admitted by omission.
    #[test]
    fn a_line_without_the_governed_column_is_rejected() {
        let batch = "cpu value=1i\n";
        assert!(
            admit_ilp_lines(
                &owner_policy("alice"),
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            )
            .is_err()
        );
    }

    /// A filter payload that does not deserialize denies rather than passing
    /// the batch through unchecked.
    #[test]
    fn a_corrupt_check_denies() {
        let batch = "cpu,owner=alice value=1i\n";
        assert!(admit_ilp_lines(&[0xFF, 0xFE], &lines(batch), Some("ts"), 0, 1, "cpu").is_err());
    }

    /// The line's own timestamp is bound to the declared time column, in the
    /// milliseconds the row is stored with.
    #[test]
    fn the_time_column_carries_the_line_timestamp_in_milliseconds() {
        let batch = "cpu,owner=alice value=1i 1700000000000000000\n";
        let parsed = lines(batch);
        let image = line_image(&parsed[0], Some("ts"), 7);
        let Value::Object(map) = image else {
            panic!("row image must be an object");
        };
        assert_eq!(map.get("ts"), Some(&Value::Integer(1_700_000_000_000)));
    }

    /// A line with no timestamp is stored with the batch's default, so that is
    /// what the policy decides.
    #[test]
    fn a_line_without_a_timestamp_carries_the_batch_default() {
        let parsed = lines("cpu,owner=alice value=1i\n");
        let image = line_image(&parsed[0], Some("ts"), 7);
        let Value::Object(map) = image else {
            panic!("row image must be an object");
        };
        assert_eq!(map.get("ts"), Some(&Value::Integer(7)));
    }
}
