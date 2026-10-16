// SPDX-License-Identifier: BUSL-1.1

//! `UNION DISTINCT` merge: every row from every payload, deduplicated on
//! its raw msgpack bytes, in encounter order.

use crate::control::server::payload_merge::{encode_msgpack_array, payload_rows};

/// Merge multiple row payloads and drop duplicate rows.
///
/// Each payload holds its rows in a shape [`payload_rows`] reads. A row's
/// raw msgpack bytes are its dedup key, so no decode round-trip runs. A
/// payload [`payload_rows`] refuses fails the merge. The output is one
/// msgpack array of the unique rows in encounter order.
pub(crate) fn dedup_union_payloads(payloads: &[Vec<u8>]) -> crate::Result<Vec<u8>> {
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for payload in payloads {
        rows.extend(payload_rows(payload)?);
    }
    let mut seen: std::collections::HashSet<Vec<u8>> =
        std::collections::HashSet::with_capacity(rows.len());
    let mut unique_rows: Vec<Vec<u8>> = Vec::with_capacity(rows.len());

    for row in rows {
        if seen.insert(row.clone()) {
            unique_rows.push(row);
        }
    }

    Ok(encode_msgpack_array(&unique_rows))
}

#[cfg(test)]
mod tests {
    use super::dedup_union_payloads;

    fn encode_array(rows: &[serde_json::Value]) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows.to_vec())).unwrap()
    }

    #[test]
    fn union_distinct_drops_byte_identical_rows_across_payloads() {
        let left = encode_array(&[serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]);
        let right = encode_array(&[serde_json::json!({"x": 2}), serde_json::json!({"x": 3})]);

        let merged = dedup_union_payloads(&[left, right]).unwrap();
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"x":1},{"x":2},{"x":3}]"#);
    }

    #[test]
    fn union_distinct_of_no_payloads_is_empty_array() {
        assert_eq!(dedup_union_payloads(&[]).unwrap(), vec![0x90]);
    }

    /// A point-get branch answers its one row as a bare map.
    #[test]
    fn a_point_get_branch_is_one_row() {
        let left = encode_array(&[serde_json::json!({"x": 1})]);
        let map = nodedb_types::json_to_msgpack(&serde_json::json!({"x": 2})).unwrap();
        let merged = dedup_union_payloads(&[left, map]).unwrap();
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);
        assert_eq!(json, r#"[{"x":1},{"x":2}]"#);
    }

    /// A branch payload in no row shape is refused, never read as one row.
    #[test]
    fn a_scalar_branch_is_refused() {
        let left = encode_array(&[serde_json::json!({"x": 1})]);
        assert!(matches!(
            dedup_union_payloads(&[left, vec![0x2a]]),
            Err(crate::Error::Codec { .. })
        ));
    }
}
