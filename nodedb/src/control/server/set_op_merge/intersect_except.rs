// SPDX-License-Identifier: BUSL-1.1

//! `INTERSECT` / `EXCEPT` merge over msgpack row payloads, comparing rows
//! by logical column values.

use crate::control::server::payload_merge::{encode_msgpack_array, payload_rows};

use super::row_key::{extract_values_key, logical_row_bytes, rows_match};

/// Which filter [`merge_set_op_payloads`] applies to the first payload.
pub(crate) enum SetMergeMode {
    /// Keep rows present in every payload.
    Intersect,
    /// Keep rows of the first payload absent from every later payload.
    Except,
}

/// Merge payloads for INTERSECT or EXCEPT.
///
/// Rows compare by value (see `row_key::rows_match`). The output holds the
/// logical row bytes (the `{id, data}` wrapper is stripped), deduplicated by
/// values key, as one msgpack array. No payloads yields an empty array. Each
/// payload holds its rows in a shape [`payload_rows`] reads, and a payload it
/// refuses fails the merge.
pub(crate) fn merge_set_op_payloads(
    payloads: &[Vec<u8>],
    mode: SetMergeMode,
) -> crate::Result<Vec<u8>> {
    let Some((first, others)) = payloads.split_first() else {
        return Ok(encode_msgpack_array(&[]));
    };

    let first_rows = payload_rows(first)?;
    let mut result_rows: Vec<Vec<u8>> = match mode {
        SetMergeMode::Intersect => {
            let other_rows: Vec<Vec<Vec<u8>>> = others
                .iter()
                .map(|p| payload_rows(p))
                .collect::<crate::Result<_>>()?;
            first_rows
                .into_iter()
                .filter(|row| {
                    other_rows
                        .iter()
                        .all(|rows| rows.iter().any(|other| rows_match(row, other)))
                })
                .map(|row| logical_row_bytes(&row).to_vec())
                .collect()
        }
        SetMergeMode::Except => {
            let mut other_rows: Vec<Vec<u8>> = Vec::new();
            for payload in others {
                other_rows.extend(payload_rows(payload)?);
            }
            first_rows
                .into_iter()
                .filter(|row| !other_rows.iter().any(|other| rows_match(row, other)))
                .map(|row| logical_row_bytes(&row).to_vec())
                .collect()
        }
    };

    let mut seen = std::collections::HashSet::with_capacity(result_rows.len());
    result_rows.retain(|r| seen.insert(extract_values_key(r)));

    Ok(encode_msgpack_array(&result_rows))
}

#[cfg(test)]
mod tests {
    use super::{SetMergeMode, merge_set_op_payloads};

    fn encode_array(rows: &[serde_json::Value]) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows.to_vec())).unwrap()
    }

    #[test]
    fn intersect_compares_wrapped_rows_by_logical_data_values() {
        let left = encode_array(&[
            serde_json::json!({"id":"u1","data":{"id":"u1","name":"Alice"}}),
            serde_json::json!({"id":"u2","data":{"id":"u2","name":"Bob"}}),
        ]);
        let right = encode_array(&[
            serde_json::json!({"id":"doc-1","data":{"user_id":"u1"}}),
            serde_json::json!({"id":"doc-2","data":{"user_id":"u3"}}),
        ]);

        let merged = merge_set_op_payloads(&[left, right], SetMergeMode::Intersect).unwrap();
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"id":"u1","name":"Alice"}]"#);
    }

    #[test]
    fn except_returns_unwrapped_logical_rows() {
        let left = encode_array(&[
            serde_json::json!({"id":"u1","data":{"id":"u1"}}),
            serde_json::json!({"id":"u2","data":{"id":"u2"}}),
        ]);
        let right = encode_array(&[serde_json::json!({"id":"doc-1","data":{"user_id":"u1"}})]);

        let merged = merge_set_op_payloads(&[left, right], SetMergeMode::Except).unwrap();
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"id":"u2"}]"#);
    }

    #[test]
    fn no_payloads_is_an_empty_array() {
        assert_eq!(
            merge_set_op_payloads(&[], SetMergeMode::Intersect).unwrap(),
            vec![0x90]
        );
    }

    /// A branch payload in no row shape is refused in either mode.
    #[test]
    fn a_scalar_branch_is_refused() {
        let left = encode_array(&[serde_json::json!({"id":"u1"})]);
        for mode in [SetMergeMode::Intersect, SetMergeMode::Except] {
            assert!(matches!(
                merge_set_op_payloads(&[left.clone(), vec![0x2a]], mode),
                Err(crate::Error::Codec { .. })
            ));
        }
    }
}
