// SPDX-License-Identifier: BUSL-1.1

//! The SET-list a columnar predicate-UPDATE WAL record carries.
//!
//! `ColumnarDmlWalRecord` lives in `nodedb-types`, which cannot name
//! `UpdateValue`. The record stores each assignment's value as the
//! MessagePack encoding of its `UpdateValue`. A literal and an expression
//! both survive the log, so replay re-executes the same computed UPDATE.

use nodedb_physical::physical_plan::UpdateValue;

/// Encode `updates` into the record's `(column, value bytes)` shape.
pub(crate) fn encode_columnar_dml_updates(
    updates: &[(String, UpdateValue)],
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    updates
        .iter()
        .map(|(field, value)| {
            let bytes =
                zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal columnar dml: assignment to '{field}': {e}"),
                })?;
            Ok((field.clone(), bytes))
        })
        .collect()
}

/// Decode the record's `(column, value bytes)` shape. A value that does not
/// decode as an `UpdateValue` is a `Serialization` error.
pub(crate) fn decode_columnar_dml_updates(
    updates: &[(String, Vec<u8>)],
) -> crate::Result<Vec<(String, UpdateValue)>> {
    updates
        .iter()
        .map(|(field, bytes)| {
            let value = zerompk::from_msgpack::<UpdateValue>(bytes).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal columnar dml: assignment to '{field}': {e}"),
                }
            })?;
            Ok((field.clone(), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_query::expr::{BinaryOp, SqlExpr};

    #[test]
    fn literal_and_expression_round_trip() {
        let updates = vec![
            (
                "label".to_string(),
                UpdateValue::Literal(
                    nodedb_types::value_to_msgpack(&nodedb_types::Value::String("x".into()))
                        .expect("encode literal"),
                ),
            ),
            (
                "v".to_string(),
                UpdateValue::Expr(SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Column("v".to_string())),
                    op: BinaryOp::Add,
                    right: Box::new(SqlExpr::Literal(nodedb_types::Value::Integer(1))),
                }),
            ),
        ];
        let encoded = encode_columnar_dml_updates(&updates).expect("encode");
        let decoded = decode_columnar_dml_updates(&encoded).expect("decode");
        assert_eq!(decoded, updates);
    }

    #[test]
    fn undecodable_value_is_an_error() {
        let err = decode_columnar_dml_updates(&[("v".to_string(), vec![0xc1])])
            .expect_err("0xc1 is not a MessagePack value");
        assert!(matches!(err, crate::Error::Serialization { .. }), "{err:?}");
    }
}
