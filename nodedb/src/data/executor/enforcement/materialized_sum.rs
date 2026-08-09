// SPDX-License-Identifier: BUSL-1.1

//! Materialized sum: on INSERT to source collection, atomically update balance
//! on the target collection within the same Data Plane transaction.
//!
//! The balance column on the target is maintained as:
//! `target.column += eval(value_expr, new_source_row)`
//!
//! This fires synchronously in the write path (not via Event Plane) to ensure
//! atomicity — the source INSERT and target balance update succeed or fail together.

use rust_decimal::Decimal;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::stored_row_encode::encode_stored_row;
use nodedb_physical::physical_plan::MaterializedSumBinding;

/// A target write performed by materialized sum, tracked for rollback.
pub struct TargetWrite {
    /// Target collection name.
    pub collection: String,
    /// Target document ID.
    pub document_id: String,
    /// Old value of the target document (before the balance update).
    /// `None` if the target document didn't exist.
    pub old_value: Option<Vec<u8>>,
}

impl CoreLoop {
    /// Apply materialized sum updates for all bindings on a source INSERT.
    ///
    /// For each binding:
    /// 1. Evaluate `value_expr` against the new source document → delta
    /// 2. Extract join key from source doc → target document ID
    /// 3. Read target document from sparse engine
    /// 4. Add delta to the target column
    /// 5. Write updated target document back
    ///
    /// Returns a list of (collection, doc_id, old_value) for rollback tracking.
    ///
    /// A `CoreLoop` method rather than a free function because the TARGET
    /// collection's stored encoding has to be resolved from `doc_configs` —
    /// each binding may point at a different collection, so the resolution has
    /// to happen per binding, inside the loop.
    pub(in crate::data::executor) fn apply_materialized_sums(
        &self,
        database_id: u64,
        tid: u64,
        bindings: &[MaterializedSumBinding],
        source_doc: &serde_json::Value,
    ) -> Result<Vec<TargetWrite>, ErrorCode> {
        let mut writes = Vec::new();

        for binding in bindings {
            let write = self.apply_single_binding(database_id, tid, binding, source_doc)?;
            if let Some(w) = write {
                writes.push(w);
            }
        }

        Ok(writes)
    }

    /// Apply a single materialized sum binding.
    fn apply_single_binding(
        &self,
        database_id: u64,
        tid: u64,
        binding: &MaterializedSumBinding,
        source_doc: &serde_json::Value,
    ) -> Result<Option<TargetWrite>, ErrorCode> {
        // 1. Evaluate value_expr against the source document to get the delta.
        // A materialized-sum binding fires on the write path: a
        // division/modulo-by-zero fails the write instead of silently skipping
        // the balance update.
        let source_val = nodedb_types::Value::from(source_doc.clone());
        let delta_val = binding
            .value_expr
            .eval(&source_val)
            .map_err(|_e| ErrorCode::DivisionByZero)?;
        let delta_json = serde_json::Value::from(delta_val);
        let delta = json_to_decimal(&delta_json);
        let Some(delta) = delta else {
            // value_expr evaluated to NULL or non-numeric → skip (no balance change).
            return Ok(None);
        };
        if delta == Decimal::ZERO {
            return Ok(None);
        }

        // 2. Extract the join key from the source document.
        let join_key = source_doc
            .get(&binding.join_column)
            .and_then(|v| v.as_str())
            .ok_or_else(|| ErrorCode::Internal {
                detail: format!(
                    "materialized_sum: join column '{}' missing or not a string in source document",
                    binding.join_column
                ),
            })?;

        // 3. Read the target document.
        //
        // The TARGET collection's encoding is resolved from `doc_configs`, not
        // assumed: the target is a different collection from the source and may
        // be strict (Binary Tuples) or vector-primary (tagged sidecars), neither
        // of which the schemaless decoder can read. The same resolved format
        // encodes the row back in step 5, so the read and the write can never
        // disagree about what this collection stores.
        let format = self.sparse_body_format(
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            &binding.target_collection,
        );

        let old_bytes = self
            .sparse
            .get(database_id, tid, &binding.target_collection, join_key)
            .map_err(|e| ErrorCode::Internal {
                detail: format!(
                    "materialized_sum: failed to read {}/{}: {e}",
                    binding.target_collection, join_key
                ),
            })?;

        let old_bytes = old_bytes.ok_or_else(|| ErrorCode::Internal {
            detail: format!(
                "materialized_sum: target row {}/{} not found",
                binding.target_collection, join_key
            ),
        })?;

        // 4. Decode, update balance, re-encode.
        let mut target_doc =
            decode_scanned_document(&old_bytes, format.as_format_ref()).map_err(|e| {
                ErrorCode::Internal {
                    detail: format!(
                        "materialized_sum: failed to decode target {}/{}: {e}",
                        binding.target_collection, join_key
                    ),
                }
            })?;

        let current_balance = target_doc
            .get(&binding.target_column)
            .and_then(json_to_decimal)
            .unwrap_or(Decimal::ZERO);

        let new_balance = current_balance + delta;

        // Update the balance field in the JSON document.
        // Always store as string to preserve exact Decimal precision — f64 is lossy
        // for values with >15 significant digits (e.g. 123456789012345.67).
        if let Some(obj) = target_doc.as_object_mut() {
            let json_val = serde_json::json!(new_balance.to_string());
            obj.insert(binding.target_column.clone(), json_val);
        }

        // 5. Write back in the encoding this collection stores. Writing msgpack
        // over a strict target would replace its Binary Tuple with a body no
        // reader of that collection can decode — a corrupted row that outlives
        // the statement.
        let new_bytes = encode_stored_row(&target_doc, format.as_format_ref()).map_err(|e| {
            ErrorCode::Internal {
                detail: format!(
                    "materialized_sum: failed to re-encode target {}/{}: {e}",
                    binding.target_collection, join_key
                ),
            }
        })?;
        self.sparse
            .put(
                database_id,
                tid,
                &binding.target_collection,
                join_key,
                &new_bytes,
            )
            .map_err(|e| ErrorCode::Internal {
                detail: format!(
                    "materialized_sum: failed to write {}/{}: {e}",
                    binding.target_collection, join_key
                ),
            })?;

        Ok(Some(TargetWrite {
            collection: binding.target_collection.clone(),
            document_id: join_key.to_string(),
            old_value: Some(old_bytes),
        }))
    }
}

/// Convert a JSON value to `rust_decimal::Decimal`.
fn json_to_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Decimal::from(i))
            } else {
                n.as_f64().and_then(|f| Decimal::try_from(f).ok())
            }
        }
        serde_json::Value::String(s) => s.parse::<Decimal>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::strict_format;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const TARGET: &str = "ms_accounts";
    const ACCOUNT: &str = "a1";

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// A strict target: `owner` is untouched by the sum and exists purely to
    /// prove the whole row survived the write-back.
    fn strict_target_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::required("owner", ColumnType::String),
            ColumnDef::required("balance", ColumnType::String),
        ])
        .expect("schema")
    }

    fn binding() -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: TARGET.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    /// MATERIALIZED SUM over a `document_strict` target must total correctly AND
    /// leave the row a Binary Tuple.
    ///
    /// The target is a different collection from the source, so its encoding has
    /// to be resolved from `doc_configs` on BOTH halves of the read-modify-write.
    /// Reading with the schemaless decoder failed on every strict row (the
    /// feature was simply broken there); writing msgpack back would have been
    /// worse — the row survives the statement and is unreadable to every strict
    /// reader afterwards.
    #[test]
    fn a_strict_target_totals_correctly_and_stays_a_binary_tuple() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());

        let schema = strict_target_schema();
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET).with_storage_mode(StorageMode::Strict {
                schema: schema.clone(),
            }),
        );

        // Seed the target row in the encoding a strict collection actually
        // stores: a Binary Tuple, not msgpack.
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), Value::String(ACCOUNT.into()));
        row.insert("owner".to_string(), Value::String("alice".into()));
        row.insert("balance".to_string(), Value::String("100".into()));
        let tuple = strict_format::value_to_binary_tuple(&Value::Object(row), &schema)
            .expect("encode seed tuple");
        core.sparse
            .put(DB, TID, TARGET, ACCOUNT, &tuple)
            .expect("seed target row");

        let bindings = vec![binding()];
        let source_row = serde_json::json!({"account_id": ACCOUNT, "amount": 25});
        core.apply_materialized_sums(DB, TID, &bindings, &source_row)
            .expect("a strict target must be supported, not rejected as undecodable");
        let source_row2 = serde_json::json!({"account_id": ACCOUNT, "amount": 75});
        core.apply_materialized_sums(DB, TID, &bindings, &source_row2)
            .expect("second contribution");

        let stored = core
            .sparse
            .get(DB, TID, TARGET, ACCOUNT)
            .expect("read back")
            .expect("row must still exist");

        // The stored bytes must still be a Binary Tuple. `binary_tuple_to_json`
        // is what every reader of this collection uses; if the write-back had
        // emitted msgpack this returns `None` and the row is lost.
        let decoded = strict_format::binary_tuple_to_json(&stored, &schema)
            .expect("the stored row must still be a readable Binary Tuple");

        assert_eq!(
            decoded.get("balance").and_then(|v| v.as_str()),
            Some("200"),
            "100 + 25 + 75 must be totalled onto the strict row: {decoded:?}"
        );
        assert_eq!(
            decoded.get("owner").and_then(|v| v.as_str()),
            Some("alice"),
            "columns the sum does not touch must survive the re-encode"
        );
        assert_eq!(decoded.get("id").and_then(|v| v.as_str()), Some(ACCOUNT));
    }

    /// The schemaless target keeps working — the encoding is chosen per
    /// collection, so fixing strict must not have moved schemaless onto the
    /// strict encoder.
    #[test]
    fn a_schemaless_target_still_totals_and_stays_msgpack() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());

        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET),
        );

        let seed = serde_json::json!({"id": ACCOUNT, "owner": "alice", "balance": "100"});
        let body = crate::data::executor::doc_format::encode_to_msgpack(&seed);
        core.sparse
            .put(DB, TID, TARGET, ACCOUNT, &body)
            .expect("seed target row");

        let bindings = vec![binding()];
        core.apply_materialized_sums(
            DB,
            TID,
            &bindings,
            &serde_json::json!({"account_id": ACCOUNT, "amount": 50}),
        )
        .expect("schemaless target");

        let stored = core
            .sparse
            .get(DB, TID, TARGET, ACCOUNT)
            .expect("read back")
            .expect("row must still exist");
        let decoded = crate::data::executor::doc_format::decode_document(&stored)
            .expect("a schemaless row must stay msgpack");
        assert_eq!(decoded.get("balance").and_then(|v| v.as_str()), Some("150"));
        assert_eq!(decoded.get("owner").and_then(|v| v.as_str()), Some("alice"));
    }

    #[test]
    fn json_to_decimal_integer() {
        assert_eq!(json_to_decimal(&serde_json::json!(100)), Some(d("100")));
    }

    #[test]
    fn json_to_decimal_float() {
        let val = json_to_decimal(&serde_json::json!(99.5));
        assert!(val.is_some());
    }

    #[test]
    fn json_to_decimal_string() {
        assert_eq!(
            json_to_decimal(&serde_json::json!("1500.75")),
            Some(d("1500.75"))
        );
    }

    #[test]
    fn json_to_decimal_null() {
        assert_eq!(json_to_decimal(&serde_json::Value::Null), None);
    }

    #[test]
    fn json_to_decimal_negative() {
        assert_eq!(json_to_decimal(&serde_json::json!(-250)), Some(d("-250")));
    }
}
