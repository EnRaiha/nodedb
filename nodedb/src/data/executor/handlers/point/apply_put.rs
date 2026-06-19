// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointPut inside an externally-owned transaction" helper.
//!
//! This is called by PointPut and by any composite path (triggers, UPSERT)
//! that needs document write + index + stats side-effects atomically.

use redb::WriteTransaction;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use nodedb_types::Surrogate;

/// Parameters for [`CoreLoop::apply_point_put`].
pub(in crate::data::executor) struct PointPutParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
}

impl CoreLoop {
    /// Apply a PointPut within an externally-owned WriteTransaction.
    ///
    /// Stores the document, auto-indexes text fields, updates column stats,
    /// and populates the document cache. Does NOT commit the transaction.
    ///
    /// `surrogate` is the stable numeric identity for this document, used
    /// to key the inverted index. `document_id` is the hex-encoded form of
    /// the surrogate (the redb storage key).
    ///
    /// Returns the prior stored bytes when this put replaced an existing row,
    /// or `None` when it was a fresh insert. The caller threads the prior
    /// bytes into `emit_write_event` so the Event Plane's `WriteOp` tag
    /// reflects the actual mutation.
    pub(in crate::data::executor) fn apply_point_put(
        &mut self,
        txn: &WriteTransaction,
        params: PointPutParams<'_>,
    ) -> crate::Result<Option<Vec<u8>>> {
        let PointPutParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            value,
        } = params;
        // Evaluate generated columns before encoding.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let value = if let Some(config) = self.doc_configs.get(&config_key)
            && !config.enforcement.generated_columns.is_empty()
        {
            if let Some(mut doc) = super::super::super::doc_format::decode_document(value) {
                if let Err(e) = super::super::generated::evaluate_generated_columns(
                    &mut doc,
                    &config.enforcement.generated_columns,
                ) {
                    return Err(crate::Error::Storage {
                        engine: "generated".into(),
                        detail: format!("generated column evaluation failed: {e:?}"),
                    });
                }
                super::super::super::doc_format::encode_to_msgpack(&doc)
            } else {
                value.to_vec()
            }
        } else {
            super::super::super::doc_format::canonicalize_document_for_storage(value)
        };
        let value = &value;

        let bitemporal = self.is_bitemporal(tid, collection);
        let sys_from_ms = self.bitemporal_now_ms();
        let valid_from_ms = i64::MIN;
        let valid_until_ms = i64::MAX;

        // Strict (Binary Tuple) encoding pipeline. Runs in two steps under
        // a single doc-config lookup:
        //   (1) When the schema has an auto-generated `_rowid` primary key
        //       (injected by `build_strict_schema` when no explicit PK is
        //       declared), the client INSERT payload won't contain it.
        //       Inject it from the surrogate before encoding so the NOT NULL
        //       constraint is satisfied.
        //   (2) Encode the (possibly-injected) MessagePack into Binary Tuple.
        // Downstream indexing reads the rebound `value` so it sees the
        // injected `_rowid` alongside the user's fields.
        let value_with_rowid: Vec<u8>;
        let (value, stored): (&[u8], Vec<u8>) = if let Some(config) =
            self.doc_configs.get(&config_key)
            && let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
        {
            let encoded_input: &[u8] = if schema
                .columns
                .first()
                .is_some_and(|c| c.name == "_rowid" && !c.nullable)
                && let Ok(mut decoded) = nodedb_types::json_from_msgpack(value)
                && let serde_json::Value::Object(ref mut obj) = decoded
                && !obj.contains_key("_rowid")
            {
                obj.insert(
                    "_rowid".to_string(),
                    serde_json::Value::Number((surrogate.0 as i64).into()),
                );
                value_with_rowid =
                    nodedb_types::json_to_msgpack(&decoded).unwrap_or_else(|_| value.to_vec());
                &value_with_rowid
            } else {
                value
            };

            let stored = if bitemporal && schema.bitemporal {
                super::super::super::strict_format::bytes_to_binary_tuple_bitemporal(
                    encoded_input,
                    schema,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                )
            } else {
                super::super::super::strict_format::bytes_to_binary_tuple(encoded_input, schema)
            }
            .map_err(|e| crate::Error::Serialization {
                format: "binary_tuple".into(),
                detail: e.to_string(),
            })?;

            (encoded_input, stored)
        } else {
            (value, value.to_vec())
        };

        // Bitemporal collections version every write: read the current
        // (pre-write) version for the `prior` slot, then append a new
        // version at `sys_from = now()`. Non-bitemporal collections use
        // the legacy overwrite path, returning the old bytes redb replaced.
        let prior = if bitemporal {
            let current =
                self.sparse
                    .versioned_get_current(database_id, tid, collection, document_id)?;
            self.sparse.versioned_put_in_txn(
                txn,
                crate::engine::sparse::btree_versioned::VersionedPut {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    doc_id: document_id,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                    body: &stored,
                },
            )?;
            current
        } else {
            self.sparse
                .put_in_txn(txn, database_id, tid, collection, document_id, &stored)?
        };

        // Text indexing and stats use the original JSON input, not the stored
        // bytes — Binary Tuple requires a schema to decode, and the input JSON
        // is already available here regardless of storage mode.
        if let Some(doc) = super::super::super::doc_format::decode_document(value) {
            if let Some(obj) = doc.as_object() {
                let text_content: String = obj
                    .values()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" ");
                if !text_content.is_empty()
                    && let Err(e) = self.inverted.index_document_in_txn(
                        txn,
                        crate::types::TenantId::new(tid),
                        collection,
                        surrogate,
                        &text_content,
                    )
                {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index update failed");
                }
            }

            if let Err(e) =
                self.stats_store
                    .observe_document_in_txn(txn, database_id, tid, collection, &doc)
            {
                warn!(core = self.core_id, %collection, error = %e, "column stats update failed");
            }

            let tid_key = crate::types::TenantId::new(tid);
            let coll_prefix = format!("{collection}\0");
            self.aggregate_cache
                .retain(|(t, rest), _| !(*t == tid_key && rest.starts_with(&coll_prefix)));
        }

        self.doc_cache
            .put(database_id, tid, collection, document_id, &stored);

        // Secondary index extraction: if this collection has registered
        // index paths, extract values and write them into the INDEXES redb
        // B-Tree inside the CALLER'S write txn. Using the non-_in_txn
        // variant here would deadlock — `execute_point_put` already owns
        // the only writer.
        //
        // UNIQUE enforcement runs first: for every `unique: true` path we
        // check whether the incoming value already belongs to a different
        // document and reject with a typed constraint error. The check
        // uses the sparse engine's read API, which opens a separate read
        // transaction (redb MVCC) — the read view won't see our outer
        // write txn but that's precisely the semantics we want for the
        // "does another row already hold this value" question.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Some(doc) = super::super::super::doc_format::decode_document(value)
        {
            let paths = config.index_paths.clone();
            check_unique_constraints(
                &self.sparse,
                database_id,
                tid,
                collection,
                &doc,
                document_id,
                &paths,
            )?;
            if bitemporal {
                let sys_from = self.bitemporal_now_ms();
                for path in &paths {
                    if let Some(ref pred) = path.predicate
                        && !pred.evaluate_json(&doc)
                    {
                        continue;
                    }
                    for v in crate::engine::document::store::extract_index_values(
                        &doc,
                        &path.path,
                        path.is_array,
                    ) {
                        let value = if path.case_insensitive {
                            v.to_lowercase()
                        } else {
                            v
                        };
                        self.sparse.versioned_index_put_in_txn(
                            txn,
                            database_id,
                            tid,
                            collection,
                            &path.path,
                            &value,
                            document_id,
                            sys_from,
                        )?;
                    }
                }
            } else {
                self.apply_secondary_indexes_in_txn(
                    txn,
                    database_id,
                    tid,
                    collection,
                    &doc,
                    document_id,
                    &paths,
                );
            }
        }

        self.apply_point_put_spatial(database_id, tid, collection, document_id, value);
        self.apply_point_put_vector_indexes(database_id, tid, collection, value);

        Ok(prior)
    }
}

/// Reject the write if any `unique: true` index already holds one of the
/// incoming document's extracted values under a *different* `document_id`.
///
/// Runs before `apply_secondary_indexes_in_txn` so the caller's write
/// transaction is still clean — rejection does not roll anything back.
/// Same-id re-puts (idempotent overwrites) are allowed through; we only
/// reject when another row owns the value.
#[allow(clippy::too_many_arguments)]
fn check_unique_constraints(
    sparse: &crate::engine::sparse::btree::SparseEngine,
    database_id: u64,
    tid: u64,
    collection: &str,
    doc: &serde_json::Value,
    document_id: &str,
    paths: &[crate::engine::document::store::IndexPath],
) -> crate::Result<()> {
    use crate::engine::document::store::extract_index_values;

    let doc_engine = crate::engine::document::store::DocumentEngine::new(sparse, database_id, tid);
    for path in paths {
        if !path.unique {
            continue;
        }
        // A partial UNIQUE index only applies to rows the predicate
        // accepts; rows outside the predicate's scope are not part of
        // the uniqueness domain. Skipping the check here mirrors the
        // skip in `apply_secondary_indexes_in_txn` so the two paths
        // agree on which rows the index governs.
        if let Some(ref p) = path.predicate
            && !p.evaluate_json(doc)
        {
            continue;
        }
        for raw in extract_index_values(doc, &path.path, path.is_array) {
            let needle = if path.case_insensitive {
                raw.to_lowercase()
            } else {
                raw
            };
            let existing = doc_engine
                .index_lookup(collection, &path.path, &needle)
                .unwrap_or_default();
            if existing.iter().any(|id| id != document_id) {
                return Err(crate::Error::RejectedConstraint {
                    collection: collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "unique index '{}' violation on field '{}' (value '{}')",
                        path.name, path.path, needle
                    ),
                });
            }
        }
    }
    Ok(())
}
