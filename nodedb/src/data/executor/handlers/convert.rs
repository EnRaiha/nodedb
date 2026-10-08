// SPDX-License-Identifier: BUSL-1.1

//! CONVERT COLLECTION handler: re-encode documents for a new storage mode.
//!
//! Scans all documents in the collection and re-encodes them in-place.
//! For `TO strict`: injects each row's client-visible `id`, validates the
//! result against the schema, and encodes it as a Binary Tuple via
//! `strict_format::bytes_to_binary_tuple`.
//! For `TO document` or `TO kv`: a Binary Tuple source re-encodes to
//! MessagePack. A schemaless source needs no re-encoding — the sparse
//! engine already stores it as MessagePack.
//! A row that fails to convert fails the whole statement: the handler
//! returns an error response instead of a success payload, so the caller
//! never flips the catalog's collection type over partially-converted data.

use sonic_rs;

use nodedb_physical::physical_plan::StorageMode;
use nodedb_query::msgpack_scan;
use nodedb_types::RowIdentity;
use nodedb_types::columnar::{ColumnDef, StrictSchema};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::data::executor::task::ExecutionTask;

/// Map the plan's declared source storage mode to the row-decode format.
///
/// The plan carries this instead of the handler reading `doc_configs`: at
/// dispatch time that cache still describes the mode from BEFORE this
/// conversion, since the catalog flip and Data Plane re-register happen
/// only after this op returns successfully.
fn source_format_of(mode: &StorageMode) -> SparseBodyFormat {
    match mode {
        StorageMode::Strict { schema } => SparseBodyFormat::Strict(schema.clone()),
        StorageMode::Schemaless => SparseBodyFormat::Document,
    }
}

impl CoreLoop {
    /// Execute a collection conversion.
    ///
    /// - `TO document` / `TO kv`: re-encodes a Binary Tuple source to
    ///   MessagePack. A schemaless source is left untouched. Catalog update
    ///   happens on the Control Plane, after this returns successfully.
    /// - `TO strict`: re-encode each document as a Binary Tuple using the
    ///   provided schema. A document that fails to encode fails the
    ///   statement.
    pub(in crate::data::executor) fn execute_convert_collection(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        target_type: &str,
        schema_json: &str,
        source_storage_mode: &StorageMode,
    ) -> Response {
        tracing::debug!(
            core = self.core_id,
            %collection,
            target_type,
            "converting collection"
        );

        let source_format = source_format_of(source_storage_mode);

        match target_type {
            "document_strict" => {
                self.convert_to_strict(task, tid, collection, schema_json, source_format)
            }
            "document_schemaless" | "kv" => {
                self.convert_from_strict(task, tid, collection, target_type, source_format)
            }
            other => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("unsupported conversion target: {other}"),
                },
            ),
        }
    }

    /// Convert to strict mode: re-encode each document as a Binary Tuple.
    ///
    /// The sparse engine keys a minted row by its storage key, not its
    /// client-visible identity. A `SELECT` synthesizes the identity under the
    /// identity column at read time; this re-encode does the same under the
    /// target schema's key column before validating and encoding, or a row
    /// that lacks its key loses its identity and the target schema's NOT NULL
    /// key column rejects it. A row that holds its key gains no `id`.
    fn convert_to_strict(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        schema_json: &str,
        source_format: SparseBodyFormat,
    ) -> Response {
        // Parse the target schema from JSON column definitions.
        let columns: Vec<ColumnDef> = match sonic_rs::from_str(schema_json) {
            Ok(c) => c,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid schema JSON: {e}"),
                    },
                );
            }
        };

        if columns.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "schema must have at least one column".into(),
                },
            );
        }

        let schema = StrictSchema {
            columns,
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let declared_primary_key = schema
            .columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| c.name.as_str());
        let identity_column = nodedb_types::declared_key(declared_primary_key)
            .unwrap_or(nodedb_types::DEFAULT_IDENTITY_COLUMN);

        // Scan all existing documents.
        let database_id = task.request.database_id.as_u64();
        let docs = match self
            .sparse
            .scan_documents(database_id, tid, collection, usize::MAX)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };

        let mut converted = 0u64;

        for (doc_id, doc_bytes) in &docs {
            let normalized = sparse_body_to_msgpack(doc_bytes, source_format.as_format_ref());
            // A row that lacks the target's key column has no client-visible
            // identity yet: inject the surrogate's decimal string there so the
            // target schema's NOT NULL key column has something to validate.
            let synth_id = doc_id.to_identity();
            let with_id =
                msgpack_scan::inject_str_field(&normalized, identity_column, synth_id.as_str());
            // The identity a user recognizes: the declared primary key's value,
            // or `id`, read from the row itself — never the internal surrogate.
            let identity = RowIdentity::of_stored_row(&normalized, declared_primary_key, *doc_id);

            let tuple_bytes = match super::super::strict_format::bytes_to_binary_tuple(
                &with_id, &schema, collection,
            ) {
                Ok(bytes) => bytes,
                // A row carrying a field the target schema does not declare is
                // a schema mismatch the client caused, not an internal fault:
                // name the offending row and column as SQLSTATE 22000
                // (data_exception) instead of collapsing into a generic
                // internal error. `ErrorCode::UndefinedColumn`, the Data
                // Plane's other 42703-classified code, carries only a column
                // name, with no room for the collection or the row that
                // failed, so it would drop both here.
                Err(crate::Error::UnknownStrictField {
                    collection, column, ..
                }) => {
                    return self.response_error(
                        task,
                        ErrorCode::DataException {
                            detail: format!(
                                "column \"{column}\" of collection \"{collection}\" does not \
                                 exist (row \"{identity}\")"
                            ),
                        },
                    );
                }
                Err(e) => return self.response_error(task, e),
            };

            // The text index follows what the tuple stores: fields the schema
            // does not declare leave the row, and their words leave the index.
            let Some(stored_msgpack) =
                super::super::strict_format::binary_tuple_to_msgpack(&tuple_bytes, &schema)
            else {
                let e = super::super::strict_format::undecodable_strict_row(
                    collection,
                    identity.as_str(),
                );
                return self.response_error(task, ErrorCode::from(e));
            };
            if let Err(e) = self.put_converted_row(
                ConvertedRow {
                    database_id,
                    tid,
                    collection,
                    doc_id,
                },
                &tuple_bytes,
                &stored_msgpack,
            ) {
                return self.response_error(task, ErrorCode::from(e));
            }
            // Write-through: a point-get after this statement must see the
            // re-encoded bytes, not a stale cache entry from before the
            // conversion. The row write alone never touches this cache.
            self.doc_cache
                .put(database_id, tid, collection, doc_id, &tuple_bytes);
            converted += 1;
        }

        tracing::info!(%collection, converted, "collection converted to document_strict");

        let result = serde_json::json!({
            "converted": converted,
            "target_type": "document_strict",
            "collection": collection,
        });
        match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Convert from strict mode to schemaless document or kv storage.
    ///
    /// A Binary Tuple source re-encodes to MessagePack against its own
    /// strict schema before the catalog flips. A schemaless source needs no
    /// re-encoding: the sparse engine already stores it as MessagePack.
    fn convert_from_strict(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        target_type: &str,
        source_format: SparseBodyFormat,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();

        let docs = match self
            .sparse
            .scan_documents(database_id, tid, collection, usize::MAX)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };

        let converted = match source_format {
            SparseBodyFormat::Strict(schema) => {
                let mut converted = 0u64;
                for (doc_id, doc_bytes) in &docs {
                    // Before decode, the row's own identity is unreadable —
                    // name it by the internal surrogate, the only handle a
                    // tuple that fails to decode has left.
                    let undecoded_identity = doc_id.to_identity();
                    let Some(mp) =
                        super::super::strict_format::binary_tuple_to_msgpack(doc_bytes, &schema)
                    else {
                        let e = super::super::strict_format::undecodable_strict_row(
                            collection,
                            undecoded_identity.as_str(),
                        );
                        return self.response_error(task, ErrorCode::from(e));
                    };
                    if let Err(e) = self.put_converted_row(
                        ConvertedRow {
                            database_id,
                            tid,
                            collection,
                            doc_id,
                        },
                        &mp,
                        &mp,
                    ) {
                        return self.response_error(task, ErrorCode::from(e));
                    }
                    // Write-through: a point-get after this statement must see
                    // the re-encoded bytes, not a stale cache entry from before
                    // the conversion. The row write alone never touches this cache.
                    self.doc_cache
                        .put(database_id, tid, collection, doc_id, &mp);
                    converted += 1;
                }
                converted
            }
            SparseBodyFormat::Document | SparseBodyFormat::VectorSidecar => docs.len() as u64,
        };

        let result = serde_json::json!({
            "converted": converted,
            "target_type": target_type,
            "collection": collection,
        });
        match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}

/// Where one converted row lands.
struct ConvertedRow<'a> {
    database_id: u64,
    tid: u64,
    collection: &'a str,
    doc_id: &'a nodedb_types::StorageKey,
}

impl CoreLoop {
    /// Write one converted row and re-index its text from the converted
    /// content, in one transaction. A conversion that drops fields also
    /// drops their words from the full-text index.
    ///
    /// `body` is the stored form; `msgpack` is the same row as MessagePack,
    /// which the text is extracted from.
    fn put_converted_row(
        &mut self,
        row: ConvertedRow<'_>,
        body: &[u8],
        msgpack: &[u8],
    ) -> crate::Result<()> {
        let new_doc = crate::data::executor::doc_format::decode_document(msgpack)?;
        let txn = self.sparse.begin_write()?;
        self.sparse.put_in_txn(
            &txn,
            row.database_id,
            row.tid,
            row.collection,
            row.doc_id,
            body,
        )?;
        self.update_reindex_text(
            &txn,
            super::point::update_reindex_text::UpdateTextReindex {
                database_id: row.database_id,
                tid: row.tid,
                collection: row.collection,
                surrogate: row.doc_id.surrogate(),
                new_doc: &new_doc,
            },
        )?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("convert commit: {e}"),
        })
    }
}
