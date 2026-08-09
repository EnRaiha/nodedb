// SPDX-License-Identifier: BUSL-1.1

//! Turning a bulk UPDATE's matched row set into the post-images it will store.
//!
//! Split out from the apply loop because the two answer different questions:
//! this decides WHAT each matched row becomes, and the loop decides when that
//! becomes durable. Separating them is what lets a statement-wide constraint be
//! judged before the first row is written — the apply loop commits one
//! transaction per row, so a check that ran while it iterated could only report
//! a violation the rows ahead of it had already made durable.
//!
//! Projecting a row is pure: it reads the stored body and computes the new one,
//! writing nothing.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::columnar::StrictSchema;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::types::{DatabaseId, TenantId};

/// One matched row and everything the apply loop needs to land it.
pub(super) struct ProjectedUpdateRow {
    /// Storage key (the surrogate hex).
    pub(super) doc_id: String,
    /// The row as stored before the update — the `old_value` of the emitted
    /// event and the old side of the secondary-index diff.
    pub(super) current_bytes: Vec<u8>,
    /// Pre-mutation image, captured before any field changed.
    pub(super) old_doc: serde_json::Value,
    /// Post-update image, with assignments and regenerated columns applied.
    pub(super) doc: serde_json::Value,
    /// The post-update image encoded in the collection's storage mode.
    pub(super) updated_bytes: Vec<u8>,
}

/// Inputs to [`CoreLoop::project_bulk_update_rows`].
pub(super) struct ProjectUpdateRows<'a> {
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    /// The settled apply set, in statement order.
    pub(super) doc_ids: &'a [String],
    pub(super) updates: &'a [(String, UpdateValue)],
    /// `Some` for a strict collection, whose bodies are Binary Tuples.
    pub(super) strict_schema: Option<&'a StrictSchema>,
}

impl CoreLoop {
    /// Compute the post-image of every matched row.
    ///
    /// A row that is gone, will not decode under its collection's storage mode,
    /// or cannot be re-encoded is skipped — the same rows the apply loop always
    /// skipped, and for the same reasons. A failure that means the STATEMENT is
    /// wrong (a body that will not decode as MessagePack on a schemaless
    /// collection, an assignment expression that cannot be evaluated) is an
    /// error, because silently skipping it would report a smaller affected
    /// count as the truth.
    pub(super) fn project_bulk_update_rows(
        &self,
        p: ProjectUpdateRows<'_>,
    ) -> crate::Result<Vec<ProjectedUpdateRow>> {
        let ProjectUpdateRows {
            database_id,
            tid,
            collection,
            doc_ids,
            updates,
            strict_schema,
        } = p;
        let config_key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );

        let mut projected = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            let Some(current_bytes) = self.sparse.get(database_id, tid, collection, doc_id)? else {
                continue;
            };

            // Decode current value — format depends on storage mode.
            let mut doc = match strict_schema {
                Some(schema) => {
                    match crate::data::executor::strict_format::binary_tuple_to_json(
                        &current_bytes,
                        schema,
                    ) {
                        Some(v) => v,
                        None => continue,
                    }
                }
                // A row skipped here is one the UPDATE silently leaves
                // untouched while reporting a smaller affected count as the
                // truth, so it fails the statement instead.
                None => doc_format::decode_document(&current_bytes)?,
            };

            // Pre-mutation image, captured before any field is changed. Feeds
            // the secondary-index SET diff so values the UPDATE drops are
            // removed from the index atomically with the write.
            let old_doc = doc.clone();
            // Snapshot the current row for expression evaluation. All
            // expression assignments see the pre-update state — multiple
            // assignments in the same UPDATE do not observe each other,
            // matching PostgreSQL semantics.
            let eval_doc: nodedb_types::Value = doc.clone().into();
            if let Some(obj) = doc.as_object_mut() {
                for (field, update_val) in updates {
                    let val: serde_json::Value = match update_val {
                        UpdateValue::Literal(bytes) => match nodedb_types::json_from_msgpack(bytes)
                        {
                            Ok(v) => v,
                            Err(_) => continue,
                        },
                        UpdateValue::Expr(expr) => {
                            // A division/modulo-by-zero in an UPDATE assignment
                            // fails the whole statement, unlike the
                            // literal-decode failure above which skips just that
                            // field.
                            let result: nodedb_types::Value = expr.eval(&eval_doc)?;
                            result.into()
                        }
                    };
                    obj.insert(field.clone(), val);
                }
            }

            // Recompute generated columns if any dependency changed.
            if let Some(config) = self.doc_configs.get(&config_key)
                && !config.enforcement.generated_columns.is_empty()
                && super::super::generated::needs_recomputation(
                    updates,
                    &config.enforcement.generated_columns,
                )
                && let Err(e) = super::super::generated::evaluate_generated_columns(
                    &mut doc,
                    &config.enforcement.generated_columns,
                )
            {
                tracing::warn!(
                    %doc_id,
                    error = ?e,
                    "generated column recomputation failed, skipping document"
                );
                continue;
            }

            // Re-encode — format depends on storage mode.
            let updated_bytes = match strict_schema {
                Some(schema) => {
                    let ndb_val: nodedb_types::Value = doc.clone().into();
                    match crate::data::executor::strict_format::value_to_binary_tuple(
                        &ndb_val, schema,
                    ) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            tracing::warn!(
                                %doc_id,
                                error = %e,
                                "strict re-encode failed, skipping document"
                            );
                            continue;
                        }
                    }
                }
                None => doc_format::encode_to_msgpack(&doc),
            };

            projected.push(ProjectedUpdateRow {
                doc_id: doc_id.clone(),
                current_bytes,
                old_doc,
                doc,
                updated_bytes,
            });
        }
        Ok(projected)
    }
}
