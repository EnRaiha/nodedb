// SPDX-License-Identifier: BUSL-1.1

//! Statement-time row-level-security enforcement for the columnar family's
//! staged writes.
//!
//! Staging is where an in-transaction statement's row image is produced and
//! made visible, so it is where the write policy has to decide it — the same
//! rule the document and key-value staging paths follow. COMMIT replay gates
//! the durable apply as well, and that stays: this is defence in depth, not a
//! replacement. What it adds is that a statement whose image the policy refuses
//! fails AT THE STATEMENT rather than reporting `{"affected": N}` and exposing
//! the refused row to the transaction's own reads until COMMIT fails.
//!
//! Every helper here decides the WHOLE set before the caller mutates any
//! overlay, so a refusal leaves the overlay exactly as it was.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::value::Value;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::{ndb_field_to_value, row_values_to_object};
use crate::data::executor::handlers::rls_write_gate::{admit_columnar_row, admit_value_row};
use crate::data::executor::handlers::upsert::apply_on_conflict_updates;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// Decide a set of schema-ordered columnar rows against the compiled write
    /// policy. Empty `rls_write_check` admits everything.
    pub(super) fn stage_admit_columnar_rows<'a>(
        &self,
        task: &ExecutionTask,
        rls_write_check: &[u8],
        rows: impl IntoIterator<Item = &'a [Value]>,
        schema: &ColumnarSchema,
        tid: u64,
        collection: &str,
    ) -> Result<(), Response> {
        if rls_write_check.is_empty() {
            return Ok(());
        }
        for row in rows {
            if let Err(error) = admit_columnar_row(rls_write_check, row, schema, tid, collection) {
                return Err(self.response_error(task, error));
            }
        }
        Ok(())
    }

    /// Decide a set of row objects against the compiled write policy, for the
    /// timeseries staging paths whose rows are field maps rather than
    /// schema-ordered vectors.
    pub(super) fn stage_admit_value_rows<'a>(
        &self,
        task: &ExecutionTask,
        rls_write_check: &[u8],
        rows: impl IntoIterator<Item = &'a Value>,
        tid: u64,
        collection: &str,
    ) -> Result<(), Response> {
        if rls_write_check.is_empty() {
            return Ok(());
        }
        for row in rows {
            if let Err(error) = admit_value_row(rls_write_check, row, tid, collection) {
                return Err(self.response_error(task, error));
            }
        }
        Ok(())
    }

    /// The row image a staged `ColumnarOp::Insert` will ultimately persist.
    ///
    /// For a plain insert that is the coerced incoming row. For the ON CONFLICT
    /// DO UPDATE shape it is the stored row with the assignments applied —
    /// resolved here exactly as `insert_columnar_rows` resolves it on the
    /// durable path, so the policy decides the row that will exist rather than
    /// the body that was submitted. Deciding the submitted body instead would
    /// refuse `ON CONFLICT DO UPDATE SET owner = <me>` on someone else's row,
    /// which the merge makes legal.
    ///
    /// `None` is impossible to distinguish from "row will be inserted as-is",
    /// so a missing prior row yields `values` unchanged, matching the durable
    /// path.
    pub(super) fn staged_columnar_write_image(
        &self,
        engine_key: &(nodedb_types::DatabaseId, TenantId, String),
        schema: &ColumnarSchema,
        values: Vec<Value>,
        on_conflict_updates: &[(String, UpdateValue)],
    ) -> Result<Vec<Value>, ErrorCode> {
        if on_conflict_updates.is_empty() {
            return Ok(values);
        }
        let Some(engine) = self.columnar_engines.get(engine_key) else {
            return Ok(values);
        };
        let pk_bytes = engine
            .encode_pk_from_row(&values)
            .map_err(|e| ErrorCode::Internal {
                detail: format!("columnar insert: pk encode failed: {e}"),
            })?;
        let prior = engine
            .lookup_memtable_row_by_pk(&pk_bytes)
            .or_else(|| self.read_flushed_row_by_pk(engine_key, &pk_bytes));
        let Some(prior) = prior else {
            return Ok(values);
        };

        let existing = row_values_to_object(schema, &prior);
        let excluded = row_values_to_object(schema, &values);
        let merged = apply_on_conflict_updates(existing, &excluded, on_conflict_updates)
            .map_err(ErrorCode::from)?;
        let Value::Object(merged) = merged else {
            return Err(ErrorCode::Internal {
                detail: "merged ON CONFLICT value was not an object".into(),
            });
        };
        schema
            .columns
            .iter()
            .map(|col| ndb_field_to_value(merged.get(&col.name), &col.column_type))
            .collect::<Result<Vec<Value>, crate::Error>>()
            .map_err(|e| ErrorCode::Internal {
                detail: format!("columnar ON CONFLICT coercion: {e}"),
            })
    }
}
