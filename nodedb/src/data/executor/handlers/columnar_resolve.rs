// SPDX-License-Identifier: BUSL-1.1

//! Shared row-selection and assignment logic for a columnar UPDATE/DELETE:
//! which memtable rows match the WHERE filters, and — for an UPDATE — what
//! their post-image is once the assignments are applied.
//!
//! `execute_columnar_update` / `execute_columnar_delete`
//! (`columnar_mutation.rs`) call this to select the rows they then mutate.
//! `execute_columnar_resolve_dml` (`columnar_resolve_dml.rs`, backing
//! `ColumnarOp::ResolveDml`) calls the same functions to report the identical
//! selection to the Control Plane without mutating anything. One
//! implementation of "which rows match and what do they become" decides both
//! what a predicate DML writes and what it is reported to write — so the two
//! can never drift into deciding the write policy against different images.

use nodedb_types::Value;
use nodedb_types::columnar::ColumnarSchema;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::handlers::columnar_read::filter::row_matches_filters;
use crate::data::executor::handlers::rls_write_gate::admit_columnar_row;

/// Column index of the schema's primary-key column, or the standard
/// "columnar UPDATE/DELETE requires a PRIMARY KEY column" error every
/// columnar mutation path returns for a PK-less schema.
pub(in crate::data::executor) fn require_pk_column_index(
    schema: &ColumnarSchema,
    op_name: &str,
) -> crate::Result<usize> {
    schema
        .columns
        .iter()
        .position(|c| c.primary_key)
        .ok_or_else(|| crate::Error::Internal {
            detail: format!("columnar {op_name} requires a PRIMARY KEY column"),
        })
}

/// Bundled arguments for [`resolve_update_rows`].
pub(in crate::data::executor) struct ResolveUpdateRowsParams<'a> {
    pub engine: &'a nodedb_columnar::MutationEngine,
    pub schema: &'a ColumnarSchema,
    pub pk_col_idx: usize,
    pub filter_predicates: &'a [ScanFilter],
    pub updates: &'a [(String, Vec<u8>)],
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    pub tid: u64,
    pub collection: &'a str,
}

/// Match memtable rows against `filter_predicates`, apply `updates` to build
/// each match's post-image, and decide every post-image against
/// `rls_write_check` — fail-fast: the first rejected row is the whole
/// statement's error, before the remaining rows are even resolved.
///
/// Mutates nothing. Returns `(old_primary_key, post_image)` pairs in match
/// order. `old_primary_key` is the row's PK column value BEFORE `updates` is
/// applied — the value that identifies the row to remove even when the
/// update assigns the PK column a new value, exactly as
/// `execute_columnar_update` extracts it today.
pub(in crate::data::executor) fn resolve_update_rows(
    params: ResolveUpdateRowsParams<'_>,
) -> crate::Result<Vec<(Value, Vec<Value>)>> {
    let ResolveUpdateRowsParams {
        engine,
        schema,
        pk_col_idx,
        filter_predicates,
        updates,
        rls_write_check,
        tid,
        collection,
    } = params;
    let mut resolved = Vec::new();
    for row in engine.scan_memtable_rows() {
        if !filter_predicates.is_empty() {
            match row_matches_filters(&row, schema, filter_predicates) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_) => return Err(crate::Error::DivisionByZero),
            }
        }

        let old_pk = row[pk_col_idx].clone();
        let mut new_row = row;
        for (field_name, value_bytes) in updates {
            if let Some(col_idx) = schema.columns.iter().position(|c| c.name == *field_name) {
                let typed_val = nodedb_types::value_from_msgpack(value_bytes).map_err(|e| {
                    crate::Error::Internal {
                        detail: format!(
                            "failed to decode update value for field '{field_name}': {e}"
                        ),
                    }
                })?;
                new_row[col_idx] = typed_val;
            }
        }

        admit_columnar_row(rls_write_check, &new_row, schema, tid, collection)?;
        resolved.push((old_pk, new_row));
    }
    Ok(resolved)
}

/// Match memtable rows against `filter_predicates` and decide each matched
/// row's pre-image — the image a DELETE removes — against
/// `rls_write_check`. Mutates nothing. Returns matched primary-key values in
/// match order.
pub(in crate::data::executor) fn resolve_delete_rows(
    engine: &nodedb_columnar::MutationEngine,
    schema: &ColumnarSchema,
    pk_col_idx: usize,
    filter_predicates: &[ScanFilter],
    rls_write_check: &nodedb_types::RlsWriteCheck,
    tid: u64,
    collection: &str,
) -> crate::Result<Vec<Value>> {
    let mut pks = Vec::new();
    for row in engine.scan_memtable_rows() {
        if !filter_predicates.is_empty() {
            match row_matches_filters(&row, schema, filter_predicates) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_) => return Err(crate::Error::DivisionByZero),
            }
        }
        admit_columnar_row(rls_write_check, &row, schema, tid, collection)?;
        pks.push(row[pk_col_idx].clone());
    }
    Ok(pks)
}
