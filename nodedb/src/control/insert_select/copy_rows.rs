// SPDX-License-Identifier: BUSL-1.1

//! Shared row-copy machinery for `INSERT ... SELECT`, used by BOTH the
//! autocommit orchestrator ([`crate::control::insert_select::run_insert_select`])
//! and the statement-time staged expander
//! ([`crate::control::insert_select::expand_staged`]).
//!
//! Both drive the same pipeline: scan the source page-by-page, NORMALIZE each
//! scanned row to standard msgpack, apply the residual `WHERE` filter, assign a
//! fresh catalog-registered surrogate keyed on the TARGET collection's primary
//! key, and emit the concrete `(target_doc_id, value, surrogate)` to write.
//!
//! ## Why normalization is mandatory
//!
//! `scan_source_page` returns each source row's RAW stored bytes. For a STRICT
//! document source those bytes are a Binary Tuple, not msgpack — but every
//! downstream step operates on standard msgpack: the `WHERE` filter
//! ([`ScanFilter::matches_binary`]), primary-key extraction inside
//! [`assign_target_surrogate`], and the target write handler (which decodes
//! msgpack and re-encodes to the target's format). Copying the raw tuple bytes
//! through unchanged silently corrupts all three. This module decodes strict
//! rows to msgpack via the SAME `binary_tuple_to_msgpack` decoder the Data
//! Plane's universal scan uses, so both copy paths behave exactly as a plain
//! `SELECT` would. Schemaless sources are already msgpack and pass through.

use nodedb_types::columnar::StrictSchema;
use nodedb_types::{CollectionType, DatabaseId, Surrogate, TenantId};

use crate::bridge::scan_filter::ScanFilter;
use crate::control::state::SharedState;
use crate::control::target_identity::{
    TargetPk, assign_target_surrogate, bare_collection_name, resolve_target_pk,
};
use crate::data::executor::strict_format::binary_tuple_to_msgpack;
use crate::engine::document::store::surrogate_to_doc_id;

/// Resolved, per-statement copy context shared across every scanned page.
pub(crate) struct CopySpec {
    /// How the TARGET collection's primary key maps a copied row to a surrogate.
    pub target_pk: TargetPk,
    /// Residual source `WHERE` predicate (deserialized `Vec<ScanFilter>`).
    pub filters: Vec<ScanFilter>,
    /// SOURCE strict schema when the source is a strict document collection;
    /// `None` for schemaless sources (whose stored bytes are already msgpack).
    /// Used to decode each scanned Binary Tuple to msgpack before filtering,
    /// PK extraction, and insertion.
    pub source_strict_schema: Option<StrictSchema>,
}

/// Resolve the target PK, the source strict schema (if any), and the residual
/// source `WHERE` filter for one `INSERT ... SELECT` statement.
///
/// `target_collection` / `source_collection` are the db-qualified names as they
/// appear in the `DocumentOp::InsertSelect` plan.
pub(crate) fn resolve_copy_spec(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &str,
    source_collection: &str,
    source_filters: &[u8],
) -> crate::Result<CopySpec> {
    let catalog = state.credentials.catalog();

    let target = catalog
        .get_collection(
            database_id,
            tenant_id.as_u64(),
            &bare_collection_name(database_id, target_collection),
        )?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id,
            collection: target_collection.to_string(),
        })?;
    let target_pk = resolve_target_pk(&target, "INSERT ... SELECT")?;

    let source_strict_schema = catalog
        .get_collection(
            database_id,
            tenant_id.as_u64(),
            &bare_collection_name(database_id, source_collection),
        )?
        .and_then(|s| match &s.collection_type {
            CollectionType::Document(mode) => mode.schema().cloned(),
            CollectionType::Columnar(_) | CollectionType::KeyValue(_) => None,
        });

    let filters: Vec<ScanFilter> = if source_filters.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(source_filters).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("insert-select source filters: {e}"),
        })?
    };

    Ok(CopySpec {
        target_pk,
        filters,
        source_strict_schema,
    })
}

/// Normalize, filter, and assign fresh surrogates for one scanned source page.
///
/// `entries` are raw `(doc_id, surrogate, value)` triples straight from
/// `scan_source_page`. For a strict source `value` is a Binary Tuple, decoded to
/// msgpack via [`CopySpec::source_strict_schema`] BEFORE filtering / PK
/// extraction / insertion. `remaining` bounds the total copied-row count across
/// pages (the SELECT `LIMIT`) and is decremented per emitted row. Returns the
/// concrete `(target_doc_id, msgpack_value, fresh_surrogate)` to write.
pub(crate) fn assign_page_rows(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &str,
    spec: &CopySpec,
    entries: Vec<(String, u32, Vec<u8>)>,
    remaining: &mut usize,
) -> crate::Result<Vec<(String, Vec<u8>, Surrogate)>> {
    let mut out = Vec::with_capacity(entries.len());
    for (_source_doc_id, _source_surrogate, raw) in entries {
        if *remaining == 0 {
            break;
        }
        // Strict source → decode Binary Tuple to msgpack. `None` means the bytes
        // were already msgpack (or not a decodable tuple), so pass them through.
        let value = match spec.source_strict_schema.as_ref() {
            Some(schema) => binary_tuple_to_msgpack(&raw, schema).unwrap_or(raw),
            None => raw,
        };
        if !spec.filters.is_empty()
            && !crate::bridge::scan_filter::ScanFilter::all_match_binary(&spec.filters, &value)?
        {
            continue;
        }
        let surrogate = assign_target_surrogate(
            state,
            database_id,
            tenant_id,
            target_collection,
            &spec.target_pk,
            &value,
        )?;
        out.push((surrogate_to_doc_id(surrogate), value, surrogate));
        *remaining -= 1;
    }
    Ok(out)
}
