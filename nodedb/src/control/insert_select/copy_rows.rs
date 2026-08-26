// SPDX-License-Identifier: BUSL-1.1

//! Shared row-copy machinery for `INSERT ... SELECT`, used by BOTH the
//! autocommit orchestrator ([`crate::control::insert_select::run_insert_select`])
//! and the statement-time staged expander
//! ([`crate::control::insert_select::expand_staged`]).
//!
//! Both drive the same pipeline: scan the source page-by-page, apply the
//! residual `WHERE` filter, assign a fresh catalog-registered surrogate keyed on
//! the TARGET collection's primary key, and emit the concrete
//! `(target_doc_id, value, surrogate)` to write.
//!
//! ## Where the bodies are normalized
//!
//! Every downstream step here operates on standard msgpack: the `WHERE` filter
//! ([`ScanFilter::all_match_binary`]), primary-key extraction inside
//! [`assign_target_surrogate`], and the target write handler (which decodes
//! msgpack and re-encodes to the target's format). A Binary Tuple from a strict
//! source would silently corrupt all three.
//!
//! `MaterializeScan` therefore normalizes each body on the Data Plane, where the
//! source collection's encoding is already known, and every consumer of a scan
//! page — this one included — receives standard msgpack. Do NOT re-add a
//! Control-Plane decode here: a second copy of that decision is what left the
//! trap armed for the next consumer.

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::bridge::scan_filter::ScanFilter;
use crate::control::state::SharedState;
use crate::control::target_identity::{
    TargetPk, assign_target_surrogate, bare_collection_name, resolve_target_pk,
};
use crate::engine::document::store::surrogate_to_doc_id;

/// Resolved, per-statement copy context shared across every scanned page.
pub(crate) struct CopySpec {
    /// How the TARGET collection's primary key maps a copied row to a surrogate.
    pub target_pk: TargetPk,
    /// Residual source `WHERE` predicate (deserialized `Vec<ScanFilter>`).
    pub filters: Vec<ScanFilter>,
}

/// Resolve the target PK and the residual source `WHERE` filter for one
/// `INSERT ... SELECT` statement.
///
/// `target_collection` is the db-qualified name as it appears in the
/// `DocumentOp::InsertSelect` plan.
pub(crate) fn resolve_copy_spec(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &str,
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

    let filters: Vec<ScanFilter> = if source_filters.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(source_filters).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("insert-select source filters: {e}"),
        })?
    };

    Ok(CopySpec { target_pk, filters })
}

/// Filter and assign fresh surrogates for one scanned source page.
///
/// `entries` are the `(doc_id, surrogate, value)` triples from
/// `scan_source_page`, whose bodies are already standard msgpack. `remaining`
/// bounds the total copied-row count across pages (the SELECT `LIMIT`) and is
/// decremented per emitted row. Returns the concrete
/// `(target_doc_id, msgpack_value, fresh_surrogate)` to write.
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
    for (_source_doc_id, _source_surrogate, value) in entries {
        if *remaining == 0 {
            break;
        }
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
