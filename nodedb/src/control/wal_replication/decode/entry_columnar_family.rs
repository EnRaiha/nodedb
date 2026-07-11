// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Columnar` / `Timeseries` / `Text` / `Spatial` — the
//! columnar-storage-family engines plus their overlay sync engines (FTS,
//! spatial). The sync-engine ingest/index arms delegate to
//! [`super::super::decode_sync_engines`]; the columnar predicate-DML arm
//! delegates to [`super::columnar`].
//!
//! Delegated from `decode/entry.rs`'s single grouped match arm. None of these
//! arms bind surrogates, so this group takes no [`DecodeCtx`]. `write` is
//! guaranteed by the caller to already be one of these variants — see
//! `entry_document::decode_arm` for the trailing-arm contract.
//!
//! [`DecodeCtx`]: super::ctx::DecodeCtx

use super::super::decode_sync_engines;
use super::super::types::ReplicatedWrite;
use super::columnar;
use crate::bridge::envelope::PhysicalPlan;

pub(super) fn decode_arm(write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::ColumnarIngest {
            collection,
            payload,
            schema_bytes,
            surrogates,
            provenance,
        } => decode_sync_engines::columnar_ingest(
            collection,
            payload,
            schema_bytes,
            surrogates,
            provenance,
        ),
        ReplicatedWrite::TimeseriesIngest {
            collection,
            payload,
            format,
            surrogates,
            provenance,
        } => decode_sync_engines::timeseries_ingest(
            collection, payload, format, surrogates, provenance,
        ),
        ReplicatedWrite::FtsIndex {
            collection,
            surrogate,
            text,
            provenance,
        } => decode_sync_engines::fts_index(collection, *surrogate, text, provenance),
        ReplicatedWrite::FtsDelete {
            collection,
            surrogate,
            provenance,
        } => decode_sync_engines::fts_delete(collection, *surrogate, provenance),
        ReplicatedWrite::SpatialInsert {
            collection,
            field,
            surrogate,
            geometry_bytes,
            provenance,
        } => decode_sync_engines::spatial_insert(
            collection,
            field,
            *surrogate,
            geometry_bytes,
            provenance,
        ),
        ReplicatedWrite::SpatialDelete {
            collection,
            field,
            surrogate,
            provenance,
        } => decode_sync_engines::spatial_delete(collection, field, *surrogate, provenance),
        ReplicatedWrite::ColumnarBulkDml {
            collection,
            filters,
            is_update,
            updates,
        } => Ok(columnar::bulk_dml(collection, filters, *is_update, updates)),
        _ => Err(crate::Error::Internal {
            detail: "entry_columnar_family::decode_arm called with a non-columnar-family \
                ReplicatedWrite variant (dispatch bug in decode/entry.rs's grouped \
                columnar-family match arm)"
                .into(),
        }),
    }
}
