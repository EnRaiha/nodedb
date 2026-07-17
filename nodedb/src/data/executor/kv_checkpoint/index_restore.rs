// SPDX-License-Identifier: BUSL-1.1

//! Installing a decoded generation's index registrations into the engine.
//!
//! Runs AFTER the collection's rows are back. That order is load-bearing in both
//! directions:
//!
//! * Rows must not be replayed through `put` while registrations are live, or
//!   PUT-driven maintenance would derive index entries alongside the exported
//!   ones — and the derived entries are not the same set. A `backfill=false`
//!   index deliberately omits the rows that predate it, and the engine's two
//!   sort-key extraction paths disagree on some column types.
//! * Nothing is installed until the whole generation has decoded, so a
//!   collection can never come back holding rows whose registrations failed to
//!   rebuild.

use super::index_decode::DecodedKvIndexes;
use crate::engine::kv::{KvEngine, RestoreCompositeIndexParams, RestoreFieldIndexParams};

/// Reinstate every registration on one collection, content included.
pub(super) fn restore_collection_indexes(
    engine: &mut KvEngine,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    indexes: &DecodedKvIndexes,
) {
    for field_index in &indexes.fields {
        engine.restore_field_index(RestoreFieldIndexParams {
            database_id,
            tenant_id,
            collection,
            field: &field_index.field,
            field_position: field_index.field_position,
            entries: &field_index.entries,
        });
    }

    for composite in &indexes.composites {
        engine.restore_composite_index(RestoreCompositeIndexParams {
            database_id,
            tenant_id,
            collection,
            fields: &composite.fields,
            field_positions: &composite.field_positions,
            entries: &composite.entries,
        });
    }

    for sorted in &indexes.sorted {
        engine.restore_sorted_index(database_id, tenant_id, sorted.def.clone(), &sorted.entries);
    }
}
