// SPDX-License-Identifier: BUSL-1.1

//! Reading one collection's index registrations out of the live engine and into
//! the on-disk shape.

use super::index_format::{
    KvCheckpointCompositeIndex, KvCheckpointFieldIndex, KvCheckpointIndexEntry,
    KvCheckpointIndexes, KvCheckpointSortColumn, KvCheckpointSortedEntry, KvCheckpointSortedIndex,
};
use crate::engine::kv::KvEngine;
use crate::engine::kv::index::{KvCompositeIndex, KvFieldIndex, KvIndexTree};
use crate::engine::kv::sorted_index::key::SortDirection;
use crate::engine::kv::sorted_index::window::WindowType;

/// Every index registered on `table_key`, with its content.
pub(super) fn export_collection_indexes(engine: &KvEngine, table_key: u64) -> KvCheckpointIndexes {
    let (fields, composites) = match engine.index_set(table_key) {
        Some(set) => (
            set.field_indexes().iter().map(export_field_index).collect(),
            set.composite_indexes()
                .iter()
                .map(export_composite_index)
                .collect(),
        ),
        None => (Vec::new(), Vec::new()),
    };

    let sorted = engine
        .sorted_index_snapshots(table_key)
        .into_iter()
        .map(|snapshot| {
            let def = snapshot.def;
            let (window_type, window_start_ms, window_end_ms) =
                export_window_type(&def.window.window_type);
            KvCheckpointSortedIndex {
                name: def.name.clone(),
                collection: def.collection.clone(),
                key_column: def.key_column.clone(),
                sort_columns: def
                    .encoder
                    .columns()
                    .iter()
                    .map(|col| KvCheckpointSortColumn {
                        name: col.name.clone(),
                        direction: export_direction(col.direction).to_string(),
                    })
                    .collect(),
                window_type: window_type.to_string(),
                window_timestamp_column: def.window.timestamp_column.clone(),
                window_start_ms,
                window_end_ms,
                entries: snapshot
                    .entries
                    .into_iter()
                    .map(|(sort_key, primary_key)| KvCheckpointSortedEntry {
                        sort_key,
                        primary_key,
                    })
                    .collect(),
            }
        })
        .collect();

    KvCheckpointIndexes {
        fields,
        composites,
        sorted,
    }
}

fn export_field_index(index: &KvFieldIndex) -> KvCheckpointFieldIndex {
    KvCheckpointFieldIndex {
        field: index.field().to_string(),
        // Widened, never narrowed: `usize` is at most 64 bits on every target
        // this runs on, so the file keeps the exact position.
        field_position: index.field_position() as u64,
        entries: export_entries(index.entries()),
    }
}

fn export_composite_index(index: &KvCompositeIndex) -> KvCheckpointCompositeIndex {
    KvCheckpointCompositeIndex {
        fields: index.fields().to_vec(),
        field_positions: index.field_positions().iter().map(|p| *p as u64).collect(),
        entries: export_entries(index.entries()),
    }
}

fn export_entries(tree: &KvIndexTree) -> Vec<KvCheckpointIndexEntry> {
    tree.iter()
        .map(|(key, primary_keys)| KvCheckpointIndexEntry {
            key: key.clone(),
            primary_keys: primary_keys.iter().cloned().collect(),
        })
        .collect()
}

/// The spelling `build_sorted_index_def` parses back, so the export and the
/// rebuild are inverses of each other by construction.
fn export_direction(direction: SortDirection) -> &'static str {
    match direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    }
}

/// Split a window into the `(type, start_ms, end_ms)` triple the record format
/// carries. Only a custom window has bounds; the rest derive theirs from the
/// clock at query time.
fn export_window_type(window_type: &WindowType) -> (&'static str, u64, u64) {
    match window_type {
        WindowType::None => ("", 0, 0),
        WindowType::Daily => ("DAILY", 0, 0),
        WindowType::Weekly => ("WEEKLY", 0, 0),
        WindowType::Monthly => ("MONTHLY", 0, 0),
        WindowType::Custom { start_ms, end_ms } => ("CUSTOM", *start_ms, *end_ms),
    }
}
