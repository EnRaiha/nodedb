// SPDX-License-Identifier: BUSL-1.1

//! Turning a generation's on-disk index registrations into the engine types the
//! restore installs.
//!
//! This runs in the DECODE phase, before a single row or registration is
//! installed, because it is the only fallible step: an unknown sort direction or
//! window type, or a field position that does not fit this machine's `usize`,
//! means the file is corrupt. Failing here costs a full WAL replay; failing
//! halfway through the install would leave a collection holding some of its
//! registrations under a floor that suppresses the records which would have
//! restored the rest.

use super::index_format::{KvCheckpointIndexEntry, KvCheckpointIndexes};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::handlers::kv::sorted_index_compute::{
    BuildSortedIndexDefParams, build_sorted_index_def,
};
use crate::engine::kv::sorted_index::manager::SortedIndexDef;

/// Flattened index content: `(index_key, primary_key)` pairs.
pub(super) type DecodedIndexEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// A single-field secondary index ready to install.
#[derive(Debug)]
pub(super) struct DecodedFieldIndex {
    pub field: String,
    pub field_position: usize,
    pub entries: DecodedIndexEntries,
}

/// A composite secondary index ready to install.
#[derive(Debug)]
pub(super) struct DecodedCompositeIndex {
    pub fields: Vec<String>,
    pub field_positions: Vec<usize>,
    pub entries: DecodedIndexEntries,
}

/// A sorted index ready to install: the rebuilt definition and its tree.
#[derive(Debug)]
pub(super) struct DecodedSortedIndex {
    pub def: SortedIndexDef,
    /// `(sort_key, primary_key)` pairs.
    pub entries: DecodedIndexEntries,
}

/// Every index registration on one collection, ready to install.
#[derive(Debug)]
pub(super) struct DecodedKvIndexes {
    pub fields: Vec<DecodedFieldIndex>,
    pub composites: Vec<DecodedCompositeIndex>,
    pub sorted: Vec<DecodedSortedIndex>,
}

/// Convert one collection's on-disk registrations. `Err` for any value outside
/// the closed set the writer emits — the caller then restores nothing.
pub(super) fn decode_kv_indexes(
    raw: &KvCheckpointIndexes,
) -> Result<DecodedKvIndexes, CheckpointDecodeError> {
    let mut fields = Vec::with_capacity(raw.fields.len());
    for f in &raw.fields {
        fields.push(DecodedFieldIndex {
            field: f.field.clone(),
            field_position: decode_field_position(f.field_position, &f.field)?,
            entries: flatten_entries(&f.entries),
        });
    }

    let mut composites = Vec::with_capacity(raw.composites.len());
    for c in &raw.composites {
        if c.field_positions.len() != c.fields.len() {
            return Err(CheckpointDecodeError::CompositeFieldPositionMismatch {
                fields: c.fields.clone(),
                positions: c.field_positions.len(),
                field_count: c.fields.len(),
            });
        }
        let mut field_positions = Vec::with_capacity(c.field_positions.len());
        for (pos, name) in c.field_positions.iter().zip(&c.fields) {
            field_positions.push(decode_field_position(*pos, name)?);
        }
        composites.push(DecodedCompositeIndex {
            fields: c.fields.clone(),
            field_positions,
            entries: flatten_entries(&c.entries),
        });
    }

    let mut sorted = Vec::with_capacity(raw.sorted.len());
    for s in &raw.sorted {
        let mut sort_columns = Vec::with_capacity(s.sort_columns.len());
        for col in &s.sort_columns {
            // Validated against the closed set the exporter writes rather than
            // defaulted: `build_sorted_index_def` reads anything that is not
            // "DESC" as ascending, so a corrupted direction would silently
            // invert the index instead of failing.
            if !matches!(col.direction.as_str(), "ASC" | "DESC") {
                return Err(CheckpointDecodeError::UnknownSortDirection {
                    index: s.name.clone(),
                    column: col.name.clone(),
                    direction: col.direction.clone(),
                });
            }
            sort_columns.push((col.name.clone(), col.direction.clone()));
        }
        // Same reason: an unknown window type reads as unwindowed, which would
        // silently widen the index to every entry ever written to it.
        if !matches!(
            s.window_type.as_str(),
            "" | "DAILY" | "WEEKLY" | "MONTHLY" | "CUSTOM"
        ) {
            return Err(CheckpointDecodeError::UnknownWindowType {
                index: s.name.clone(),
                window_type: s.window_type.clone(),
            });
        }

        // Rebuilt through the same builder the live registration and the WAL
        // replay use, so a restored def can never diverge from one that was
        // registered normally.
        let def = build_sorted_index_def(BuildSortedIndexDefParams {
            collection: &s.collection,
            index_name: &s.name,
            sort_columns: &sort_columns,
            key_column: &s.key_column,
            window_type: &s.window_type,
            window_timestamp_column: &s.window_timestamp_column,
            window_start_ms: s.window_start_ms,
            window_end_ms: s.window_end_ms,
        })
        .map_err(|code| CheckpointDecodeError::SortedIndexNotRebuildable {
            index: s.name.clone(),
            code: Box::new(code),
        })?;

        sorted.push(DecodedSortedIndex {
            def,
            entries: s
                .entries
                .iter()
                .map(|e| (e.sort_key.clone(), e.primary_key.clone()))
                .collect(),
        });
    }

    Ok(DecodedKvIndexes {
        fields,
        composites,
        sorted,
    })
}

/// Narrow an on-disk field position to this machine's `usize`.
///
/// The file holds `u64` so it does not encode the writer's pointer width; a
/// value that does not fit is a corrupt file, not something to clamp.
fn decode_field_position(position: u64, field: &str) -> Result<usize, CheckpointDecodeError> {
    usize::try_from(position).map_err(|_| CheckpointDecodeError::FieldPositionOutOfRange {
        field: field.to_string(),
        position,
    })
}

/// Expand `(key, [primary_key])` buckets into flat `(key, primary_key)` pairs.
fn flatten_entries(entries: &[KvCheckpointIndexEntry]) -> DecodedIndexEntries {
    entries
        .iter()
        .flat_map(|e| e.primary_keys.iter().map(|pk| (e.key.clone(), pk.clone())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::index_format::{
        KvCheckpointFieldIndex, KvCheckpointSortColumn, KvCheckpointSortedIndex,
    };
    use super::*;

    fn sorted_dto(direction: &str, window_type: &str) -> KvCheckpointSortedIndex {
        KvCheckpointSortedIndex {
            name: "lb".into(),
            collection: "scores".into(),
            key_column: "player_id".into(),
            sort_columns: vec![KvCheckpointSortColumn {
                name: "score".into(),
                direction: direction.into(),
            }],
            window_type: window_type.into(),
            window_timestamp_column: String::new(),
            window_start_ms: 0,
            window_end_ms: 0,
            entries: Vec::new(),
        }
    }

    /// An unknown direction must fail the whole generation. `build_sorted_index_def`
    /// treats every non-"DESC" spelling as ascending, so accepting one would
    /// silently invert a leaderboard rather than fall back to a WAL replay.
    #[test]
    fn unknown_sort_direction_is_refused() {
        let raw = KvCheckpointIndexes {
            sorted: vec![sorted_dto("SIDEWAYS", "")],
            ..Default::default()
        };
        let err = decode_kv_indexes(&raw)
            .expect_err("unknown direction must be refused")
            .to_string();
        assert!(
            err.contains("SIDEWAYS"),
            "error must name the bad value: {err}"
        );
    }

    /// Same for an unknown window type, which would otherwise read as
    /// unwindowed and widen the index to every entry it holds.
    #[test]
    fn unknown_window_type_is_refused() {
        let raw = KvCheckpointIndexes {
            sorted: vec![sorted_dto("DESC", "FORTNIGHTLY")],
            ..Default::default()
        };
        let err = decode_kv_indexes(&raw)
            .expect_err("unknown window type must be refused")
            .to_string();
        assert!(
            err.contains("FORTNIGHTLY"),
            "error must name the bad value: {err}"
        );
    }

    #[test]
    fn composite_with_mismatched_positions_is_refused() {
        let raw = KvCheckpointIndexes {
            composites: vec![super::super::index_format::KvCheckpointCompositeIndex {
                fields: vec!["a".into(), "b".into()],
                field_positions: vec![0],
                entries: Vec::new(),
            }],
            ..Default::default()
        };
        assert!(decode_kv_indexes(&raw).is_err());
    }

    /// Buckets hold one key with many primary keys; the installer wants pairs.
    #[test]
    fn buckets_flatten_to_every_pair() {
        let raw = KvCheckpointIndexes {
            fields: vec![KvCheckpointFieldIndex {
                field: "region".into(),
                field_position: 0,
                entries: vec![KvCheckpointIndexEntry {
                    key: b"us".to_vec(),
                    primary_keys: vec![b"k1".to_vec(), b"k2".to_vec()],
                }],
            }],
            ..Default::default()
        };
        let decoded = decode_kv_indexes(&raw).expect("decode");
        assert_eq!(decoded.fields[0].entries.len(), 2);
        assert_eq!(decoded.fields[0].entries[0].0, b"us".to_vec());
    }
}
