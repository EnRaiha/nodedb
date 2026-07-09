// SPDX-License-Identifier: BUSL-1.1

//! Pure `SortedIndexDef` construction for `RegisterSortedIndex`, shared by
//! the autocommit handler (`sorted.rs`) and WAL replay
//! (`wal_replay_kv_sorted_index.rs`), so a live registration and its durable
//! replay are always built by the exact same code — mirrors the
//! `merge_field_updates` / `compute_transfer` splits for `FieldSet` /
//! `Transfer`.

use crate::bridge::envelope::ErrorCode;
use crate::engine::kv::sorted_index::key::{SortColumn, SortDirection, SortKeyEncoder};
use crate::engine::kv::sorted_index::manager::SortedIndexDef;
use crate::engine::kv::sorted_index::window::WindowConfig;

/// Raw fields needed to build a [`SortedIndexDef`], bundled so
/// [`build_sorted_index_def`] stays under the `too_many_arguments` clippy
/// threshold.
pub(in crate::data::executor) struct BuildSortedIndexDefParams<'a> {
    pub collection: &'a str,
    pub index_name: &'a str,
    pub sort_columns: &'a [(String, String)],
    pub key_column: &'a str,
    pub window_type: &'a str,
    pub window_timestamp_column: &'a str,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

/// Build a [`SortedIndexDef`] from the raw `RegisterSortedIndex` fields,
/// validating that a windowed index's timestamp column is included in its
/// sort columns. Returns `Err` on that validation failure; never panics.
pub(in crate::data::executor) fn build_sorted_index_def(
    params: BuildSortedIndexDefParams<'_>,
) -> Result<SortedIndexDef, ErrorCode> {
    let BuildSortedIndexDefParams {
        collection,
        index_name,
        sort_columns,
        key_column,
        window_type,
        window_timestamp_column,
        window_start_ms,
        window_end_ms,
    } = params;

    let columns: Vec<SortColumn> = sort_columns
        .iter()
        .map(|(name, dir)| SortColumn {
            name: name.clone(),
            direction: if dir.eq_ignore_ascii_case("DESC") {
                SortDirection::Desc
            } else {
                SortDirection::Asc
            },
        })
        .collect();

    let window = match window_type.to_uppercase().as_str() {
        "DAILY" => WindowConfig::daily(window_timestamp_column),
        "WEEKLY" => WindowConfig::weekly(window_timestamp_column),
        "MONTHLY" => WindowConfig::monthly(window_timestamp_column),
        "CUSTOM" => WindowConfig::custom(window_timestamp_column, window_start_ms, window_end_ms),
        _ => WindowConfig::none(),
    };

    let encoder = SortKeyEncoder::new(columns);

    // Validate: if windowed, the timestamp column must be in the sort key columns.
    if !window.is_unwindowed() {
        let ts_col = &window.timestamp_column;
        let has_ts = encoder.columns().iter().any(|c| c.name == *ts_col);
        if !has_ts {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "WINDOW timestamp column '{}' must be included in sort columns",
                    ts_col
                ),
            });
        }
    }

    Ok(SortedIndexDef {
        name: index_name.to_string(),
        collection: collection.to_string(),
        key_column: key_column.to_string(),
        encoder,
        window,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_unwindowed_def() {
        let sort_columns = vec![("score".to_string(), "DESC".to_string())];
        let def = build_sorted_index_def(BuildSortedIndexDefParams {
            collection: "players",
            index_name: "lb",
            sort_columns: &sort_columns,
            key_column: "player_id",
            window_type: "",
            window_timestamp_column: "",
            window_start_ms: 0,
            window_end_ms: 0,
        })
        .expect("build unwindowed def");
        assert_eq!(def.name, "lb");
        assert_eq!(def.collection, "players");
        assert!(def.window.is_unwindowed());
    }

    #[test]
    fn custom_window_missing_timestamp_column_is_rejected() {
        let sort_columns = vec![("score".to_string(), "DESC".to_string())];
        let err = build_sorted_index_def(BuildSortedIndexDefParams {
            collection: "players",
            index_name: "lb",
            sort_columns: &sort_columns,
            key_column: "player_id",
            window_type: "CUSTOM",
            window_timestamp_column: "updated_at",
            window_start_ms: 1_000,
            window_end_ms: 2_000,
        })
        .expect_err("must reject missing timestamp column");
        match err {
            ErrorCode::Internal { detail } => {
                assert!(detail.contains("updated_at"));
            }
            other => panic!("expected Internal error, got {other:?}"),
        }
    }

    #[test]
    fn custom_window_preserves_exact_bounds() {
        let sort_columns = vec![("updated_at".to_string(), "ASC".to_string())];
        let def = build_sorted_index_def(BuildSortedIndexDefParams {
            collection: "players",
            index_name: "lb",
            sort_columns: &sort_columns,
            key_column: "player_id",
            window_type: "CUSTOM",
            window_timestamp_column: "updated_at",
            window_start_ms: 1_700_000_000_000,
            window_end_ms: 1_700_100_000_000,
        })
        .expect("build custom window def");
        match def.window.window_type {
            crate::engine::kv::sorted_index::window::WindowType::Custom { start_ms, end_ms } => {
                assert_eq!(start_ms, 1_700_000_000_000);
                assert_eq!(end_ms, 1_700_100_000_000);
            }
            other => panic!("expected Custom window, got {other:?}"),
        }
    }
}
