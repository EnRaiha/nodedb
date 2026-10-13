// SPDX-License-Identifier: BUSL-1.1

//! Rebuild a memtable's per-series row counts from the rows it holds.
//!
//! A row stores no series id. Ingest derives the id from the row's tags: the
//! measurement and every tag the row carries form a [`SeriesKey`], and the
//! collection's series catalog maps the key to its id. A recount derives the
//! key the same way. The tag columns are the columns the known keys name,
//! and a row carries a tag where its cell is not null. A string field is a
//! symbol column too, but no key names it, so it is never taken for a tag.

use std::collections::{HashMap, HashSet};

use nodedb_types::timeseries::{SeriesId, SeriesKey};

use super::super::types::{ColumnData, ColumnType, NULL_SYMBOL_ID};
use super::table::ColumnarMemtable;

impl ColumnarMemtable {
    /// The series this memtable counts rows for.
    pub fn series_ids(&self) -> Vec<SeriesId> {
        self.series_row_counts.keys().copied().collect()
    }

    /// Rebuild the per-series counts from the rows held now.
    ///
    /// `known` is each series the rows can belong to with its catalog key: the
    /// series this memtable counted before rows left it. A row whose tags
    /// match no known key counts under the id its key hashes to, so the
    /// number of series stays exact.
    pub fn recount_series(&mut self, known: &[(SeriesId, SeriesKey)]) {
        let tag_names: HashSet<&str> = known
            .iter()
            .flat_map(|(_, key)| key.tags.iter().map(|(name, _)| name.as_str()))
            .collect();
        let tag_columns: Vec<(usize, &str)> = self
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (name, ty))| {
                *ty == ColumnType::Symbol && tag_names.contains(name.as_str())
            })
            .map(|(index, (name, _))| (index, name.as_str()))
            .collect();
        let by_tags: HashMap<&[(String, String)], SeriesId> = known
            .iter()
            .map(|(id, key)| (key.tags.as_slice(), *id))
            .collect();
        let metric = known.first().map_or("", |(_, key)| key.metric.as_str());

        let mut counts: HashMap<SeriesId, u64> = HashMap::new();
        for row in 0..self.row_count as usize {
            let tags: Vec<(String, String)> = tag_columns
                .iter()
                .filter_map(|&(column, name)| {
                    self.tag_value(column, row)
                        .map(|value| (name.to_string(), value))
                })
                .collect();
            let key = SeriesKey::new(metric, tags);
            let id = by_tags
                .get(&key.tags.as_slice())
                .copied()
                .unwrap_or_else(|| key.to_series_id(0));
            *counts.entry(id).or_insert(0) += 1;
        }
        self.series_row_counts = counts;
    }

    /// The tag value of `row` in symbol column `column`, or `None` for a null
    /// cell.
    fn tag_value(&self, column: usize, row: usize) -> Option<String> {
        match self.columns.get(column)? {
            ColumnData::Symbol(ids) => {
                let id = *ids.get(row)?;
                if id == NULL_SYMBOL_ID {
                    return None;
                }
                self.symbol_dicts.get(&column)?.get(id).map(str::to_string)
            }
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                if !valid.get(row).copied().unwrap_or(false) {
                    return None;
                }
                let id = usize::try_from(*ids.get(row)?).ok()?;
                dictionary.get(id).cloned()
            }
            ColumnData::Timestamp(_) | ColumnData::Float64(_) | ColumnData::Int64(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::types::{
        ColumnValue, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };
    use super::*;

    fn schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("note".into(), ColumnType::Symbol),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        }
    }

    fn key(host: &str) -> SeriesKey {
        SeriesKey::new("cpu", vec![("host".into(), host.into())])
    }

    #[test]
    fn a_recount_counts_the_series_of_the_rows_held() {
        let mut mt = ColumnarMemtable::new(schema(), ColumnarMemtableConfig::default());
        let known = vec![(11, key("a")), (22, key("b"))];
        for (series, host, note) in [(11, "a", "x"), (22, "b", "y"), (11, "a", "z")] {
            mt.ingest_row(
                series,
                &[
                    ColumnValue::Timestamp(1_000),
                    ColumnValue::Symbol(host.into()),
                    ColumnValue::Symbol(note.into()),
                    ColumnValue::Float64(1.0),
                ],
            )
            .expect("ingest");
        }
        assert_eq!(mt.series_count(), 2);
        mt.recount_series(&known);
        assert_eq!(mt.series_count(), 2, "a string field is not a tag");
        let mut ids = mt.series_ids();
        ids.sort_unstable();
        assert_eq!(ids, vec![11, 22]);
    }
}
