// SPDX-License-Identifier: BUSL-1.1

//! Whole-partition column reads for the timeseries scan paths.
//!
//! The partition writer writes `schema.json` and one `.col` file per schema
//! column, so a schema or column that does not read is corruption. A `.sym`
//! file is written only for a symbol column that has a dictionary, so an
//! absent one is no dictionary, while a present one that does not read is
//! corruption. Corruption refuses the read and files one report here. No
//! caller skips a partition or a column it cannot read.

use std::collections::HashMap;
use std::path::Path;

use nodedb_types::timeseries::SymbolDictionary;

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType, ColumnarSchema};
use crate::engine::timeseries::columnar_segment::{ColumnarSegmentReader, SegmentError};

/// Every column of one on-disk timeseries partition, read whole.
pub(in crate::data::executor) struct TsPartitionColumns {
    pub schema: ColumnarSchema,
    /// One entry per schema column, in schema order. Every entry is `Some`:
    /// the `Option` matches the partition-row emitters this feeds.
    pub columns: Vec<Option<ColumnData>>,
    /// Dictionary of each symbol column that has one, by column index.
    pub sym_dicts: HashMap<usize, SymbolDictionary>,
}

/// The corruption error for the partition at `part_dir`, with one report
/// filed. Every timeseries path that finds a listed partition unreadable
/// calls this at the site that finds it. `stage` names what failed and
/// `site` names the read path.
pub(crate) fn partition_corrupt(
    part_dir: &Path,
    stage: &'static str,
    site: &'static str,
    detail: impl std::fmt::Display,
) -> crate::Error {
    let err = crate::Error::SegmentCorrupted {
        detail: format!("timeseries partition {}: {detail}", part_dir.display()),
    };
    let partition = part_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    crate::diag::timeseries_partition_unreadable(&err, partition, stage, site);
    err
}

/// The error for a partition the registry lists and the disk lacks, or
/// `Ok` when its directory is present.
pub(crate) fn require_partition_dir(part_dir: &Path, site: &'static str) -> crate::Result<()> {
    if part_dir.is_dir() {
        return Ok(());
    }
    Err(partition_corrupt(
        part_dir,
        "directory",
        site,
        "the partition registry lists it, and its directory is missing",
    ))
}

/// Read the schema, every column, and every symbol dictionary of the
/// partition at `part_dir`. `site` names the read path in the report.
///
/// `Err` when the directory is missing: the caller reads only partitions
/// the registry lists.
pub(in crate::data::executor) fn read_ts_partition(
    part_dir: &Path,
    site: &'static str,
) -> crate::Result<TsPartitionColumns> {
    require_partition_dir(part_dir, site)?;
    let report =
        |stage: &'static str, err: SegmentError| partition_corrupt(part_dir, stage, site, err);
    let schema =
        ColumnarSegmentReader::read_schema(part_dir, None).map_err(|e| report("schema", e))?;
    // Prefetch all column files into page cache before reading.
    let all_col_names: Vec<String> = schema.columns.iter().map(|(n, _)| n.clone()).collect();
    crate::data::io::fadvise::prefetch_partition_columns(part_dir, &all_col_names);
    let columns = schema
        .columns
        .iter()
        .map(|(name, ty)| ColumnarSegmentReader::read_column(part_dir, name, *ty, None).map(Some))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| report("column", e))?;
    let mut sym_dicts = HashMap::new();
    for (i, (name, ty)) in schema.columns.iter().enumerate() {
        if *ty != ColumnType::Symbol || !part_dir.join(format!("{name}.sym")).exists() {
            continue;
        }
        let dict = ColumnarSegmentReader::read_symbol_dict(part_dir, name, None)
            .map_err(|e| report("symbol_dict", e))?;
        sym_dicts.insert(i, dict);
    }
    Ok(TsPartitionColumns {
        schema,
        columns,
        sym_dicts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partition_without_a_schema_is_refused_as_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = read_ts_partition(dir.path(), "test");
        assert!(matches!(result, Err(crate::Error::SegmentCorrupted { .. })));
    }

    #[test]
    fn a_listed_partition_without_a_directory_is_refused_as_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = read_ts_partition(&dir.path().join("ts-0001"), "test");
        assert!(matches!(
            result,
            Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("directory is missing")
        ));
    }
}
