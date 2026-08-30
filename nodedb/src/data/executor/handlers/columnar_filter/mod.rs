// SPDX-License-Identifier: BUSL-1.1

//! Columnar predicate evaluation on raw column vectors.
//!
//! Evaluates `ScanFilter` predicates directly on typed columnar data
//! (`Vec<f64>`, `Vec<i64>`, `Vec<u32>` symbol IDs) without constructing
//! JSON rows. Used by timeseries scans (memtable + sealed partitions),
//! columnar aggregation, and any path that filters columnar data.
//!
//! Returns a bitmask of passing rows. Falls back to `None` for filter
//! patterns that can't be evaluated on columnar data (OR clauses, string
//! ordering, unsupported operators).

mod dict;
mod eval;
mod memtable_source;
mod partition_source;
mod source;

pub(crate) use eval::{apply_mask, eval_filters_bitmask, eval_filters_dense, eval_filters_sparse};
pub(crate) use partition_source::PartitionColumns;
pub(crate) use source::ColumnarSource;
