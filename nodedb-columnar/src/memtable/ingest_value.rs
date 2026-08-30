// SPDX-License-Identifier: Apache-2.0

//! Borrowed value type for zero-copy memtable ingest.

/// Borrowed value for zero-copy ingest into the columnar memtable.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum IngestValue<'a> {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Timestamp(i64),
    /// Borrowed string — for `String` or `DictEncoded` columns.
    Str(&'a str),
}
