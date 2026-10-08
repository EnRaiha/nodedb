// SPDX-License-Identifier: Apache-2.0

//! Forensic payloads carried by columnar reports.
//!
//! Grouping keys carry no row index. The row identifies the occurrence, so a
//! scan over many bad rows of one column files one report with a count.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A memtable cell whose bytes do not hold a value of its declared type.
pub(super) struct MemtableCellCorrupt<'a> {
    /// Collection whose memtable holds the cell.
    pub collection: &'a str,
    /// Column of the cell.
    pub column: &'a str,
    /// The error text: column, row, and why the bytes do not decode.
    pub detail: String,
}

/// A columnar WAL row whose bytes do not decode.
pub(super) struct WalRowCorrupt {
    /// The error text: the byte offset and why the bytes do not decode.
    pub detail: String,
}

impl DomainContext for WalRowCorrupt {
    fn domain_kind(&self) -> &'static str {
        "nodedb_columnar.wal_row_corrupt"
    }

    fn grouping_key(&self) -> String {
        "wal_row".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "detail": self.detail,
            "why_fatal": "the row the record carries cannot be rebuilt, so its decode is \
                          refused",
            "operator_action": "restore the collection from a snapshot taken before the \
                                damaged record",
        })
    }
}

/// A string column cell whose bytes are not UTF-8 when a segment is written.
pub(super) struct StringCellNotUtf8<'a> {
    /// Column of the cell.
    pub column: &'a str,
    /// The error text: column and row.
    pub detail: String,
}

impl DomainContext for StringCellNotUtf8<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb_columnar.string_cell_not_utf8"
    }

    fn grouping_key(&self) -> String {
        format!("column={}", self.column)
    }

    fn to_json(&self) -> Value {
        json!({
            "column": self.column,
            "detail": self.detail,
            "why_fatal": "the segment write is refused: block statistics built from these \
                          bytes would prune rows a predicate matches",
            "operator_action": "rewrite or delete the named row, then let the flush retry",
        })
    }
}

impl DomainContext for MemtableCellCorrupt<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb_columnar.memtable_cell_corrupt"
    }

    fn grouping_key(&self) -> String {
        format!("collection={} column={}", self.collection, self.column)
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "column": self.column,
            "detail": self.detail,
            "why_fatal": "every read of the row is refused until the row is rewritten or \
                          removed. A flush of the memtable would copy the bad bytes into a \
                          segment",
            "operator_action": "rewrite or delete the named row. If many rows fail, \
                                restore the collection from a snapshot",
        })
    }
}
