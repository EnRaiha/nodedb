// SPDX-License-Identifier: Apache-2.0

//! The recording implementation of the columnar report sites.
//!
//! Each function runs only on a path that already returns the error it
//! reports. `Capture::emit` never panics and returns `None` when the host
//! never initialized the recorder, so its result is discarded.

use faultbox::{Capture, EventKind, error_chain_of};

use super::context;
use crate::error::ColumnarError;

/// Report a memtable cell whose bytes do not hold a value of its type.
///
/// Called only from the `MutationEngine` memtable row reader, the one site
/// that detects it. The memtable writer encodes every cell, so the bytes
/// were damaged in memory or restored damaged from a checkpoint.
pub fn memtable_cell_corrupt(err: &ColumnarError, collection: &str) {
    let column = match err {
        ColumnarError::MemtableCellCorrupt { column, .. } => column.as_str(),
        _ => "",
    };
    let ctx = context::MemtableCellCorrupt {
        collection,
        column,
        detail: err.to_string(),
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "columnar memtable cell does not decode, so the read is refused",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a columnar WAL row whose bytes do not decode.
///
/// Called only from `decode_row_from_wal`, the one site that detects it.
pub fn wal_row_corrupt(err: &ColumnarError) {
    let ctx = context::WalRowCorrupt {
        detail: err.to_string(),
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "columnar WAL row does not decode, so the decode is refused",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a string column cell whose bytes are not UTF-8 at segment write.
///
/// Called only from the block statistics builder, the one site that
/// detects it. The memtable push stores only UTF-8, so the bytes were
/// damaged in memory.
pub fn string_cell_not_utf8(err: &ColumnarError) {
    let column = match err {
        ColumnarError::StringCellNotUtf8 { column, .. } => column.as_str(),
        _ => "",
    };
    let ctx = context::StringCellNotUtf8 {
        column,
        detail: err.to_string(),
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "string column cell is not UTF-8, so the segment write is refused",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
