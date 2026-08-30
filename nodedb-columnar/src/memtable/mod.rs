// SPDX-License-Identifier: Apache-2.0

//! Columnar memtable: in-memory row buffer with typed column vectors.
//!
//! Each column is stored as a typed vector (Vec<i64>, Vec<f64>, etc.) rather
//! than Vec<Value> to avoid enum overhead and enable SIMD-friendly memory layout.
//! The memtable accumulates INSERTs and flushes to a segment when the row count
//! reaches the configured threshold.
//!
//! NOT thread-safe — lives on a single Data Plane core (!Send by design in Origin,
//! Mutex-wrapped in Lite).

mod column_data;
mod core;
mod ingest_value;
mod iter;
mod mutation;

pub use column_data::{ColumnData, DICT_ENCODE_MAX_CARDINALITY};
pub use core::{ColumnarMemtable, DEFAULT_FLUSH_THRESHOLD};
pub use ingest_value::IngestValue;
pub use iter::MemtableRowIter;
