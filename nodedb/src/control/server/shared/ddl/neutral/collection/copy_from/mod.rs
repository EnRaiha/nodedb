// SPDX-License-Identifier: BUSL-1.1

//! Handler for `COPY <collection> FROM '<path>'` bulk import.
//!
//! Supports NDJSON, JSON array, and CSV formats. Format is auto-detected from
//! the file extension when not explicitly specified via WITH clause.
//!
//! Engine routing:
//! - Document (schemaless/strict), Columnar, KV: supported.
//! - Timeseries: rejected (use ILP or INSERT with time_key column directly).
//! - Spatial: rejected (use INSERT with geometry column directly).
//! - Vector primary: supported (documents with embedding fields import normally).
//! - Array: tracked separately; COPY is not the Array ingest path.

mod csv_import;
mod entry;
mod json_import;

pub use entry::{CopyFromOptions, copy_from_file};
