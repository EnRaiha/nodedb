// SPDX-License-Identifier: BUSL-1.1

//! Storage-mode-aware decode of a STORED document blob into
//! `serde_json::Value` for secondary-index value extraction.
//!
//! The pre-write (old) bytes of a row live in the collection's on-disk
//! format: MessagePack for schemaless collections, Binary Tuple for strict
//! collections. `doc_format::decode_document` only understands the former —
//! it returns `None` for a Binary Tuple because decoding one requires the
//! schema. This helper bridges that gap so the non-bitemporal secondary-index
//! UPDATE diff and the DELETE rollback capture compute the real old index
//! values for BOTH storage modes.

use nodedb_physical::physical_plan::StorageMode;

use crate::data::executor::doc_format;
use crate::data::executor::strict_format;
use crate::engine::document::store::CollectionConfig;

use super::CoreLoop;

impl CoreLoop {
    /// Decode STORED document bytes (the on-disk blob of an existing row) to a
    /// `serde_json::Value` using the collection's storage mode.
    ///
    /// - Strict collections → Binary Tuple decode via the collection's
    ///   [`StrictSchema`](nodedb_types::columnar::StrictSchema).
    /// - Schemaless collections → MessagePack/JSON auto-detect via
    ///   [`doc_format::decode_document`].
    ///
    /// Returns `None` when the bytes cannot be decoded under the resolved
    /// mode (e.g. malformed tuple). Intended for extracting old secondary
    /// index values from the pre-write row; the NEW document is decoded
    /// straight from the input JSON and does not need this path.
    pub(in crate::data::executor) fn decode_stored_document(
        &self,
        config: &CollectionConfig,
        bytes: &[u8],
    ) -> Option<serde_json::Value> {
        match &config.storage_mode {
            StorageMode::Strict { schema } => strict_format::binary_tuple_to_json(bytes, schema),
            StorageMode::Schemaless => doc_format::decode_document(bytes),
        }
    }
}
