// SPDX-License-Identifier: BUSL-1.1

//! Materialization of applied CRDT deltas into the sparse document store.
//!
//! When a CRDT (Loro) delta is applied — whether from a sync peer or a native
//! client — the merged document must also be written into the sparse DOCUMENTS
//! store so `DocumentScan` / `ShapeSnapshot` observe it, exactly as a native
//! schemaless put does. These helpers are split out of `crdt.rs` to keep that
//! file within the file-size limit; they extend `CoreLoop` with the encode +
//! write steps invoked from `execute_crdt_apply`.

use tracing::warn;

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format::canonicalize_document_for_storage;
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::document::crdt_store::loro_value_to_json;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::engine::sparse::btree_versioned::VersionedPut;

impl CoreLoop {
    /// Read the merged Loro row back and encode it into the canonical
    /// schemaless storage bytes the native put path writes.
    ///
    /// Called while the CRDT engine `&mut` borrow is still live (the borrow
    /// checker forbids touching `self.sparse` here), so it is an associated
    /// function over the borrowed engine rather than a method. Returns `None`
    /// when the row is absent or cannot be converted — the caller then skips
    /// the sparse write. A materialization miss must never fail the delta
    /// apply: the Loro merge has already succeeded and the sync stream must
    /// not wedge.
    pub(super) fn encode_crdt_row(
        engine: &TenantCrdtEngine,
        collection: &str,
        document_id: &str,
    ) -> Option<Vec<u8>> {
        let loro_val = engine.read_row(collection, document_id)?;
        let json = loro_value_to_json(&loro_val);
        let msgpack = nodedb_types::json_to_msgpack(&json).ok()?;
        Some(canonicalize_document_for_storage(&msgpack))
    }

    /// Write the merged CRDT document into the sparse document store so
    /// `DocumentScan` / `ShapeSnapshot` observe the synced write, matching the
    /// key and bytes the native schemaless put path produces.
    ///
    /// The storage key is the hex-encoded surrogate (identical to the native
    /// path), NOT the CRDT `document_id` (which is the user-facing Loro row
    /// id). Bitemporal collections append a version per applied delta;
    /// non-bitemporal collections overwrite by key (idempotent under replay).
    /// FTS is intentionally NOT indexed here — the sync path delivers a
    /// separate `FtsIndex` frame, and re-indexing would double-index. A write
    /// failure is logged and swallowed so a materialization miss never wedges
    /// the sync stream.
    pub(super) fn materialize_synced_document(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        stored: &[u8],
    ) {
        let storage_key = surrogate_to_doc_id(surrogate);
        let result = if self.is_bitemporal(tid, collection) {
            self.sparse.versioned_put(VersionedPut {
                database_id,
                tenant: tid,
                coll: collection,
                doc_id: storage_key.as_str(),
                sys_from_ms: self.bitemporal_now_ms(),
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: stored,
            })
        } else {
            self.sparse
                .put(database_id, tid, collection, storage_key.as_str(), stored)
                .map(|_| ())
        };
        if let Err(e) = result {
            warn!(
                core = self.core_id,
                %collection,
                document_id = %storage_key,
                error = %e,
                "crdt sync materialize into sparse document store failed"
            );
        }
    }
}
