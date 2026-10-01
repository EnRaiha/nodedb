// SPDX-License-Identifier: BUSL-1.1

//! Copy-up write helper for cloned collections.
//!
//! When an UPDATE targets a row that exists only in the source of a
//! `Shadowed` clone, this module performs the copy-up:
//!
//! 1. Allocate a fresh target surrogate.
//! 2. Write the source row to the target shard's owner, through its
//!    replicated write path, so every replica holds it.
//! 3. Record `(target_collection, source_surrogate) → target_surrogate` in
//!    the `clone_copyups` table of every node, through the metadata log.
//!
//! The target row goes first. A clone read suppresses every source row that
//! has a mapping, so a mapping without its target row hides the row
//! entirely. A failure after the target write instead leaves an exact copy of
//! the source row with no mapping. A clone read also suppresses a source row
//! whose primary key the target holds, so the row still reads once, from the
//! target. A retry of the statement assigns the same target surrogate,
//! rewrites the same row, and then writes the mapping, so it converges. The
//! materializer's insert-if-absent skips the existing target row, and
//! materialization then drops the source side.

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::maintenance::clone_materializer::dispatch_to_owner;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

/// Parameters for a KV copy-up operation.
pub struct KvCopyUpParams<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub target_db_id: DatabaseId,
    /// Plain (non-db_qualified) collection name.
    pub target_collection: &'a str,
    /// The KV key bytes (primary key).
    pub kv_key: Vec<u8>,
    /// Serialized KV value bytes (the stored value columns, msgpack).
    pub source_value_bytes: Vec<u8>,
}

/// Perform a KV copy-up: write `source_value_bytes` into the target shard
/// under `kv_key` on every replica, making the row available for subsequent
/// FieldSet or Delete operations in the clone.
///
/// A KV copy-up records no mapping: a clone read hides a source key the
/// target holds, and the caller's tombstone follows. A failure before the
/// tombstone leaves a target row that already shadows the source, and a retry
/// rewrites it.
pub async fn perform_kv_clone_copyup(params: KvCopyUpParams<'_>) -> crate::Result<()> {
    let KvCopyUpParams {
        state,
        tenant_id,
        target_db_id,
        target_collection,
        kv_key,
        source_value_bytes,
    } = params;

    let target_key = nodedb_types::CollectionKey::from_bare(target_db_id, target_collection);
    let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
        state,
        target_key,
        tenant_id,
        &kv_key,
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| crate::Error::Storage {
        engine: "clone_kv_copyup".into(),
        detail: format!("surrogate alloc failed: {e}"),
    })?;

    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: nodedb_types::QualifiedCollection::new(target_db_id, target_collection),
        key: kv_key,
        value: source_value_bytes,
        ttl_ms: 0,
        surrogate,
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    });
    dispatch_to_owner(
        state,
        tenant_id,
        target_db_id,
        &db_qualified(target_db_id, target_collection),
        put_plan,
    )
    .await
    .map_err(|e| crate::Error::Storage {
        engine: "clone_kv_copyup".into(),
        detail: format!("target write failed: {e}"),
    })?;
    Ok(())
}

/// Parameters for a copy-up operation.
pub struct CopyUpParams<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub target_db_id: DatabaseId,
    /// Plain (non-db_qualified) collection name.
    pub target_collection: &'a str,
    /// The source surrogate to copy up.
    pub source_surrogate: Surrogate,
    /// Serialized source row body (msgpack).  Must be obtained by the caller
    /// via a prior GET on the source collection.
    pub source_doc_id: String,
    pub source_row_bytes: Vec<u8>,
}

/// Perform a copy-up: write `source_row_bytes` into the target shard with a
/// fresh surrogate on every replica, then record the mapping in
/// `clone_copyups` on every node.
///
/// Returns the fresh target surrogate so the caller can apply the pending
/// UPDATE to it.
pub async fn perform_clone_copyup(params: CopyUpParams<'_>) -> crate::Result<Surrogate> {
    let CopyUpParams {
        state,
        tenant_id,
        target_db_id,
        target_collection,
        source_surrogate,
        source_doc_id,
        source_row_bytes,
    } = params;

    // Allocate a fresh target surrogate using the (collection, doc_id) key.
    let target_key = nodedb_types::CollectionKey::from_bare(target_db_id, target_collection);
    let target_surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
        state,
        target_key,
        tenant_id,
        source_doc_id.as_bytes(),
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| crate::Error::Storage {
        engine: "clone_copyup".into(),
        detail: format!("surrogate alloc failed: {e}"),
    })?;

    let value = match state.credentials.catalog().get_collection(
        target_db_id,
        tenant_id.as_u64(),
        target_collection,
    )? {
        Some(coll) => super::identity::carry_identity(&coll, source_row_bytes, &source_doc_id),
        None => source_row_bytes,
    };
    let put_plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: nodedb_types::QualifiedCollection::new(target_db_id, target_collection),
        document_id: source_doc_id.clone(),
        value,
        surrogate: target_surrogate,
        pk_bytes: source_doc_id.as_bytes().to_vec(),
        // A copy-up is internal plumbing behind the caller's own statement; it
        // projects nothing and needs no read gate of its own.
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    });
    dispatch_to_owner(
        state,
        tenant_id,
        target_db_id,
        &db_qualified(target_db_id, target_collection),
        put_plan,
    )
    .await
    .map_err(|e| crate::Error::Storage {
        engine: "clone_copyup".into(),
        detail: format!("target write of document '{source_doc_id}' failed: {e}"),
    })?;

    // Keyed by the TARGET collection: every reader looks the mapping up
    // under the clone it belongs to.
    let row = CopyupRow {
        database_id: target_db_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        collection: target_collection.to_string(),
        source_surrogate: source_surrogate.as_u32(),
    };
    super::cow_entry::replicate_async(state, &row.put(target_surrogate))
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "clone_copyup".into(),
            detail: format!(
                "mapping of document '{source_doc_id}' failed after its target write: {e}"
            ),
        })?;
    Ok(target_surrogate)
}

/// One copy-up mapping row, before it becomes a catalog entry.
struct CopyupRow {
    database_id: u64,
    tenant_id: u64,
    collection: String,
    source_surrogate: u32,
}

impl CopyupRow {
    fn put(self, target_surrogate: Surrogate) -> CatalogEntry {
        CatalogEntry::PutCloneCopyup {
            database_id: self.database_id,
            tenant_id: self.tenant_id,
            collection: self.collection,
            source_surrogate: self.source_surrogate,
            target_surrogate: target_surrogate.as_u32(),
        }
    }
}
