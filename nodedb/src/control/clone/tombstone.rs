// SPDX-License-Identifier: BUSL-1.1

//! Tombstone write helper for cloned collections.
//!
//! When a DELETE targets a row that exists only in the source of a `Shadowed`
//! clone, this module records a tombstone in `_system.clone_tombstones` on
//! every node. The read path consults this table before falling back to
//! source storage, so subsequent reads return "not found."

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;

/// Parameters for a tombstone write.
pub struct TombstoneParams<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    /// The database ID of the clone (target).
    pub target_db_id: DatabaseId,
    /// The plain collection name (not db_qualified).
    pub target_collection: &'a str,
    /// The source surrogate to tombstone.
    pub source_surrogate: Surrogate,
}

/// Parameters for a KV tombstone write.
pub struct KvTombstoneParams<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    /// The database ID of the clone (target).
    pub target_db_id: DatabaseId,
    /// The plain collection name (not db_qualified).
    pub target_collection: &'a str,
    /// The KV primary key to tombstone (raw string).
    pub kv_key: String,
}

/// Record a KV tombstone for `kv_key` in `target_collection` on every node.
///
/// After this call, the clone read path will exclude the source row with this
/// KV key from scan results, even though the row still exists in the source.
pub async fn perform_kv_clone_tombstone(params: KvTombstoneParams<'_>) -> crate::Result<()> {
    let KvTombstoneParams {
        state,
        tenant_id,
        target_db_id,
        target_collection,
        kv_key,
    } = params;
    super::cow_entry::replicate_async(
        state,
        &CatalogEntry::PutKvCloneTombstone {
            database_id: target_db_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            collection: target_collection.to_string(),
            kv_key,
        },
    )
    .await
}

/// Record a tombstone for `source_surrogate` in `target_collection` on every
/// node.
///
/// After this call, the clone read path will return "not found" for this
/// surrogate, even though the row still exists in the source database.
pub async fn perform_clone_tombstone(params: TombstoneParams<'_>) -> crate::Result<()> {
    let TombstoneParams {
        state,
        tenant_id,
        target_db_id,
        target_collection,
        source_surrogate,
    } = params;
    super::cow_entry::replicate_async(
        state,
        &CatalogEntry::PutCloneTombstone {
            database_id: target_db_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            collection: target_collection.to_string(),
            source_surrogate: source_surrogate.as_u32(),
        },
    )
    .await
}
