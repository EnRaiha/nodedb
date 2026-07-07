// SPDX-License-Identifier: BUSL-1.1

//! Tombstone write helper for cloned collections.
//!
//! When a DELETE targets a row that exists only in the source of a `Shadowed`
//! clone, this module records a tombstone in `_system.clone_tombstones`.  The
//! read path consults this table before falling back to source storage, so
//! subsequent reads correctly return "not found."

use std::sync::Arc;

use nodedb_types::{DatabaseId, Surrogate};

use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::state::SharedState;

/// Parameters for a tombstone write.
pub struct TombstoneParams<'a> {
    pub state: &'a Arc<SharedState>,
    /// The database ID of the clone (target).
    pub target_db_id: DatabaseId,
    /// The plain collection name (not db_qualified).
    pub target_collection: &'a str,
    /// The source surrogate to tombstone.
    pub source_surrogate: Surrogate,
}

/// Parameters for a KV tombstone write.
pub struct KvTombstoneParams<'a> {
    pub state: &'a Arc<SharedState>,
    /// The database ID of the clone (target).
    pub target_db_id: DatabaseId,
    /// The plain collection name (not db_qualified).
    pub target_collection: &'a str,
    /// The KV primary key to tombstone (raw string).
    pub kv_key: String,
}

/// Record a KV tombstone for `kv_key` in `target_collection`.
///
/// After this call, the clone read path will exclude the source row with this
/// KV key from scan results, even though the row still exists in the source.
pub fn perform_kv_clone_tombstone(params: KvTombstoneParams<'_>) -> crate::Result<()> {
    let KvTombstoneParams {
        state,
        target_db_id,
        target_collection,
        kv_key,
    } = params;

    let catalog = state.credentials.catalog();

    let target_coll_qualified = db_qualified(target_db_id, target_collection);

    catalog
        .put_kv_clone_tombstone(&target_coll_qualified, &kv_key)
        .map_err(|e| crate::Error::Storage {
            engine: "clone_kv_tombstone".into(),
            detail: format!("put_kv_clone_tombstone failed: {e}"),
        })
}

/// Record a tombstone for `source_surrogate` in `target_collection`.
///
/// After this call, the clone read path will return "not found" for this
/// surrogate, even though the row still exists in the source database.
pub fn perform_clone_tombstone(params: TombstoneParams<'_>) -> crate::Result<()> {
    let TombstoneParams {
        state,
        target_db_id,
        target_collection,
        source_surrogate,
    } = params;

    let catalog = state.credentials.catalog();

    let target_coll_qualified = db_qualified(target_db_id, target_collection);

    catalog
        .put_clone_tombstone(&target_coll_qualified, source_surrogate.as_u32())
        .map_err(|e| crate::Error::Storage {
            engine: "clone_tombstone".into(),
            detail: format!("put_clone_tombstone failed: {e}"),
        })
}
