// SPDX-License-Identifier: BUSL-1.1

//! Snapshot phase for `MOVE TENANT`.
//!
//! Captures every active collection of the source database from the whole
//! cluster. Each vShard is read once, from the leader of its group, after a
//! consistent cut. A collection's rows can live on any node and any core, so
//! a snapshot of one node will miss the rows of every vShard it does not
//! lead.
//!
//! The capture is grouped by each collection's owning tenant: the Data Plane
//! keys every row by that tenant.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::control::backup::capture::capture_collections;
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantDataSnapshot, TenantId};
use nodedb_types::NodeDbError;

/// The source database's live namespace and its captured rows.
pub struct SourceCapture {
    /// Every active collection of the source database. The cutover moves
    /// exactly these.
    pub collections: Vec<StoredCollection>,
    /// The captured rows of each owning tenant's collections.
    pub data: Vec<(TenantId, TenantDataSnapshot)>,
}

/// Run the snapshot phase for the move of `tenant_id` out of `source_db_id`.
///
/// Any node or core error, or a capture past `timeout`, fails the phase.
pub async fn run(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    timeout: Duration,
) -> Result<SourceCapture, NodeDbError> {
    let failed = |detail: String| {
        NodeDbError::move_tenant_snapshot_failed(tenant_id.as_u64().to_string(), detail)
    };

    // Soft-deleted collections are pending GC and stay out of the move.
    let collections: Vec<StoredCollection> = state
        .credentials
        .catalog()
        .load_all_collections(source_db_id)
        .map_err(|e| failed(format!("failed to enumerate source collections: {e}")))?
        .into_iter()
        .filter(|c| c.is_active)
        .collect();

    let owners = names_by_owner(&collections);
    let capture = async {
        let mut data = Vec::with_capacity(owners.len());
        for (owner, names) in &owners {
            // Arrays move by rekey at the cutover, never through the capture.
            let snap = capture_collections(state, *owner, source_db_id, names, false).await?;
            data.push((TenantId::new(*owner), snap));
        }
        Ok::<_, crate::Error>(data)
    };
    // The phase code stays the statement's verdict, and the typed capture
    // error rides as its cause with its own class.
    let data = tokio::time::timeout(timeout, capture)
        .await
        .map_err(|_| {
            failed(format!(
                "capture of source database {} did not finish within {timeout:?}",
                source_db_id.as_u64()
            ))
        })?
        .map_err(|e| {
            failed(format!(
                "capture of source database {} failed",
                source_db_id.as_u64()
            ))
            .with_cause(crate::error_classify::classify(&e))
        })?;

    Ok(SourceCapture { collections, data })
}

/// The bare names of `collections`, grouped by owning tenant.
fn names_by_owner(collections: &[StoredCollection]) -> BTreeMap<u64, BTreeSet<String>> {
    let mut owners: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for coll in collections {
        owners
            .entry(coll.tenant_id)
            .or_default()
            .insert(coll.name.clone());
    }
    owners
}

/// Return the temporary in-cluster storage key for the tenant's snapshot.
///
/// This key is recorded in the journal so crash recovery can clean up any
/// partial snapshot artifact.
pub fn temp_key(tenant_id: TenantId) -> String {
    format!("_move_tenant_snapshot_{}", tenant_id.as_u64())
}

/// Delete the temporary snapshot (best-effort; called on cutover success or
/// failure compensation).
///
/// The capture lives in memory and is not persisted to a separate store. The
/// `temp_key` recorded in the journal identifies the move only, so there is
/// nothing to delete.
pub async fn delete_temp(_state: &SharedState, _key: &str) -> Result<(), NodeDbError> {
    Ok(())
}
