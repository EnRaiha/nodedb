// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane-facing [`SnapshotApplier`] implementation for the Raft snapshot
//! RECEIVE path.
//!
//! `nodedb-cluster` defines the [`nodedb_cluster::SnapshotApplier`] trait but
//! cannot depend on `nodedb` (circular), so the host crate supplies this
//! implementation. The install-snapshot finalize path calls it on the FOLLOWER
//! after the atomic `.partial`→`.snap` rename and before advancing Raft.
//!
//! The apply reuses the existing Data-Plane restore handler
//! (`MetaOp::RestoreTenantSnapshot`) with `replace_mode: true`, so a Raft
//! install OVERWRITES keys present in the snapshot (a Raft install must replace
//! local state, unlike user RESTORE which fail-closes on collisions). The
//! per-group snapshot bytes are the same vshard-filtered `TenantDataSnapshot`
//! the leader's `DataPlaneSnapshotBuilder` ships; the handler installs by
//! payload keys regardless of the `tenant_id` plan field, so a multi-tenant
//! snapshot applies correctly.
//!
//! Scope (DECLARED, tracked residual — NOT a silent gap): this makes a
//! FRESH/new-replica follower fully correct, plus OVERWRITE of keys PRESENT in
//! the snapshot. It does NOT yet remove keys that were DELETED before the
//! snapshot index, nor DROP collections that no longer exist, on a LAGGING
//! follower — i.e. it is install-over-present, not exact clear-then-install.
//! Exact clear-then-install lands in the next unit.

use std::sync::Arc;
use std::time::Duration;

use nodedb_types::id::DatabaseId;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::TenantId;
use nodedb_physical::physical_plan::MetaOp;

/// The Raft group that owns cluster topology / metadata (group 0). Its state
/// machine is restored inline by `MultiRaft::handle_install_snapshot`, so the
/// applier is a no-op for it.
const METADATA_GROUP_ID: u64 = 0;

/// Per-group snapshot apply dispatch timeout (mirrors the restore orchestrator's
/// node timeout and the snapshot builder's per-tenant timeout).
const SNAPSHOT_APPLY_TIMEOUT: Duration = Duration::from_secs(120);

/// Applies received per-group snapshots to the local Data Plane on the Raft
/// snapshot RECEIVE path.
pub struct DataPlaneSnapshotApplier {
    shared: Arc<SharedState>,
}

impl DataPlaneSnapshotApplier {
    /// Construct an applier bound to the node's shared state.
    pub fn new(shared: Arc<SharedState>) -> Self {
        Self { shared }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::SnapshotApplier for DataPlaneSnapshotApplier {
    async fn apply_snapshot(
        &self,
        group_id: u64,
        snapshot_bytes: &[u8],
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Metadata group is restored inline by the Raft state machine — nothing
        // to apply to the Data Plane here.
        if group_id == METADATA_GROUP_ID {
            return Ok(());
        }
        // Empty payload is the bootstrap stub — nothing to restore.
        if snapshot_bytes.is_empty() {
            return Ok(());
        }

        // Reuse the existing local restore handler with replace_mode = true so a
        // Raft install OVERWRITES present keys. The handler installs by the
        // snapshot's own per-key tenant/db prefixes, so the `tenant_id` plan
        // field is only the dispatch routing key — mirror the local RESTORE
        // dispatch (DEFAULT db, "__system" collection). Tenant 0 is used as the
        // representative routing tenant for the multi-tenant payload.
        let plan = PhysicalPlan::Meta(MetaOp::RestoreTenantSnapshot {
            tenant_id: 0,
            snapshot: snapshot_bytes.to_vec(),
            replace_mode: true,
        });

        crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
            &self.shared,
            TenantId::new(0),
            DatabaseId::DEFAULT,
            "__system",
            plan,
            SNAPSHOT_APPLY_TIMEOUT,
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(())
    }
}
