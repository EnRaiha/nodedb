// SPDX-License-Identifier: BUSL-1.1

//! Per-node in-memory view of the replicated metadata state.
//!
//! The cache tracks everything `nodedb-cluster` natively understands:
//! the applied raft index, the HLC watermark, topology / routing
//! change history, descriptor leases, and cluster version. It
//! **does not** maintain per-DDL-object descriptor state — that
//! lives on the host side via
//! `nodedb::control::catalog_entry::CatalogEntry::apply_to` writing
//! into `SystemCatalog` redb. The `CatalogDdl { payload }` variant
//! is opaque here: the cache tracks its applied index and forwards
//! the payload to the host's `MetadataCommitApplier`.

use std::collections::HashMap;

use nodedb_types::Hlc;
use tracing::{debug, info, warn};

use crate::error::{ClusterError, MigrationCheckpointError};
use crate::metadata_group::compensation::Compensation;
use crate::metadata_group::descriptors::{DescriptorId, DescriptorLease};
use crate::metadata_group::entry::{MetadataEntry, RoutingChange, TopologyChange};
use crate::metadata_group::migration_state::{
    MigrationPhaseTag, PersistedMigrationCheckpoint, SharedMigrationStateTable,
};
use crate::routing::RoutingTable;

/// In-memory view of the committed metadata state.
#[derive(Debug, Default)]
pub struct MetadataCache {
    pub applied_index: u64,
    pub last_applied_hlc: Hlc,

    /// `(descriptor_id, node_id) -> lease`.
    pub leases: HashMap<(DescriptorId, u64), DescriptorLease>,

    /// Topology mutations applied so far.
    pub topology_log: Vec<TopologyChange>,
    pub routing_log: Vec<RoutingChange>,

    pub cluster_version: u16,

    /// Monotonically-increasing count of committed `CatalogDdl`
    /// entries. Exposed for tests and metrics — planners read
    /// catalog state through the host-side `SystemCatalog`, not
    /// this counter.
    pub catalog_entries_applied: u64,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a committed entry. Idempotent by `applied_index`:
    /// entries at or below the current watermark are ignored.
    pub fn apply(&mut self, index: u64, entry: &MetadataEntry) {
        if index != 0 && index <= self.applied_index {
            debug!(
                index,
                watermark = self.applied_index,
                "metadata cache: skipping already-applied entry"
            );
            return;
        }
        self.applied_index = index;

        match entry {
            MetadataEntry::CatalogDdl { payload: _ }
            | MetadataEntry::CatalogDdlAudited { payload: _, .. } => {
                // Opaque to the cluster crate. The host-side applier
                // decodes the payload and writes through to
                // `SystemCatalog`. We just count it — both DDL
                // shapes contribute to `catalog_entries_applied`.
                self.catalog_entries_applied += 1;
            }
            MetadataEntry::TopologyChange(change) => self.topology_log.push(change.clone()),
            MetadataEntry::RoutingChange(change) => self.routing_log.push(change.clone()),

            MetadataEntry::ClusterVersionBump { from, to } => {
                if *from != self.cluster_version && self.cluster_version != 0 {
                    warn!(
                        expected = self.cluster_version,
                        got = *from,
                        "cluster version bump mismatch"
                    );
                }
                self.cluster_version = *to;
            }

            MetadataEntry::DescriptorLeaseGrant(lease) => {
                if lease.expires_at > self.last_applied_hlc {
                    self.last_applied_hlc = lease.expires_at;
                }
                self.leases
                    .insert((lease.descriptor_id.clone(), lease.node_id), lease.clone());
            }
            MetadataEntry::DescriptorLeaseRelease {
                node_id,
                descriptor_ids,
            } => {
                for id in descriptor_ids {
                    self.leases.remove(&(id.clone(), *node_id));
                }
            }
            // Drain state is host-side (lives in
            // `nodedb::control::lease::DescriptorDrainTracker`);
            // the cluster-side cache only tracks lease state
            // directly. These no-op arms keep the exhaustive
            // match coverage so adding new variants is a
            // compile-time error here too.
            MetadataEntry::DescriptorDrainStart { expires_at, .. } => {
                if *expires_at > self.last_applied_hlc {
                    self.last_applied_hlc = *expires_at;
                }
            }
            MetadataEntry::DescriptorDrainEnd { .. } => {}
            MetadataEntry::CaTrustChange { .. } => {
                // CA trust mutations are host-side only: the production
                // applier in the nodedb crate writes/deletes
                // `tls/ca.d/<fp>.crt` and rebuilds the rustls config.
                // Cluster cache has nothing to track.
            }
            MetadataEntry::SyncProducerRegister { .. } => {
                // host-side only: the production applier calls
                // `SyncProducerRegistry::apply_register` on every node.
            }
            MetadataEntry::SyncProducerFence { .. } => {
                // host-side only: the production applier calls
                // `SyncProducerRegistry::apply_fence` on every node.
            }
            MetadataEntry::SurrogateAlloc { .. } => {
                // Surrogate HWM advance is host-side only: the production
                // applier calls `SurrogateRegistry::restore_hwm`. The
                // cluster cache has no surrogate state to track.
            }
            MetadataEntry::JoinTokenTransition { .. } => {
                // Token lifecycle transitions are enforced by the bootstrap-
                // listener handler at apply time. The cluster cache records
                // the applied index but carries no per-token state — the
                // host-side token store is authoritative.
            }
            MetadataEntry::Batch { entries } => {
                for sub in entries {
                    self.apply(index, sub);
                }
            }
            // Migration checkpoint/abort upserts are handled by the
            // live-state applier (`CacheApplier`) which holds the
            // `SharedMigrationStateTable` handle. The cache itself
            // has no migration state to track beyond applied_index.
            MetadataEntry::MigrationCheckpoint { .. } => {}
            MetadataEntry::MigrationAbort { .. } => {}
        }
    }
}

// ── Migration live-state application helpers ─────────────────────────────────

/// Apply a `MigrationCheckpoint` entry against the shared state table.
///
/// Returns `Err` on CRC32C mismatch (fatal — corruption is louder than data
/// loss) and on storage failures. Idempotent: if
/// `(migration_id, phase, attempt)` is already present, the call is a no-op.
pub fn apply_migration_checkpoint(
    table: &SharedMigrationStateTable,
    migration_id: uuid::Uuid,
    _phase: MigrationPhaseTag,
    attempt: u32,
    payload: crate::metadata_group::migration_state::MigrationCheckpointPayload,
    expected_crc: u32,
    ts_ms: u64,
) -> Result<(), ClusterError> {
    // Validate CRC32C against the encoded payload bytes.
    let actual_crc = payload.crc32c()?;
    if actual_crc != expected_crc {
        return Err(ClusterError::MigrationCheckpoint(
            MigrationCheckpointError::Crc32cMismatch {
                migration_id,
                expected: expected_crc,
                actual: actual_crc,
            },
        ));
    }

    let row = PersistedMigrationCheckpoint {
        migration_id: migration_id.hyphenated().to_string(),
        attempt,
        payload,
        crc32c: actual_crc,
        ts_ms,
    };

    let mut guard = table.lock().unwrap_or_else(|p| p.into_inner());
    guard.upsert(row)
}

/// Apply a `MigrationAbort` entry against the shared state table and
/// live routing table.
///
/// Applies each compensation in order; any failure is fatal (no
/// warn-and-continue — a partial abort is as broken as a partial commit).
/// On success, removes the migration row from the state table.
pub fn apply_migration_abort(
    table: &SharedMigrationStateTable,
    routing: Option<&std::sync::Arc<std::sync::RwLock<RoutingTable>>>,
    migration_id: uuid::Uuid,
    reason: &str,
    compensations: &[Compensation],
) -> Result<(), ClusterError> {
    info!(
        migration_id = %migration_id,
        reason,
        steps = compensations.len(),
        "applying migration abort"
    );

    for (step, comp) in compensations.iter().enumerate() {
        apply_compensation(routing, step, migration_id, comp)?;
    }

    let mut guard = table.lock().unwrap_or_else(|p| p.into_inner());
    guard.remove(&migration_id)
}

fn apply_compensation(
    routing: Option<&std::sync::Arc<std::sync::RwLock<RoutingTable>>>,
    step: usize,
    migration_id: uuid::Uuid,
    comp: &Compensation,
) -> Result<(), ClusterError> {
    let Some(live) = routing else {
        // No routing handle attached — log and treat as success.
        // This path is only reachable in unit tests without live state.
        debug!(
            migration_id = %migration_id,
            step,
            ?comp,
            "compensation: no live routing handle, skipping"
        );
        return Ok(());
    };

    let mut rt = live.write().unwrap_or_else(|p| p.into_inner());
    match comp {
        Compensation::RemoveLearner { group_id, peer_id }
        | Compensation::RemoveVoter { group_id, peer_id } => {
            rt.remove_group_member(*group_id, *peer_id);
        }
        Compensation::RestoreLeaderHint { group_id, peer_id } => {
            rt.set_leader(*group_id, *peer_id);
        }
        Compensation::RemoveGhostStub { vshard_id: _ } => {
            // Ghost stub removal is handled by the caller's ghost_table;
            // the routing table has no ghost state.
        }
    }
    drop(rt);

    debug!(
        migration_id = %migration_id,
        step,
        ?comp,
        "compensation applied"
    );
    Ok(())
}
