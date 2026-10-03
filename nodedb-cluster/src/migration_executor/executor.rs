// SPDX-License-Identifier: BUSL-1.1

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::info;
use uuid::Uuid;

use crate::catalog::ClusterCatalog;
use crate::decommission::MetadataProposer;
use crate::error::{ClusterError, Result};
use crate::ghost::GhostTable;
use crate::metadata_group::MetadataEntry as Entry;
use crate::metadata_group::migration_state::{
    MigrationCheckpointPayload, MigrationId, MigrationPhaseTag, SharedMigrationStateTable,
};
use crate::migration::{MigrationPhase, MigrationState};
use crate::multi_raft::MultiRaft;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;
use crate::transport::NexarTransport;

/// Configuration for a vShard migration.
#[derive(Debug, Clone)]
pub struct MigrationRequest {
    pub vshard_id: u32,
    pub source_node: u64,
    pub target_node: u64,
    /// Maximum allowed write pause during Phase 3 (microseconds).
    pub write_pause_budget_us: u64,
}

impl Default for MigrationRequest {
    fn default() -> Self {
        Self {
            vshard_id: 0,
            source_node: 0,
            target_node: 0,
            write_pause_budget_us: 500_000,
        }
    }
}

/// Result of a completed migration.
#[derive(Debug)]
pub struct MigrationResult {
    pub vshard_id: u32,
    pub source_node: u64,
    pub target_node: u64,
    pub phase: MigrationPhase,
    pub elapsed: Option<Duration>,
    pub migration_id: MigrationId,
}

/// Executes a vShard migration through the 3-phase protocol.
pub struct MigrationExecutor {
    pub(super) multi_raft: Arc<Mutex<MultiRaft>>,
    pub(super) routing: Arc<RwLock<RoutingTable>>,
    pub(super) topology: Arc<RwLock<ClusterTopology>>,
    pub(super) transport: Arc<NexarTransport>,
    pub(super) ghost_table: Arc<Mutex<GhostTable>>,
    pub(super) catalog: Option<Arc<ClusterCatalog>>,
    pub(super) metadata_proposer: Option<Arc<dyn MetadataProposer>>,
    pub(super) migration_state: Option<SharedMigrationStateTable>,
    /// Where each migration this executor runs reports its state.
    pub(super) tracker: Arc<super::MigrationTracker>,
}

impl MigrationExecutor {
    pub fn new(
        multi_raft: Arc<Mutex<MultiRaft>>,
        routing: Arc<RwLock<RoutingTable>>,
        topology: Arc<RwLock<ClusterTopology>>,
        transport: Arc<NexarTransport>,
    ) -> Self {
        Self {
            multi_raft,
            routing,
            topology,
            transport,
            ghost_table: Arc::new(Mutex::new(GhostTable::new())),
            catalog: None,
            metadata_proposer: None,
            migration_state: None,
            tracker: Arc::new(super::MigrationTracker::new()),
        }
    }

    /// Report every migration's state to `tracker`, which `SHOW MIGRATIONS`
    /// and the status route read.
    pub fn with_tracker(mut self, tracker: Arc<super::MigrationTracker>) -> Self {
        self.tracker = tracker;
        self
    }

    pub fn with_metadata_proposer(mut self, proposer: Arc<dyn MetadataProposer>) -> Self {
        self.metadata_proposer = Some(proposer);
        self
    }

    pub fn with_catalog(mut self, catalog: Arc<ClusterCatalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    pub fn with_migration_state(mut self, state: SharedMigrationStateTable) -> Self {
        self.migration_state = Some(state);
        self
    }

    pub fn ghost_table(&self) -> &Arc<Mutex<GhostTable>> {
        &self.ghost_table
    }

    pub async fn execute(&self, req: MigrationRequest) -> Result<MigrationResult> {
        let source_group = {
            let routing = self.routing.read().unwrap_or_else(|p| p.into_inner());
            routing.group_for_vshard(req.vshard_id)?
        };

        if let Some(state_table) = &self.migration_state {
            let guard = state_table.lock().unwrap_or_else(|p| p.into_inner());
            for row in guard.all_checkpoints() {
                if let Some(_id) = row.migration_uuid() {
                    let vshard_matches = match &row.payload {
                        MigrationCheckpointPayload::AddLearner { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::CatchUp { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::PromoteLearner { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::LeadershipTransfer { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::Cutover { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::Complete { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                    };
                    if vshard_matches && row.payload.phase_tag() != MigrationPhaseTag::Complete {
                        return Err(ClusterError::MigrationInProgress {
                            vshard_id: req.vshard_id,
                        });
                    }
                }
            }
        }

        let migration_id = Uuid::new_v4();

        let mut state = MigrationState::new(
            req.vshard_id,
            source_group,
            source_group,
            req.source_node,
            req.target_node,
            req.write_pause_budget_us,
        );

        info!(
            vshard = req.vshard_id,
            source = req.source_node,
            target = req.target_node,
            group = source_group,
            migration_id = %migration_id,
            "starting vShard migration"
        );

        self.tracker.record(migration_id, &state);
        let phases = self
            .run_phases(&mut state, source_group, &req, migration_id)
            .await;
        // The final state, a failure included, stays visible until GC.
        self.tracker.record(migration_id, &state);
        phases?;

        let elapsed = state.elapsed();
        let phase = state.phase().clone();

        info!(
            vshard = req.vshard_id,
            migration_id = %migration_id,
            elapsed_ms = elapsed.map(|d| d.as_millis() as u64).unwrap_or(0),
            "vShard migration completed"
        );

        Ok(MigrationResult {
            vshard_id: req.vshard_id,
            source_node: req.source_node,
            target_node: req.target_node,
            phase,
            elapsed,
            migration_id,
        })
    }

    /// Run the three phases, recording the state after each.
    async fn run_phases(
        &self,
        state: &mut MigrationState,
        source_group: u64,
        req: &MigrationRequest,
        migration_id: MigrationId,
    ) -> Result<()> {
        super::phases::phase1_base_copy(self, state, source_group, req, migration_id).await?;
        self.tracker.record(migration_id, state);
        super::phases::phase2_wal_catchup(self, state, source_group, req, migration_id).await?;
        self.tracker.record(migration_id, state);
        super::phases::phase3_cutover(self, state, source_group, req, migration_id).await
    }

    pub(super) async fn propose_checkpoint(
        &self,
        migration_id: MigrationId,
        attempt: u32,
        payload: MigrationCheckpointPayload,
    ) -> Result<()> {
        let Some(proposer) = &self.metadata_proposer else {
            return Ok(());
        };

        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let crc32c = payload.crc32c()?;
        let phase = payload.phase_tag();

        let entry = Entry::MigrationCheckpoint {
            migration_id: migration_id.hyphenated().to_string(),
            phase,
            attempt,
            payload,
            crc32c,
            ts_ms,
        };

        proposer.propose_and_wait(entry).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_request_default() {
        let req = MigrationRequest::default();
        assert_eq!(req.write_pause_budget_us, 500_000);
    }
}
