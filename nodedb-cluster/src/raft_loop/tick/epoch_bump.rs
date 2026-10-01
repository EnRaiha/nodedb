// SPDX-License-Identifier: BUSL-1.1

//! Proposing a new cluster generation on metadata-group leadership acquisition.

use crate::catalog::ClusterCatalog;
use crate::cluster_epoch::ClusterEpochState;
use crate::error::Result;
use crate::forward::PlanExecutor;
use crate::metadata_group::codec::encode_entry;
use crate::metadata_group::entry::MetadataEntry;
use crate::raft_loop::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Propose the next cluster epoch after winning metadata-group leadership.
    ///
    /// A leadership change is the event the epoch exists to mark: whatever the
    /// previous leader was in the middle of, the cluster's topology view has a
    /// new authority. Proposing the bump — rather than incrementing a local
    /// counter — is what lets every node arrive at the same number by applying
    /// the same entry.
    ///
    /// The new epoch takes effect on this node only when the entry commits and
    /// the applier advances the applied mark, exactly as it does on every other
    /// node. A leader that proposes and then loses leadership before the entry
    /// commits simply never advances, which is the correct outcome.
    ///
    /// Failure to propose is logged, not fatal: the next leadership acquisition
    /// proposes again, and until then every node keeps operating on the last
    /// generation they all agreed on.
    pub(super) fn propose_cluster_epoch_bump(&self) {
        let next = self.cluster_epoch.applied() + 1;
        let entry = MetadataEntry::ClusterEpochBump { epoch: next };
        let bytes = match encode_entry(&entry) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    node = self.node_id,
                    epoch = next,
                    error = %e,
                    "could not encode cluster epoch bump"
                );
                return;
            }
        };
        match self.propose_to_metadata_group(bytes) {
            Ok(index) => tracing::info!(
                node = self.node_id,
                epoch = next,
                log_index = index,
                "proposed cluster epoch bump on metadata-group leadership acquisition"
            ),
            Err(e) => tracing::warn!(
                node = self.node_id,
                epoch = next,
                error = %e,
                "could not propose cluster epoch bump; the next acquisition retries"
            ),
        }
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Adopt the epoch bumps `entry` carries, committed at `index`.
    ///
    /// The caller runs this only after every earlier entry of the batch has
    /// applied, so no epoch lands ahead of an entry the applier stopped at.
    ///
    /// The epoch is persisted to the catalog, so the adoption runs on a
    /// blocking thread and the metadata lane awaits it.
    pub(super) async fn adopt_cluster_epoch(
        &self,
        entry: &MetadataEntry,
        index: u64,
    ) -> Result<()> {
        let state = std::sync::Arc::clone(&self.cluster_epoch);
        let catalog = self.catalog.clone();
        let node_id = self.node_id;
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            adopt_entry_epoch(&state, catalog.as_deref(), node_id, &entry, index)
        })
        .await
        .map_err(|e| crate::error::ClusterError::Storage {
            detail: format!("cluster epoch adoption task at log index {index}: {e}"),
        })?
    }
}

/// Whether `entry` carries a [`MetadataEntry::ClusterEpochBump`], directly or
/// nested in a batch or a prepared DDL.
pub(super) fn carries_epoch(entry: &MetadataEntry) -> bool {
    match entry {
        MetadataEntry::ClusterEpochBump { .. } => true,
        MetadataEntry::Batch { entries } => entries.iter().any(carries_epoch),
        MetadataEntry::DdlPrepared { entry, .. } => carries_epoch(entry),
        _ => false,
    }
}

/// Adopt every epoch bump `entry` carries.
///
/// Applying the entry is the moment the generation becomes this node's own:
/// before it, the number was something a peer asserted. The epoch is persisted
/// before the in-memory mark advances, so the applied mark is always durable.
/// A bump at or below the applied mark is already durable and is not written.
fn adopt_entry_epoch(
    state: &ClusterEpochState,
    catalog: Option<&ClusterCatalog>,
    node_id: u64,
    entry: &MetadataEntry,
    index: u64,
) -> Result<()> {
    match entry {
        MetadataEntry::ClusterEpochBump { epoch } => {
            if *epoch > state.applied() {
                if let Some(catalog) = catalog {
                    crate::cluster_epoch::persist_applied_epoch(catalog, *epoch)?;
                }
                state.advance_applied(*epoch);
            }
            tracing::info!(
                node = node_id,
                epoch = *epoch,
                log_index = index,
                "applied cluster epoch"
            );
            Ok(())
        }
        MetadataEntry::Batch { entries } => {
            for sub in entries {
                adopt_entry_epoch(state, catalog, node_id, sub, index)?;
            }
            Ok(())
        }
        MetadataEntry::DdlPrepared { entry, .. } => {
            adopt_entry_epoch(state, catalog, node_id, entry, index)
        }
        _ => Ok(()),
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// A handle to this node's cluster-epoch state, for callers that need to
    /// know whether the node is operating on a superseded topology view.
    pub fn cluster_epoch_handle(&self) -> std::sync::Arc<crate::cluster_epoch::ClusterEpochState> {
        std::sync::Arc::clone(&self.cluster_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::codec::decode_entry;

    /// Two nodes in one process must hold independent generations. A single
    /// process-wide counter would alias them and no disagreement could ever be
    /// represented, let alone tested.
    #[test]
    fn nodes_sharing_a_process_hold_separate_generations() {
        let a = ClusterEpochState::new(0);
        let b = ClusterEpochState::new(0);
        a.advance_applied(4);
        assert_eq!(a.applied(), 4);
        assert_eq!(b.applied(), 0, "one node's apply is not another's");
    }

    /// The generation a node reports is the one it applied from the log, so a
    /// bump that has been proposed but not yet committed changes nothing.
    #[test]
    fn a_proposed_bump_does_not_advance_anyone() {
        let state = ClusterEpochState::new(2);
        let _entry = MetadataEntry::ClusterEpochBump { epoch: 3 };
        assert_eq!(
            state.applied(),
            2,
            "only applying the committed entry advances the generation"
        );
    }

    /// A committed bump round-trips through the metadata entry codec, so every
    /// node decodes the same generation from the same bytes.
    #[test]
    fn a_bump_survives_the_metadata_entry_codec() {
        let bytes = encode_entry(&MetadataEntry::ClusterEpochBump { epoch: 11 }).unwrap();
        match decode_entry(&bytes).unwrap() {
            MetadataEntry::ClusterEpochBump { epoch } => assert_eq!(epoch, 11),
            other => panic!("expected a cluster epoch bump, got {other:?}"),
        }
    }

    /// A bump packed inside an atomic batch still advances the generation —
    /// otherwise a transactional DDL carrying one would silently drop it.
    #[test]
    fn a_bump_nested_in_a_batch_still_counts() {
        let batch = MetadataEntry::Batch {
            entries: vec![
                MetadataEntry::CatalogDdl {
                    payload: b"unrelated".to_vec(),
                },
                MetadataEntry::ClusterEpochBump { epoch: 8 },
            ],
        };
        let bytes = encode_entry(&batch).unwrap();
        let decoded = decode_entry(&bytes).unwrap();
        let mut found = None;
        if let MetadataEntry::Batch { entries } = &decoded {
            for sub in entries {
                if let MetadataEntry::ClusterEpochBump { epoch } = sub {
                    found = Some(*epoch);
                }
            }
        }
        assert_eq!(found, Some(8), "a nested bump must be reachable");
    }

    fn bump(epoch: u64) -> MetadataEntry {
        MetadataEntry::ClusterEpochBump { epoch }
    }

    /// A failed epoch persist leaves the in-memory mark and the stored epoch
    /// where they were. The retry persists and advances.
    #[test]
    fn epoch_persist_error_does_not_advance() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap();
        let state = ClusterEpochState::new(1);

        adopt_entry_epoch(&state, Some(&catalog), 1, &bump(2), 10).unwrap();
        catalog.fail_next_epoch_write_for_test();
        assert!(adopt_entry_epoch(&state, Some(&catalog), 1, &bump(3), 11).is_err());
        assert_eq!(state.applied(), 2);
        assert_eq!(catalog.load_cluster_epoch().unwrap(), Some(2));

        adopt_entry_epoch(&state, Some(&catalog), 1, &bump(3), 11).unwrap();
        assert_eq!(state.applied(), 3);
        assert_eq!(catalog.load_cluster_epoch().unwrap(), Some(3));
    }

    /// Replaying an older bump never lowers the persisted epoch.
    #[test]
    fn replayed_older_bump_keeps_the_persisted_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap();
        let state = ClusterEpochState::new(0);
        adopt_entry_epoch(&state, Some(&catalog), 1, &bump(7), 5).unwrap();
        adopt_entry_epoch(&state, Some(&catalog), 1, &bump(3), 2).unwrap();
        assert_eq!(state.applied(), 7);
        assert_eq!(catalog.load_cluster_epoch().unwrap(), Some(7));
    }

    #[test]
    fn nested_bumps_are_detected() {
        let batch = MetadataEntry::Batch {
            entries: vec![MetadataEntry::CatalogDdl { payload: vec![] }, bump(4)],
        };
        assert!(carries_epoch(&batch));
        assert!(!carries_epoch(&MetadataEntry::CatalogDdl {
            payload: vec![]
        }));
    }
}
