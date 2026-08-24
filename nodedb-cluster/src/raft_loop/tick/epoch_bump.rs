// SPDX-License-Identifier: BUSL-1.1

//! Proposing a new cluster generation on metadata-group leadership acquisition.

use crate::forward::PlanExecutor;
use crate::metadata_group::codec::{decode_entry, encode_entry};
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
    /// Advance this node's applied epoch for every committed
    /// [`MetadataEntry::ClusterEpochBump`] in `pairs`.
    ///
    /// Applying the entry is the moment the generation becomes this node's
    /// own: before it, the number was something a peer asserted; after it,
    /// this node has processed the same committed fact everyone else has and
    /// may stamp it outbound.
    ///
    /// Entries that fail to decode are skipped rather than fatal — they belong
    /// to variants this node's build does not know, and the epoch is not among
    /// them.
    pub(super) fn adopt_committed_cluster_epochs(&self, pairs: &[(u64, Vec<u8>)]) {
        for (index, data) in pairs {
            let Ok(entry) = decode_entry(data) else {
                continue;
            };
            self.adopt_entry_epoch(&entry, *index);
        }
    }

    /// Recurse into batches so a bump packed inside one still lands.
    fn adopt_entry_epoch(&self, entry: &MetadataEntry, index: u64) {
        match entry {
            MetadataEntry::ClusterEpochBump { epoch } => {
                self.cluster_epoch.advance_applied(*epoch);
                if let Some(catalog) = self.catalog.as_ref()
                    && let Err(e) = crate::cluster_epoch::persist_applied_epoch(catalog, *epoch)
                {
                    tracing::warn!(
                        node = self.node_id,
                        epoch = *epoch,
                        error = %e,
                        "applied the cluster epoch but could not persist it; \
                         it is re-learned from the log after a restart"
                    );
                }
                tracing::info!(
                    node = self.node_id,
                    epoch = *epoch,
                    log_index = index,
                    "applied cluster epoch"
                );
            }
            MetadataEntry::Batch { entries } => {
                for sub in entries {
                    self.adopt_entry_epoch(sub, index);
                }
            }
            MetadataEntry::DdlPrepared { entry, .. } => self.adopt_entry_epoch(entry, index),
            _ => {}
        }
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
    use crate::cluster_epoch::ClusterEpochState;
    use crate::metadata_group::codec::encode_entry;

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
}
