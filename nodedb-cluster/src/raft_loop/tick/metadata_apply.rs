// SPDX-License-Identifier: BUSL-1.1

//! Metadata group (0) apply: cluster epochs adopted in log order between
//! applier calls, then the durable applied floor and log compaction.

use nodedb_raft::LogEntry;
use tracing::warn;

use crate::conf_change::ConfChange;
use crate::forward::PlanExecutor;
use crate::metadata_group::METADATA_GROUP_ID;
use crate::metadata_group::applier::CommittedMetadata;
use crate::metadata_group::entry::MetadataEntry;

use super::super::loop_core::{CommitApplier, RaftLoop};
use super::epoch_bump::carries_epoch;

/// Group 0's committed entries, each decoded once.
struct MetadataBatch<'a> {
    /// A conf change is an empty entry: the applier advances its watermark
    /// past it and applies nothing.
    commits: Vec<CommittedMetadata<'a>>,
    /// The index of the first entry whose apply changes the live routing
    /// table.
    first_routing_change: Option<u64>,
}

/// Whether `entry` changes the live routing table when it applies.
fn touches_routing(entry: &MetadataEntry) -> bool {
    match entry {
        MetadataEntry::RoutingChange(_) => true,
        MetadataEntry::Batch { entries } => entries.iter().any(touches_routing),
        MetadataEntry::DdlPrepared { entry, .. } => touches_routing(entry),
        _ => false,
    }
}

/// Decode each of `entries` once.
fn decode_batch(entries: &[LogEntry]) -> MetadataBatch<'_> {
    let mut first_routing_change = None;
    let commits = entries
        .iter()
        .map(|entry| {
            let (commit, touches) = if ConfChange::from_entry_data(&entry.data).is_some() {
                (CommittedMetadata::empty(entry.index), true)
            } else {
                let commit = CommittedMetadata::decode(entry.index, &entry.data);
                let touches = commit.entry().is_some_and(touches_routing);
                (commit, touches)
            };
            if touches && first_routing_change.is_none() {
                first_routing_change = Some(entry.index);
            }
            commit
        })
        .collect();
    MetadataBatch {
        commits,
        first_routing_change,
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Apply group 0's committed `entries`. Conf changes are already applied
    /// to the membership. Returns the highest index delivered, or 0.
    ///
    /// When the applier reports durable effects, the returned index is also
    /// saved as the group's applied floor, after the routing table it changed.
    pub(super) async fn apply_metadata_commits(&self, entries: &[LogEntry]) -> u64 {
        let batch = decode_batch(entries);
        let delivered = self.apply_in_epoch_order(&batch.commits).await;
        if self.metadata_applier.durable_effects() {
            self.save_metadata_floor(batch.first_routing_change, delivered)
                .await;
        }
        delivered
    }

    /// Hand `commits` to the applier in runs that end before each epoch
    /// bump. A bump is adopted only after every earlier entry applied, so no
    /// epoch lands past an entry the applier stopped at.
    async fn apply_in_epoch_order(&self, commits: &[CommittedMetadata<'_>]) -> u64 {
        let mut delivered = 0u64;
        let mut run_start = 0usize;
        for (position, commit) in commits.iter().enumerate() {
            let Some(epoch_entry) = commit.entry().filter(|e| carries_epoch(e)) else {
                continue;
            };
            if !self
                .apply_run(&commits[run_start..position], &mut delivered)
                .await
            {
                return delivered;
            }
            // The bump entry itself opens the next run.
            run_start = position;
            if let Err(e) = self.adopt_cluster_epoch(epoch_entry, commit.index).await {
                warn!(
                    node = self.node_id,
                    log_index = commit.index,
                    error = %e,
                    "could not persist a committed cluster epoch; the entry is re-delivered"
                );
                return delivered;
            }
        }
        self.apply_run(&commits[run_start..], &mut delivered).await;
        delivered
    }

    /// Apply `run` and raise `delivered` to what the applier reports.
    /// Returns whether every entry of the run applied.
    async fn apply_run(&self, run: &[CommittedMetadata<'_>], delivered: &mut u64) -> bool {
        let Some(through) = run.last().map(|commit| commit.index) else {
            return true;
        };
        let applied = self.metadata_applier.apply_decoded(run).await;
        if applied > *delivered {
            *delivered = applied;
        }
        applied == through
    }

    /// Save the durable applied floor for group 0, then compact the log up
    /// to it when the configured threshold is reached.
    ///
    /// The floor is `delivered`, lowered below the first routing change of
    /// the batch when the routing table cannot be persisted. It never passes
    /// an entry whose effects are not durable.
    ///
    /// Every disk write runs off the async threads: the routing save through
    /// the routing persister, the floor and the compaction on a blocking
    /// thread. The lane awaits them. The tick never does.
    async fn save_metadata_floor(&self, first_routing_change: Option<u64>, delivered: u64) {
        if delivered == 0 {
            return;
        }
        let mut floor = delivered;
        if let Some(first) = first_routing_change.filter(|index| *index <= delivered)
            && let Some(persister) = self.routing_persister.as_ref()
            && !persister.wait(persister.request()).await
        {
            warn!(
                node = self.node_id,
                log_index = first,
                "could not persist the routing table; the applied floor stays below the change"
            );
            floor = first.saturating_sub(1);
        }
        if floor == 0 {
            return;
        }
        let multi_raft = std::sync::Arc::clone(&self.multi_raft);
        let node_id = self.node_id;
        let saved = tokio::task::spawn_blocking(move || {
            let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if let Err(e) = mr.save_applied_index(METADATA_GROUP_ID, floor) {
                warn!(
                    node = node_id,
                    floor,
                    error = %e,
                    "could not save the metadata applied floor; a restart replays from the previous floor"
                );
                return;
            }
            // Entries at or below the floor are durable in the host state,
            // and a peer that needs them catches up from a group 0 snapshot.
            if let Err(e) = mr.maybe_compact_group(METADATA_GROUP_ID, floor) {
                warn!(node = node_id, floor, error = %e, "group 0 log compaction failed");
            }
        })
        .await;
        if let Err(e) = saved {
            warn!(node = self.node_id, floor, error = %e, "metadata floor save task failed");
        }
    }
}
