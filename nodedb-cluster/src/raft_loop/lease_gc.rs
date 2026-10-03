// SPDX-License-Identifier: BUSL-1.1

//! Periodic lease GC on the metadata-group leader.
//!
//! Mirrors `placement_reconcile`: leader-gated, throttled by tick count in
//! `tick::core::do_tick`. Sweeps `MetadataCache.leases` and proposes
//! `DescriptorLeaseRelease` for every holder that is no longer in the cluster
//! topology, or that is both SWIM-Dead and Raft-silent past its grace (see
//! [`crate::lease_liveness`]). This is the safety net behind the Leave apply
//! hook.

use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, warn};

use crate::forward::PlanExecutor;
use crate::lease_liveness::LeaseHolderLiveness;
use crate::metadata_group::cache::MetadataCache;
use crate::metadata_group::descriptors::DescriptorId;
use crate::topology::ClusterTopology;

use super::loop_core::{CommitApplier, RaftLoop};

/// Pure collection: `(node_id, descriptor_ids)` for every lease holder that
/// is not in `topology`, or whose leases `liveness` counts as released in
/// `leader_term` at `now`. `local_node` is never collected. Sorted by
/// `node_id` for deterministic proposal order.
pub(super) fn collect_stale_lease_releases(
    topology: &ClusterTopology,
    cache: &MetadataCache,
    liveness: &LeaseHolderLiveness,
    local_node: u64,
    leader_term: u64,
    now: Instant,
) -> Vec<(u64, Vec<DescriptorId>)> {
    let mut by_holder: HashMap<u64, Vec<DescriptorId>> = HashMap::new();
    for (id, holder) in cache.leases.keys() {
        if *holder == local_node {
            continue;
        }
        if !topology.contains(*holder)
            || liveness.dead_holder_released(*holder, Some(leader_term), now)
        {
            by_holder.entry(*holder).or_default().push(id.clone());
        }
    }
    let mut out: Vec<(u64, Vec<DescriptorId>)> = by_holder.into_iter().collect();
    out.sort_by_key(|(node_id, _)| *node_id);
    out
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// On the metadata-group leader, sample each peer's Raft contact, then
    /// propose `DescriptorLeaseRelease` for every lease whose holder left the
    /// topology, or stayed SWIM-Dead and Raft-silent past its grace.
    pub(super) fn gc_stale_node_leases(&self) {
        let Some(cache) = &self.metadata_cache else {
            return; // not wired (some tests) — nothing to sweep
        };
        let to_release: Vec<(u64, Vec<DescriptorId>)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let Some(sample) = mr.leader_peer_acks(crate::metadata_group::METADATA_GROUP_ID) else {
                self.lease_holder_liveness.clear_raft_contact();
                return;
            };
            let now = Instant::now();
            self.lease_holder_liveness
                .observe_raft_contact(&sample, now);
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            // A holder outside topology is released by membership alone.
            self.lease_holder_liveness.retain(|id| topo.contains(id));
            let cache = cache.read().unwrap_or_else(|p| p.into_inner());
            collect_stale_lease_releases(
                &topo,
                &cache,
                &self.lease_holder_liveness,
                self.node_id,
                sample.term,
                now,
            )
        };

        for (node_id, descriptor_ids) in to_release {
            let entry = crate::metadata_group::entry::MetadataEntry::DescriptorLeaseRelease {
                node_id,
                descriptor_ids,
            };
            let bytes = match crate::metadata_group::codec::encode_entry(&entry) {
                Ok(b) => b,
                Err(e) => {
                    warn!(node_id, error = %e, "lease GC: encode DescriptorLeaseRelease failed");
                    continue;
                }
            };
            match self.propose_stamped_to_metadata_group(&bytes) {
                Ok(idx) => debug!(
                    node_id,
                    log_index = idx,
                    "lease GC: released leases of non-member or dead node"
                ),
                Err(e) => {
                    warn!(node_id, error = %e, "lease GC: proposal failed; will be retried on next sweep")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata_group::cache::MetadataCache;
    use crate::metadata_group::descriptors::{DescriptorId, DescriptorKind};
    use std::time::{Duration, Instant};

    use crate::lease_liveness::{DEAD_HOLDER_LEASE_GRACE, LeaseHolderLiveness};
    use crate::multi_raft::PeerAckSample;
    use crate::topology::{ClusterTopology, NodeInfo, NodeState};

    use super::collect_stale_lease_releases;

    const LOCAL: u64 = 1;
    const TERM: u64 = 2;

    /// Collect with no SWIM Dead records.
    fn collect_non_member(
        topo: &ClusterTopology,
        cache: &MetadataCache,
    ) -> Vec<(u64, Vec<DescriptorId>)> {
        collect_stale_lease_releases(
            topo,
            cache,
            &LeaseHolderLiveness::new(),
            LOCAL,
            TERM,
            Instant::now(),
        )
    }

    fn topo_with(ids: &[u64]) -> ClusterTopology {
        let mut t = ClusterTopology::new();
        for (i, id) in ids.iter().enumerate() {
            let addr: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(NodeInfo::new(*id, addr, NodeState::Active));
        }
        t
    }

    fn lease(
        id: &DescriptorId,
        holder: u64,
    ) -> crate::metadata_group::descriptors::DescriptorLease {
        crate::metadata_group::descriptors::DescriptorLease {
            descriptor_id: id.clone(),
            version: 1,
            node_id: holder,
            expires_at: nodedb_types::Hlc::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64
                    + 60_000_000_000,
                0,
            ),
        }
    }

    #[test]
    fn gc_stale_node_leases_proposes_for_non_members_only() {
        let topo = topo_with(&[1]);
        let mut cache = MetadataCache::new();
        let orders = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        let metrics = DescriptorId::new(0, 1, DescriptorKind::Collection, "metrics".to_string());

        // Member holder 1: must NOT be collected.
        cache.leases.insert((orders.clone(), 1), lease(&orders, 1));
        // Non-member holder 2: must be collected.
        cache.leases.insert((orders.clone(), 2), lease(&orders, 2));
        // Non-member holder 3 with two descriptors.
        cache.leases.insert((orders.clone(), 3), lease(&orders, 3));
        cache
            .leases
            .insert((metrics.clone(), 3), lease(&metrics, 3));

        let collected = collect_non_member(&topo, &cache);
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].0, 2);
        assert_eq!(collected[0].1, vec![orders.clone()]);
        assert_eq!(collected[1].0, 3);
        assert_eq!(collected[1].1.len(), 2);
        assert!(collected[1].1.contains(&orders));
        assert!(collected[1].1.contains(&metrics));
    }

    #[test]
    fn gc_is_noop_when_all_holders_are_members() {
        let topo = topo_with(&[1, 2, 3]);
        let mut cache = MetadataCache::new();
        let orders = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        for holder in [1, 2, 3] {
            cache
                .leases
                .insert((orders.clone(), holder), lease(&orders, holder));
        }

        assert!(collect_non_member(&topo, &cache).is_empty());
    }

    #[test]
    fn gc_collects_empty_cache() {
        let topo = topo_with(&[1]);
        let cache = MetadataCache::new();
        assert!(collect_non_member(&topo, &cache).is_empty());
    }

    /// Leader samples showing holder 2 Raft-silent from `from` to `to`.
    fn raft_silent(liveness: &LeaseHolderLiveness, from: Instant, to: Instant) {
        let sample = PeerAckSample {
            term: TERM,
            acks: vec![(2, 5)],
        };
        liveness.observe_raft_contact(&sample, from);
        liveness.observe_raft_contact(&sample, to);
    }

    #[test]
    fn dead_member_holder_is_collected_only_after_the_grace() {
        let topo = topo_with(&[1, 2]);
        let mut cache = MetadataCache::new();
        let orders = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        cache.leases.insert((orders.clone(), 2), lease(&orders, 2));
        let liveness = LeaseHolderLiveness::new();

        let dead_at = Instant::now();
        let after_grace = dead_at + DEAD_HOLDER_LEASE_GRACE + Duration::from_millis(1);
        raft_silent(&liveness, dead_at, after_grace);
        liveness.record_dead_at(2, dead_at);
        assert!(
            collect_stale_lease_releases(&topo, &cache, &liveness, LOCAL, TERM, dead_at).is_empty()
        );

        let collected =
            collect_stale_lease_releases(&topo, &cache, &liveness, LOCAL, TERM, after_grace);
        assert_eq!(collected, vec![(2, vec![orders])]);
    }

    /// SWIM says Dead, but holder 2 answered the leader over Raft recently:
    /// it has not fenced, so its lease is not released.
    #[test]
    fn dead_holder_recently_acked_by_raft_is_kept() {
        let topo = topo_with(&[1, 2]);
        let mut cache = MetadataCache::new();
        let orders = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        cache.leases.insert((orders.clone(), 2), lease(&orders, 2));
        let liveness = LeaseHolderLiveness::new();

        let dead_at = Instant::now();
        let after_grace = dead_at + DEAD_HOLDER_LEASE_GRACE + Duration::from_millis(1);
        liveness.record_dead_at(2, dead_at);
        liveness.observe_raft_contact(
            &PeerAckSample {
                term: TERM,
                acks: vec![(2, 5)],
            },
            dead_at,
        );
        liveness.observe_raft_contact(
            &PeerAckSample {
                term: TERM,
                acks: vec![(2, 6)],
            },
            after_grace,
        );

        assert!(
            collect_stale_lease_releases(&topo, &cache, &liveness, LOCAL, TERM, after_grace)
                .is_empty()
        );
    }

    #[test]
    fn local_node_is_never_collected() {
        let topo = topo_with(&[2]);
        let mut cache = MetadataCache::new();
        let orders = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        cache
            .leases
            .insert((orders.clone(), LOCAL), lease(&orders, LOCAL));
        let liveness = LeaseHolderLiveness::new();
        let dead_at = Instant::now();
        liveness.record_dead_at(LOCAL, dead_at);

        let after_grace = dead_at + DEAD_HOLDER_LEASE_GRACE + Duration::from_millis(1);
        assert!(
            collect_stale_lease_releases(&topo, &cache, &liveness, LOCAL, TERM, after_grace)
                .is_empty()
        );
    }
}
