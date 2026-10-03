// SPDX-License-Identifier: BUSL-1.1

//! The vShards a cross-shard graph read observed, for the transaction
//! read-set.
//!
//! A coordinator notes each vShard its legs read, at the watermark the
//! serving node reported, then publishes the log once the read finishes
//! (`session::graph_reads`). The request's protocol records the published
//! reads into the transaction read-set, so commit validation checks every
//! vShard the read depended on. A read that runs on one node only (no
//! cluster) publishes nothing: its cores' watermarks sit in this node's WAL,
//! which single-shard SI already compares against.

use std::collections::BTreeMap;

use crate::control::server::shared::session::graph_reads::{
    GraphShardReads, ShardObservation, note,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

/// Every vShard a graph read observed, each at its earliest served watermark
/// and with the node that served it.
#[derive(Debug, Default)]
pub(crate) struct ShardReadLog {
    served: BTreeMap<u32, Served>,
}

/// One vShard's observation: the earliest watermark, and the node whose WAL
/// numbers it (`0` once two nodes served the vShard).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Served {
    pub(crate) watermark: Lsn,
    pub(crate) node: u64,
}

impl ShardReadLog {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Note that `node` served `vshards` at `watermark`. A vShard read twice
    /// keeps its earlier watermark: the first observation is the one the
    /// result rests on. A vShard two nodes served has no one node whose
    /// versions compare with the read, so its node becomes `0`.
    pub(crate) fn note(
        &mut self,
        vshards: impl IntoIterator<Item = u32>,
        watermark: Lsn,
        node: u64,
    ) {
        for vshard in vshards {
            self.served
                .entry(vshard)
                .and_modify(|seen| {
                    seen.watermark = seen.watermark.min(watermark);
                    if seen.node != node {
                        seen.node = 0;
                    }
                })
                .or_insert(Served { watermark, node });
        }
    }

    /// Note one leg's response from `node`: `vshards` read at the highest
    /// watermark the leg reported.
    pub(crate) fn note_leg(
        &mut self,
        vshards: impl IntoIterator<Item = u32>,
        watermarks: &[(VShardId, Lsn)],
        node: u64,
    ) {
        let watermark = watermarks
            .iter()
            .map(|(_, lsn)| *lsn)
            .max()
            .unwrap_or(Lsn::ZERO);
        self.note(vshards, watermark, node);
    }

    /// Fold `other` into this log.
    pub(crate) fn merge(&mut self, other: ShardReadLog) {
        for (vshard, served) in other.served {
            self.note([vshard], served.watermark, served.node);
        }
    }

    /// Hand the log to the running request for its transaction read-set.
    /// `collection` is the database-qualified collection the read scoped, or
    /// `None` when it walked every collection. Without a cluster the log is
    /// dropped (see the module doc).
    pub(crate) fn publish(
        self,
        state: &SharedState,
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: Option<String>,
    ) {
        if state.cluster_routing.is_none() {
            return;
        }
        note(GraphShardReads {
            tenant_id,
            database_id,
            collection,
            shards: self
                .served
                .into_iter()
                .map(|(vshard, served)| ShardObservation {
                    vshard: VShardId::new(vshard),
                    watermark: served.watermark,
                    node: served.node,
                })
                .collect(),
        });
    }

    #[cfg(test)]
    pub(crate) fn served(&self) -> &BTreeMap<u32, Served> {
        &self.served
    }
}

/// The stored, database-qualified name of `bare` in `database_id`.
pub(crate) fn qualified(database_id: DatabaseId, bare: &str) -> String {
    nodedb_types::QualifiedCollection::new(database_id, bare)
        .as_str()
        .to_owned()
}

/// The key vShard of each node in `nodes`.
pub(crate) fn key_vshards<'a>(nodes: impl IntoIterator<Item = &'a String>) -> Vec<u32> {
    nodes
        .into_iter()
        .map(|node| VShardId::from_key(node.as_bytes()).as_u32())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn watermark(log: &ShardReadLog, vshard: u32) -> Option<Lsn> {
        log.served().get(&vshard).map(|served| served.watermark)
    }

    #[test]
    fn a_vshard_read_twice_keeps_its_earliest_watermark() {
        let mut log = ShardReadLog::new();
        log.note([3, 4], Lsn::new(20), 1);
        log.note([3], Lsn::new(10), 1);
        log.note([4], Lsn::new(30), 1);
        assert_eq!(watermark(&log, 3), Some(Lsn::new(10)));
        assert_eq!(watermark(&log, 4), Some(Lsn::new(20)));
        assert_eq!(log.served().get(&3).map(|s| s.node), Some(1));
    }

    #[test]
    fn a_vshard_two_nodes_served_has_no_node() {
        let mut log = ShardReadLog::new();
        log.note([3], Lsn::new(20), 1);
        log.note([3], Lsn::new(30), 2);
        assert_eq!(log.served().get(&3).map(|s| s.node), Some(0));
    }

    #[test]
    fn a_leg_is_noted_at_its_highest_reported_watermark() {
        let mut log = ShardReadLog::new();
        log.note_leg(
            [7],
            &[
                (VShardId::new(1), Lsn::new(5)),
                (VShardId::new(2), Lsn::new(9)),
            ],
            2,
        );
        assert_eq!(watermark(&log, 7), Some(Lsn::new(9)));
    }

    #[test]
    fn merging_keeps_the_earliest_watermark_per_vshard() {
        let mut left = ShardReadLog::new();
        left.note([1], Lsn::new(8), 1);
        let mut right = ShardReadLog::new();
        right.note([1], Lsn::new(4), 1);
        right.note([2], Lsn::new(6), 1);
        left.merge(right);
        assert_eq!(watermark(&left, 1), Some(Lsn::new(4)));
        assert_eq!(watermark(&left, 2), Some(Lsn::new(6)));
    }
}
