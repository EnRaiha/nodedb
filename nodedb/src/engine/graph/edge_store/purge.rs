// SPDX-License-Identifier: BUSL-1.1

//! Tenant- and collection-scoped edge purge.
//!
//! Structural range deletes on both `EDGES` and `REVERSE_EDGES`. No
//! lexical-prefix scans at the tenant boundary — tenant is the first
//! tuple component. Collection purge uses the `"{collection}\x00"`
//! prefix on the composite string, exploiting the
//! `collection\x00src\x00label\x00dst` layout of the composite key.

use std::collections::HashSet;

use nodedb_types::TenantId;
use redb::KeyRange;

use super::stats::table::{GRAPH_STATS, collection_stat_prefix};
use super::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};
use super::temporal::parse_versioned_edge_key;
use super::temporal::visibility::{EDGE_APPLIED, EDGE_CUTS};
use crate::types::{HomedRecord, RecordHomes};

/// Remove every entry of `table` inside `range`, returning how many went.
///
/// One pass over the range: `extract_from_if` yields each entry and removes it
/// as it is read, so nothing is materialized and no key is descended to twice.
/// The shape this replaces collected every key in the range into a `Vec` and
/// then called `remove` per key — holding the whole range in memory and paying
/// a fresh root-to-leaf descent for each removal, on a path whose entire job is
/// to empty that range.
///
/// `what` names the table for the error message; a purge that fails partway
/// should say which table it was draining.
fn drain_range<'a, K, V>(
    table: &mut redb::Table<'_, K, V>,
    range: impl KeyRange<'a, K>,
    what: &'static str,
) -> crate::Result<usize>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    let mut removed = 0usize;
    let drained = table
        .extract_from_if(range, |_, _| true)
        .map_err(|e| redb_err(what, e))?;
    for entry in drained {
        entry.map_err(|e| redb_err(what, e))?;
        removed += 1;
    }
    Ok(removed)
}

/// Remove every entry of `table` whose versioned edge key has an endpoint
/// home in `vshards`, returning how many went. The forward and the reverse
/// key name the same two endpoints, so one test serves every table keyed by
/// either. A key that does not parse names no home and stays.
fn drain_homed(
    table: &mut redb::Table<'_, (u64, u64, &'static str), impl redb::Value + 'static>,
    vshards: &HashSet<u32>,
    what: &'static str,
) -> crate::Result<usize> {
    let mut removed = 0usize;
    let drained = table
        .extract_from_if((0u64, 0u64, "").., |key, _| {
            parse_versioned_edge_key(key.2).is_some_and(|(_, src, _, dst, _)| {
                RecordHomes::of(HomedRecord::Edge { src, dst }).intersects(vshards)
            })
        })
        .map_err(|e| redb_err(what, e))?;
    for entry in drained {
        entry.map_err(|e| redb_err(what, e))?;
        removed += 1;
    }
    Ok(removed)
}

impl EdgeStore {
    /// Purge every edge version, in every database, tenant and collection,
    /// with an endpoint home in `vshards`: its forward and reverse keys and
    /// its applied ordinal. Returns the number of forward versions removed.
    ///
    /// A data-group snapshot carries exactly these edges, so its install
    /// replaces them and nothing else. An edge lives on its endpoints'
    /// vShards, which need not be in the group that homes its collection.
    /// TRUNCATE cuts are collection facts every replica applies, so they
    /// stay. The `GRAPH_STATS` counters are not split by vShard and stay.
    pub fn purge_homed(&self, vshards: &HashSet<u32>) -> crate::Result<usize> {
        if vshards.is_empty() {
            return Ok(0);
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        let removed;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            removed = drain_homed(&mut edges, vshards, "homed edge purge")?;
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            drain_homed(&mut rev_t, vshards, "homed reverse edge purge")?;
            let mut applied = write_txn
                .open_table(EDGE_APPLIED)
                .map_err(|e| redb_err("open edge_applied", e))?;
            drain_homed(&mut applied, vshards, "homed edge applied purge")?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit homed edge purge", e))?;
        Ok(removed)
    }

    /// Purge all edges belonging to a `(database, tenant)`. O(tenant-size)
    /// range delete — no cross-tenant or cross-database scan.
    pub fn purge_tenant(&self, db: u64, tid: TenantId) -> crate::Result<usize> {
        let t = tid.as_u64();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        let mut removed = 0;

        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            removed += drain_range(&mut edges, (db, t, "")..(db, t + 1, ""), "edge purge")?;
        }

        {
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            removed += drain_range(
                &mut rev_t,
                (db, t, "")..(db, t + 1, ""),
                "reverse edge purge",
            )?;
        }

        // Clear the GRAPH_STATS rows for the whole tenant too — otherwise a
        // tenant purge removes the edges but orphans the persistent stats
        // counters (read by `SHOW GRAPH STATS`), which are a separate summary
        // table rather than being derived on the fly from EDGES. GRAPH_STATS
        // shares the `(db, tenant, key)` tuple layout, so the tenant range is
        // built exactly like the EDGES/REVERSE_EDGES ranges above.
        {
            let mut stats_t = write_txn
                .open_table(GRAPH_STATS)
                .map_err(|e| redb_err("open graph_stats", e))?;
            drain_range(
                &mut stats_t,
                (db, t, "")..(db, t + 1, ""),
                "graph stats purge",
            )?;
            let mut cuts = write_txn
                .open_table(EDGE_CUTS)
                .map_err(|e| redb_err("open edge_cuts", e))?;
            drain_range(&mut cuts, (db, t, "")..(db, t + 1, ""), "edge cut purge")?;
            let mut applied = write_txn
                .open_table(EDGE_APPLIED)
                .map_err(|e| redb_err("open edge_applied", e))?;
            drain_range(
                &mut applied,
                (db, t, "")..(db, t + 1, ""),
                "edge applied purge",
            )?;
        }

        write_txn
            .commit()
            .map_err(|e| redb_err("commit tenant purge", e))?;
        Ok(removed)
    }

    /// Purge all edges belonging to a specific collection within a
    /// `(database, tenant)`. Returns the number of forward edges removed.
    pub fn purge_collection(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<usize> {
        let t = tid.as_u64();
        let prefix = format!("{collection}\x00");
        let prefix_end = format!("{collection}\x01");

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        let mut removed = 0;

        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            removed += drain_range(
                &mut edges,
                (db, t, prefix.as_str())..(db, t, prefix_end.as_str()),
                "edge purge",
            )?;
        }

        {
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            drain_range(
                &mut rev_t,
                (db, t, prefix.as_str())..(db, t, prefix_end.as_str()),
                "reverse edge purge",
            )?;
        }

        // Clear the GRAPH_STATS summary + per-label rows for this collection
        // too — otherwise a hard purge removes the edges but the persistent
        // stats counters (read by `SHOW GRAPH STATS`) survive, since stats
        // are a separate summary table, not derived on the fly from EDGES.
        {
            let stats_prefix = collection_stat_prefix(collection);
            let stats_prefix_end = format!("{collection}\x01");
            let mut stats_t = write_txn
                .open_table(GRAPH_STATS)
                .map_err(|e| redb_err("open graph_stats", e))?;
            drain_range(
                &mut stats_t,
                (db, t, stats_prefix.as_str())..(db, t, stats_prefix_end.as_str()),
                "graph stats purge",
            )?;
            // Cut keys and applied keys both lead with `{collection}\x00`.
            let mut cuts = write_txn
                .open_table(EDGE_CUTS)
                .map_err(|e| redb_err("open edge_cuts", e))?;
            drain_range(
                &mut cuts,
                (db, t, prefix.as_str())..(db, t, prefix_end.as_str()),
                "edge cut purge",
            )?;
            let mut applied = write_txn
                .open_table(EDGE_APPLIED)
                .map_err(|e| redb_err("open edge_applied", e))?;
            drain_range(
                &mut applied,
                (db, t, prefix.as_str())..(db, t, prefix_end.as_str()),
                "edge applied purge",
            )?;
        }

        write_txn
            .commit()
            .map_err(|e| redb_err("commit collection purge", e))?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::temporal::{EdgeValuePayload, versioned_edge_key};
    use crate::types::VShardId;

    /// Two node keys whose key vShards differ.
    fn two_homes() -> (String, String) {
        let a = "n0".to_string();
        let home = VShardId::from_key(a.as_bytes());
        let b = (1..4096)
            .map(|i| format!("n{i}"))
            .find(|k| VShardId::from_key(k.as_bytes()) != home)
            .expect("a second home in 4096 keys");
        (a, b)
    }

    /// A group install purges exactly the edges with an endpoint home in the
    /// group, in every collection: an edge whose collection homes in the
    /// group but whose endpoints do not stays.
    #[test]
    fn purge_homed_takes_exactly_the_group_edges() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open");
        let (a, b) = two_homes();
        let value = EdgeValuePayload::new(0, i64::MAX, Vec::new())
            .encode()
            .expect("payload");
        let tid = TenantId::new(1);
        for (src, dst) in [(&a, &a), (&a, &b), (&b, &b)] {
            let key = versioned_edge_key("g", src, "L", dst, 1).expect("key");
            store.put_edge_raw(0, tid, &key, &value).expect("put");
        }

        let group: HashSet<u32> = [VShardId::from_key(a.as_bytes()).as_u32()].into();
        assert_eq!(store.purge_homed(&group).expect("purge"), 2);

        let left: Vec<String> = store
            .export_edges()
            .expect("export")
            .into_iter()
            .map(|(_, _, key, _)| key)
            .collect();
        let kept = versioned_edge_key("g", &b, "L", &b, 1).expect("key");
        assert_eq!(left, vec![kept]);
        assert_eq!(store.purge_homed(&HashSet::new()).expect("purge"), 0);
    }
}
