// SPDX-License-Identifier: BUSL-1.1

//! Tenant- and collection-scoped edge purge.
//!
//! Structural range deletes on both `EDGES` and `REVERSE_EDGES`. No
//! lexical-prefix scans at the tenant boundary — tenant is the first
//! tuple component. Collection purge uses the `"{collection}\x00"`
//! prefix on the composite string, exploiting the
//! `collection\x00src\x00label\x00dst` layout of the composite key.

use nodedb_types::TenantId;
use redb::KeyRange;

use super::stats::table::{GRAPH_STATS, collection_stat_prefix};
use super::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};

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

impl EdgeStore {
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
        }

        write_txn
            .commit()
            .map_err(|e| redb_err("commit collection purge", e))?;
        Ok(removed)
    }
}
