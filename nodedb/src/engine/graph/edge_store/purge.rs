// SPDX-License-Identifier: BUSL-1.1

//! Tenant- and collection-scoped edge purge.
//!
//! Structural range deletes on both `EDGES` and `REVERSE_EDGES`. No
//! lexical-prefix scans at the tenant boundary — tenant is the first
//! tuple component. Collection purge uses the `"{collection}\x00"`
//! prefix on the composite string, exploiting the
//! `collection\x00src\x00label\x00dst` layout of the composite key.

use nodedb_types::TenantId;
use redb::ReadableTable;

use super::stats::table::{GRAPH_STATS, collection_stat_prefix};
use super::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};

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
            let keys: Vec<String> = edges
                .range((db, t, "")..(db, t + 1, ""))
                .map_err(|e| redb_err("edge range", e))?
                .map(|r| r.map(|(k, _)| k.value().2.to_string()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| redb_err("range iter", e))?;
            removed += keys.len();
            for key in &keys {
                edges
                    .remove((db, t, key.as_str()))
                    .map_err(|e| redb_err("edge remove", e))?;
            }
        }

        {
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            let keys: Vec<String> = rev_t
                .range((db, t, "")..(db, t + 1, ""))
                .map_err(|e| redb_err("rev range", e))?
                .map(|r| r.map(|(k, _)| k.value().2.to_string()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| redb_err("range iter", e))?;
            removed += keys.len();
            for key in &keys {
                rev_t
                    .remove((db, t, key.as_str()))
                    .map_err(|e| redb_err("reverse remove", e))?;
            }
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
            let keys: Vec<String> = stats_t
                .range((db, t, "")..(db, t + 1, ""))
                .map_err(|e| redb_err("stats range", e))?
                .map(|r| r.map(|(k, _)| k.value().2.to_string()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| redb_err("range iter", e))?;
            for key in &keys {
                stats_t
                    .remove((db, t, key.as_str()))
                    .map_err(|e| redb_err("stats remove", e))?;
            }
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
            let keys: Vec<String> = edges
                .range((db, t, prefix.as_str())..(db, t, prefix_end.as_str()))
                .map_err(|e| redb_err("edge range", e))?
                .map(|r| r.map(|(k, _)| k.value().2.to_string()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| redb_err("range iter", e))?;
            removed += keys.len();
            for key in &keys {
                edges
                    .remove((db, t, key.as_str()))
                    .map_err(|e| redb_err("edge remove", e))?;
            }
        }

        {
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            let keys: Vec<String> = rev_t
                .range((db, t, prefix.as_str())..(db, t, prefix_end.as_str()))
                .map_err(|e| redb_err("rev range", e))?
                .map(|r| r.map(|(k, _)| k.value().2.to_string()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| redb_err("range iter", e))?;
            for key in &keys {
                rev_t
                    .remove((db, t, key.as_str()))
                    .map_err(|e| redb_err("reverse remove", e))?;
            }
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
            let keys: Vec<String> = stats_t
                .range((db, t, stats_prefix.as_str())..(db, t, stats_prefix_end.as_str()))
                .map_err(|e| redb_err("stats range", e))?
                .map(|r| r.map(|(k, _)| k.value().2.to_string()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| redb_err("range iter", e))?;
            for key in &keys {
                stats_t
                    .remove((db, t, key.as_str()))
                    .map_err(|e| redb_err("stats remove", e))?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| redb_err("commit collection purge", e))?;
        Ok(removed)
    }
}
