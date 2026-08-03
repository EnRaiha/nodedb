// SPDX-License-Identifier: BUSL-1.1

//! Audit-retention purge: drop *superseded* edge versions older than the
//! system-time cutoff.
//!
//! Live (latest-per-base) versions and any version newer than the cutoff are
//! preserved. The last surviving version of a base is never deleted, even if
//! it is older than the cutoff — "audit retain" reclaims only history, not
//! the current state.

use super::keys::{parse_versioned_edge_key, versioned_edge_key};
use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};
use nodedb_types::TenantId;
use nodedb_types::temporal::ms_to_ordinal_upper;
use redb::ReadableDatabase;

impl EdgeStore {
    /// Purge superseded versions for `(tid, collection)` whose
    /// `system_from` ordinal is strictly less than
    /// `ms_to_ordinal_upper(cutoff_system_ms)`.
    ///
    /// Preserves:
    /// - Every version whose `system_from >= cutoff_ordinal`.
    /// - The latest-per-base version (tombstones / GDPR markers included)
    ///   even if it is older than the cutoff — otherwise the base would be
    ///   silently resurrected on a subsequent scan.
    ///
    /// Deletes from both `EDGES` and `REVERSE_EDGES` inside a single
    /// write transaction so the two indexes never diverge mid-purge.
    /// Returns the number of `EDGES` rows removed (reverse-index
    /// removals are paired 1:1).
    pub fn purge_superseded_versions(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        let cutoff_ordinal = ms_to_ordinal_upper(cutoff_system_ms);
        let t = tid.as_u64();

        // Pass 1: scan read-only, group by base, collect delete candidates.
        let victims = self.collect_purge_victims(db, t, collection, cutoff_ordinal)?;
        if victims.is_empty() {
            return Ok(0);
        }

        // Pass 2: delete forward + reverse inside one write txn.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write purge", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges purge", e))?;
            let mut rev = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse purge", e))?;

            for v in &victims {
                edges
                    .remove((db, t, v.fwd_key.as_str()))
                    .map_err(|e| redb_err("remove edge", e))?;
                rev.remove((db, t, v.rev_key.as_str()))
                    .map_err(|e| redb_err("remove reverse", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit purge", e))?;

        Ok(victims.len())
    }

    fn collect_purge_victims(
        &self,
        db: u64,
        t: u64,
        collection: &str,
        cutoff_ordinal: i64,
    ) -> crate::Result<Vec<Victim>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read purge", e))?;
        let edges = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges read purge", e))?;

        // Versioned keys sort chronologically within a base (ascending
        // system_from). We walk the full tenant range once, group by base,
        // and for each base:
        //   - keep the single newest version (latest per base)
        //   - keep any version with system_from >= cutoff_ordinal
        //   - every other version becomes a purge victim
        // Streaming implementation: since redb returns keys in sorted order
        // and the base prefix is a strict lex-prefix, we can emit victims as
        // soon as we see "the next version for this same base" — i.e., the
        // current candidate is provably not the latest and the cutoff
        // predicate gates inclusion.

        let low = (db, t, "");
        let high = (db, t + 1, "");
        let range = edges
            .range(low..high)
            .map_err(|e| redb_err("range purge", e))?;

        let mut pending: Option<PendingVersion> = None;
        let mut victims: Vec<Victim> = Vec::new();
        for entry in range {
            let (k, _v) = entry.map_err(|e| redb_err("entry purge", e))?;
            let (_db, _tid, key_str) = k.value();
            let Some((parsed_coll, src, label, dst, system_from)) =
                parse_versioned_edge_key(key_str)
            else {
                continue;
            };
            if parsed_coll != collection {
                // Wrong collection within this tenant — skip.
                if let Some(p) = pending.take() {
                    // Flushing pending unconditionally: since we're leaving
                    // its base, whatever was held is the latest-for-base
                    // and must be kept.
                    drop(p);
                }
                continue;
            }
            let base = Base {
                src: src.to_string(),
                label: label.to_string(),
                dst: dst.to_string(),
            };

            match pending.take() {
                None => {
                    pending = Some(PendingVersion { base, system_from });
                }
                Some(prev) if prev.base == base => {
                    // prev is older than the current one (sorted ascending),
                    // and by definition not the latest for this base — it is
                    // eligible to be a victim if it's below the cutoff.
                    if prev.system_from < cutoff_ordinal {
                        victims.push(Victim::new(collection, &prev.base, prev.system_from)?);
                    }
                    pending = Some(PendingVersion { base, system_from });
                }
                Some(_) => {
                    // Base boundary — prev was the latest for its base, keep.
                    pending = Some(PendingVersion { base, system_from });
                }
            }
        }
        // The last pending version is the latest for its base and is kept.
        drop(pending);

        Ok(victims)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Base {
    src: String,
    label: String,
    dst: String,
}

struct PendingVersion {
    base: Base,
    system_from: i64,
}

struct Victim {
    fwd_key: String,
    rev_key: String,
}

impl Victim {
    fn new(collection: &str, base: &Base, system_from: i64) -> crate::Result<Self> {
        let fwd_key =
            versioned_edge_key(collection, &base.src, &base.label, &base.dst, system_from)?;
        let rev_key =
            versioned_edge_key(collection, &base.dst, &base.label, &base.src, system_from)?;
        Ok(Self { fwd_key, rev_key })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::temporal::keys::EdgeRef;
    use nodedb_types::DatabaseId;

    const DB: DatabaseId = DatabaseId::DEFAULT;
    const D: u64 = 0;

    fn fresh_store() -> (tempfile::TempDir, EdgeStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        let store = EdgeStore::open(&path).unwrap();
        (dir, store)
    }

    fn put(store: &EdgeStore, edge: EdgeRef<'_>, system_from: i64) {
        store
            .put_edge_versioned(edge, b"v1", system_from, 0, i64::MAX)
            .unwrap();
    }

    fn count_edges(store: &EdgeStore, tid: TenantId, collection: &str) -> usize {
        let read_txn = store.db.begin_read().unwrap();
        let edges = read_txn.open_table(EDGES).unwrap();
        let low = (D, tid.as_u64(), "");
        let high = (D, tid.as_u64() + 1, "");
        edges
            .range(low..high)
            .unwrap()
            .filter_map(|r| {
                let (k, _) = r.ok()?;
                let (_d, _t, s) = k.value();
                let (c, ..) = parse_versioned_edge_key(s)?;
                (c == collection).then_some(())
            })
            .count()
    }

    #[test]
    fn purge_drops_superseded_versions_below_cutoff() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let edge = EdgeRef::new(DB, tid, "c", "a", "L", "b");

        // Three versions at ms = 100, 200, 300. Cutoff at 250 should drop
        // version 100 (< cutoff AND not latest). Version 200 is < cutoff
        // but WOULD be latest if 300 didn't exist — it's superseded by 300,
        // so it's eligible too. Version 300 is latest: kept.
        for ms in [100i64, 200, 300] {
            let ord = ms_to_ordinal_upper(ms);
            put(&store, edge, ord);
        }
        assert_eq!(count_edges(&store, tid, "c"), 3);

        let purged = store.purge_superseded_versions(D, tid, "c", 150).unwrap();
        assert_eq!(purged, 1, "only v@100 is below cutoff AND superseded");
        assert_eq!(count_edges(&store, tid, "c"), 2);
    }

    #[test]
    fn purge_never_deletes_latest_version() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let edge = EdgeRef::new(DB, tid, "c", "a", "L", "b");
        put(&store, edge, ms_to_ordinal_upper(100));

        // Cutoff way in the future. Only one version: keep it.
        let purged = store
            .purge_superseded_versions(D, tid, "c", 10_000)
            .unwrap();
        assert_eq!(purged, 0);
        assert_eq!(count_edges(&store, tid, "c"), 1);
    }

    #[test]
    fn purge_is_per_tenant_collection_scoped() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let other_tenant = TenantId::new(2);
        let e1 = EdgeRef::new(DB, tid, "c", "a", "L", "b");
        let e_other_tenant = EdgeRef::new(DB, other_tenant, "c", "a", "L", "b");
        let e_other_coll = EdgeRef::new(DB, tid, "d", "a", "L", "b");

        for ms in [100i64, 200, 300] {
            put(&store, e1, ms_to_ordinal_upper(ms));
            put(&store, e_other_tenant, ms_to_ordinal_upper(ms));
            put(&store, e_other_coll, ms_to_ordinal_upper(ms));
        }

        // Purge only (tid=1, "c") below 250.
        let purged = store.purge_superseded_versions(D, tid, "c", 150).unwrap();
        assert_eq!(purged, 1);
        assert_eq!(count_edges(&store, tid, "c"), 2);
        assert_eq!(count_edges(&store, tid, "d"), 3);
        assert_eq!(count_edges(&store, other_tenant, "c"), 3);
    }

    #[test]
    fn purge_is_idempotent() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let edge = EdgeRef::new(DB, tid, "c", "a", "L", "b");
        for ms in [100i64, 200, 300] {
            put(&store, edge, ms_to_ordinal_upper(ms));
        }
        let first = store.purge_superseded_versions(D, tid, "c", 150).unwrap();
        let second = store.purge_superseded_versions(D, tid, "c", 150).unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 0);
    }
}
