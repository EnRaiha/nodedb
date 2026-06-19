// SPDX-License-Identifier: BUSL-1.1

//! Node-level cascade: soft-delete every edge incident on a node.

use std::collections::HashMap;

use super::store::{BaseKey, EDGES, EdgeStore, redb_err};
use super::temporal::{EdgeRef, is_sentinel, parse_versioned_edge_key};
use nodedb_types::{DatabaseId, TenantId};

impl EdgeStore {
    /// Soft-delete every edge incident on `node` (as either src or dst) in
    /// the caller's tenant, across all collections. Emits a tombstone
    /// version at `system_from` for each distinct base edge that has a
    /// live (non-sentinel) latest version.
    pub fn delete_edges_for_node(
        &self,
        db: u64,
        tid: TenantId,
        node: &str,
        system_from: i64,
    ) -> crate::Result<()> {
        // Snapshot all live bases touching `node`. Done in a read txn first
        // so the write txn can call soft_delete_edge without nested locks.
        let bases = self.live_bases_touching_node(db, tid, node)?;
        for (collection, src, label, dst) in &bases {
            self.soft_delete_edge(
                EdgeRef::new(DatabaseId::new(db), tid, collection, src, label, dst),
                system_from,
            )?;
        }
        Ok(())
    }

    /// Enumerate `(collection, src, label, dst)` tuples for every base edge
    /// in this `(database, tenant)` whose latest version touches `node` as src
    /// or dst and is not a sentinel.
    fn live_bases_touching_node(
        &self,
        db: u64,
        tid: TenantId,
        node: &str,
    ) -> crate::Result<Vec<BaseKey>> {
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        let mut latest: HashMap<BaseKey, (i64, bool)> = HashMap::new();
        // DB-scoped range: a node-delete in database A must NOT cascade into
        // the same tenant's edges in database B.
        let range = table
            .range((db, t, "")..(db, t + 1, ""))
            .map_err(|e| redb_err("iter", e))?;
        for entry in range {
            let (k, v) = entry.map_err(|e| redb_err("iter entry", e))?;
            let composite = k.value().2;
            let Some((coll, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if src != node && dst != node {
                continue;
            }
            let base = (
                coll.to_string(),
                src.to_string(),
                label.to_string(),
                dst.to_string(),
            );
            let is_sent = is_sentinel(v.value());
            latest
                .entry(base)
                .and_modify(|(cur, cur_sent)| {
                    if sys > *cur {
                        *cur = sys;
                        *cur_sent = is_sent;
                    }
                })
                .or_insert((sys, is_sent));
        }
        Ok(latest
            .into_iter()
            .filter_map(|(base, (_sys, is_sent))| if is_sent { None } else { Some(base) })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::OrdinalClock;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const D: u64 = 0;
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn put(store: &EdgeStore, clock: &OrdinalClock, src: &str, label: &str, dst: &str, p: &[u8]) {
        let ord = clock.next_ordinal();
        store
            .put_edge_versioned(
                EdgeRef::new(DB, T, COLL, src, label, dst),
                p,
                ord,
                ord,
                i64::MAX,
            )
            .unwrap();
    }

    #[test]
    fn delete_edges_for_node_soft_deletes_all_incident() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"1");
        put(&store, &clock, "alice", "KNOWS", "carol", b"2");
        put(&store, &clock, "dave", "KNOWS", "alice", b"3");
        put(&store, &clock, "eve", "KNOWS", "frank", b"4");

        let purge_ord = clock.next_ordinal();
        store
            .delete_edges_for_node(D, T, "alice", purge_ord)
            .unwrap();

        assert!(
            store
                .get_edge(D, T, COLL, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_edge(D, T, COLL, "alice", "KNOWS", "carol")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_edge(D, T, COLL, "dave", "KNOWS", "alice")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.get_edge(D, T, COLL, "eve", "KNOWS", "frank").unwrap(),
            Some(b"4".to_vec())
        );
    }

    #[test]
    fn delete_edges_for_node_skips_already_tombstoned() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"1");
        store
            .soft_delete_edge(
                EdgeRef::new(DB, T, COLL, "alice", "KNOWS", "bob"),
                clock.next_ordinal(),
            )
            .unwrap();

        // Should be a no-op — no live bases to cascade through.
        store
            .delete_edges_for_node(D, T, "alice", clock.next_ordinal())
            .unwrap();
        assert!(
            store
                .get_edge(D, T, COLL, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
    }
}
