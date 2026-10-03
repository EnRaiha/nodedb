// SPDX-License-Identifier: BUSL-1.1

//! The stamp each staged edge version of a resolve takes.
//!
//! A version is applied at the transaction's frozen ordinal, always: a
//! TRUNCATE's cut compares against it. It takes the same ordinal as its
//! `system_from`, unless an earlier write stored a version of the edge at or
//! above it (a restored version from a clock that ran ahead). The version
//! then takes a `system_from` one above that one, so it sorts after every
//! earlier write, and its applied ordinal stays the transaction's.
//!
//! The raise reads only the versions other writes stored. One core can hold
//! both homes of a cross-shard edge and store this transaction's version on
//! the other home before this home resolves. Counting that version
//! raises this home's version one above it, a second version no replica with
//! the homes on two cores holds. Every earlier writer of the edge flushed on
//! both homes before this transaction resolves on either, since each holds
//! the edge's lock on both. So every home and replica raises alike.

use crate::engine::graph::edge_store::temporal::keys::EdgeRef;
use crate::engine::graph::edge_store::{EdgeStore, VersionStamp};
use crate::types::{DatabaseId, TenantId};

/// The edge store a resolve reads, and the scope its edges live in.
pub(super) struct StoredEdges<'a> {
    pub store: &'a EdgeStore,
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
}

/// The stamps of one resolve's edge versions.
pub(super) struct EdgeOrdinals<'a> {
    txn: i64,
    stored: Option<StoredEdges<'a>>,
}

impl<'a> EdgeOrdinals<'a> {
    /// Every version is stamped and applied at `txn`.
    #[cfg(test)]
    pub(super) const fn fixed(txn: i64) -> Self {
        Self { txn, stored: None }
    }

    /// Every version is applied at `txn`, and stamped at `txn` raised above
    /// the newest version another write stored of its edge in `stored`.
    pub(super) const fn above_stored(txn: i64, stored: StoredEdges<'a>) -> Self {
        Self {
            txn,
            stored: Some(stored),
        }
    }

    /// The stamp of the version of `(collection, src, label, dst)`.
    pub(super) fn for_edge(
        &self,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<VersionStamp> {
        let Some(stored) = &self.stored else {
            return Ok(VersionStamp::at(self.txn));
        };
        let newest = stored.store.latest_version_ordinal_of_other_writes(
            EdgeRef::new(
                stored.database_id,
                stored.tenant_id,
                collection,
                src,
                label,
                dst,
            ),
            self.txn,
        )?;
        let system_from = match newest {
            Some(newest) if newest >= self.txn => newest.saturating_add(1),
            _ => self.txn,
        };
        Ok(VersionStamp {
            system_from,
            applied: self.txn,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: DatabaseId = DatabaseId::DEFAULT;
    const TID: TenantId = TenantId::new(1);
    const TXN: i64 = 5_000;

    fn store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("edge store");
        (store, dir)
    }

    fn e(dst: &str) -> EdgeRef<'_> {
        EdgeRef::new(DB, TID, "g", "a", "L", dst)
    }

    fn put(store: &EdgeStore, dst: &str, stamp: VersionStamp) {
        store
            .put_edge_version_recorded(e(dst), b"v", stamp, 0, i64::MAX, true)
            .expect("put version");
    }

    fn ordinals(store: &EdgeStore) -> EdgeOrdinals<'_> {
        EdgeOrdinals::above_stored(
            TXN,
            StoredEdges {
                store,
                database_id: DB,
                tenant_id: TID,
            },
        )
    }

    #[test]
    fn a_version_is_applied_at_the_txn_ordinal_above_older_versions() {
        let (store, _dir) = store();
        put(&store, "b", VersionStamp::at(100));
        // A restored version from a clock that ran ahead of the sequence.
        put(
            &store,
            "c",
            VersionStamp::applied_at(9_000, Some(900)).expect("stamp"),
        );
        let ordinals = ordinals(&store);
        assert_eq!(
            ordinals.for_edge("g", "a", "L", "b").expect("older"),
            VersionStamp::at(TXN)
        );
        assert_eq!(
            ordinals.for_edge("g", "a", "L", "c").expect("ahead"),
            VersionStamp {
                system_from: 9_001,
                applied: TXN,
            },
            "raised to sort after the earlier write, applied at the txn ordinal"
        );
        assert_eq!(
            ordinals.for_edge("g", "x", "L", "y").expect("absent"),
            VersionStamp::at(TXN)
        );
        assert_eq!(
            EdgeOrdinals::fixed(7)
                .for_edge("g", "a", "L", "c")
                .expect("fixed"),
            VersionStamp::at(7)
        );
    }

    /// One core holds both homes of an edge, and the transaction's version
    /// on the other home is stored before this home resolves. The stamp is
    /// the one a replica with the homes on two cores computes, and the
    /// transaction's TRUNCATE neighbour hides the edge on both.
    #[test]
    fn the_other_homes_version_of_the_same_txn_raises_nothing() {
        let (shared, _a) = store();
        let (apart, _b) = store();
        for layout in [&shared, &apart] {
            put(layout, "b", VersionStamp::at(100));
        }
        // The other home of the same transaction flushed first on the
        // shared core.
        put(&shared, "b", VersionStamp::at(TXN));

        let on_shared = ordinals(&shared)
            .for_edge("g", "a", "L", "b")
            .expect("shared");
        let on_apart = ordinals(&apart)
            .for_edge("g", "a", "L", "b")
            .expect("apart");
        assert_eq!(on_shared, VersionStamp::at(TXN));
        assert_eq!(on_shared, on_apart, "every layout stamps one version key");

        // Both layouts store the version, then a TRUNCATE sequenced right
        // after the transaction cuts at the next ordinal.
        for layout in [&shared, &apart] {
            put(layout, "b", on_shared);
            layout.install_edge_cut(DB, TID, "g", TXN + 1).expect("cut");
            assert_eq!(
                layout
                    .ceiling_resolve_edge(e("b"), i64::MAX, None)
                    .expect("read"),
                None,
                "the TRUNCATE after the write hides it"
            );
        }
        assert_eq!(
            shared.scan_edges_for_tenant(0, TID).expect("scan"),
            apart.scan_edges_for_tenant(0, TID).expect("scan"),
            "both layouts store the same versions"
        );
    }

    /// A version raised above a stored one sits above its applied ordinal.
    /// A TRUNCATE sequenced after its transaction still hides it.
    #[test]
    fn a_raised_version_is_hidden_by_a_later_cut() {
        let (store, _dir) = store();
        put(
            &store,
            "c",
            VersionStamp::applied_at(9_000, Some(900)).expect("stamp"),
        );
        let stamp = ordinals(&store)
            .for_edge("g", "a", "L", "c")
            .expect("stamp");
        put(&store, "c", stamp);
        assert_eq!(
            store
                .ceiling_resolve_edge(e("c"), i64::MAX, None)
                .expect("read"),
            Some(b"v".to_vec())
        );
        store.install_edge_cut(DB, TID, "g", TXN + 1).expect("cut");
        assert_eq!(
            store
                .ceiling_resolve_edge(e("c"), i64::MAX, None)
                .expect("read"),
            None,
            "the cut above the applied ordinal hides it, though its system time is above the cut"
        );
    }
}
