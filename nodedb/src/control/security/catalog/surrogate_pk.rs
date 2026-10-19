// SPDX-License-Identifier: BUSL-1.1

//! Surrogate ↔ PK catalog ops on one key, plus whole-collection scans and
//! deletes. Batched ops live in [`super::surrogate_pk_batch`].
//!
//! Every write of a binding writes its forward and reverse rows in one redb
//! write transaction, so the two directions never drift. Every entry point
//! takes a [`CollectionKey`] and a tenant: their scope id names the rows (see
//! [`super::surrogate_scope`]).

use nodedb_types::{CollectionKey, Surrogate, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::surrogate_scope::{
    drain_scope, fwd_key, fwd_range, pk_of, read_scope, rev_key, take_scope, write_scope,
};
use super::types::{SURROGATE_PK, SURROGATE_PK_REV, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Insert or overwrite a surrogate ↔ PK binding, as
    /// [`Self::overwrite_surrogates`] does for one row.
    pub fn put_surrogate(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
        surrogate: Surrogate,
    ) -> crate::Result<()> {
        self.overwrite_surrogates(key, tenant_id, &[(pk_bytes, surrogate)])
    }

    /// The surrogate bound to `pk_bytes` in `key` under `tenant_id`, or `None`
    /// when no binding exists.
    pub fn get_surrogate_for_pk(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Surrogate>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk read txn", e))?;
        let Some(scope) = read_scope(&txn, key, tenant_id)? else {
            return Ok(None);
        };
        let table = txn
            .open_table(SURROGATE_PK)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        let found = table
            .get(fwd_key(scope, pk_bytes).as_slice())
            .map_err(|e| catalog_err("get surrogate_pk", e))?
            .map(|v| Surrogate::new(v.value()));
        Ok(found)
    }

    /// The PK bound to `surrogate` in `key` under `tenant_id`, or `None` when
    /// no binding exists.
    pub fn get_pk_for_surrogate(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        surrogate: Surrogate,
    ) -> crate::Result<Option<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk_rev read txn", e))?;
        let Some(scope) = read_scope(&txn, key, tenant_id)? else {
            return Ok(None);
        };
        let table = txn
            .open_table(SURROGATE_PK_REV)
            .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
        let found = table
            .get(rev_key(scope, surrogate.as_u32()))
            .map_err(|e| catalog_err("get surrogate_pk_rev", e))?
            .map(|v| v.value().to_vec());
        Ok(found)
    }

    /// The PK bound to each of `surrogates` in `key` under `tenant_id`, in
    /// `surrogates` order, read in one read transaction. `None` for a
    /// surrogate with no binding.
    pub fn get_pks_for_surrogates(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        surrogates: &[Surrogate],
    ) -> crate::Result<Vec<Option<Vec<u8>>>> {
        if surrogates.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk_rev batch read txn", e))?;
        let Some(scope) = read_scope(&txn, key, tenant_id)? else {
            return Ok(vec![None; surrogates.len()]);
        };
        let table = txn
            .open_table(SURROGATE_PK_REV)
            .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
        surrogates
            .iter()
            .map(|surrogate| {
                Ok(table
                    .get(rev_key(scope, surrogate.as_u32()))
                    .map_err(|e| catalog_err("get surrogate_pk_rev", e))?
                    .map(|v| v.value().to_vec()))
            })
            .collect()
    }

    /// Remove a surrogate ↔ PK binding atomically. Idempotent.
    pub fn delete_surrogate(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk delete txn", e))?;
        if let Some(scope) = write_scope(&txn, key, tenant_id)? {
            let mut fwd = txn
                .open_table(SURROGATE_PK)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            let removed = fwd
                .remove(fwd_key(scope, pk_bytes).as_slice())
                .map_err(|e| catalog_err("remove surrogate_pk", e))?
                .map(|v| v.value());
            if let Some(surrogate) = removed {
                let mut rev = txn
                    .open_table(SURROGATE_PK_REV)
                    .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
                rev.remove(rev_key(scope, surrogate))
                    .map_err(|e| catalog_err("remove surrogate_pk_rev", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk delete commit", e))
    }

    /// Every binding of `key` under `tenant_id`, as `(pk_bytes, surrogate)`
    /// pairs in pk byte order.
    pub fn scan_surrogates_for_collection(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<(Vec<u8>, Surrogate)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk scan txn", e))?;
        let Some(scope) = read_scope(&txn, key, tenant_id)? else {
            return Ok(Vec::new());
        };
        let table = txn
            .open_table(SURROGATE_PK)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        let (lo, hi) = fwd_range(scope);
        let mut out = Vec::new();
        for row in table
            .range(lo.as_slice()..hi.as_slice())
            .map_err(|e| catalog_err("range surrogate_pk", e))?
        {
            let (k, v) = row.map_err(|e| catalog_err("iter surrogate_pk row", e))?;
            out.push((pk_of(k.value())?.to_vec(), Surrogate::new(v.value())));
        }
        Ok(out)
    }

    /// The highest surrogate any live binding refers to, or `0` when there are
    /// none.
    ///
    /// This is the allocator's boot floor of last resort. The `surrogate_hwm`
    /// singleton is flushed lazily (batched by op count and elapsed time), so a
    /// crash can leave it stale; the WAL's `SurrogateAlloc` / `SurrogateBindBatch`
    /// records normally cover the gap, but nothing contributes a "surrogate
    /// durable through" floor to WAL truncation, so a checkpoint can truncate
    /// past those records while the singleton still lags. Seeding from the
    /// singleton alone would then re-issue surrogates already bound to live
    /// rows — cross-engine identity corruption, since every engine keys its
    /// indexes on the surrogate.
    ///
    /// Derived from the bindings themselves, the floor cannot go backwards: a
    /// surrogate is written here before it is ever observable, and rows are
    /// removed only when their identity genuinely dies with them.
    ///
    /// Cost is one full scan of the forward table, paid once at boot. The
    /// reverse table orders by scope first, so its last key is the largest
    /// surrogate of the last scope, not the largest overall.
    pub fn max_bound_surrogate(&self) -> crate::Result<Surrogate> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk max txn", e))?;
        let table = txn
            .open_table(SURROGATE_PK)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        let mut max = 0u32;
        let iter = table
            .iter()
            .map_err(|e| catalog_err("iter surrogate_pk", e))?;
        for row in iter {
            let (_k, v) = row.map_err(|e| catalog_err("iter surrogate_pk row", e))?;
            max = max.max(v.value());
        }
        Ok(Surrogate::new(max))
    }

    /// Delete every surrogate binding of `key` under `tenant_id` from both
    /// tables, and its scope. Idempotent.
    pub fn delete_all_surrogates_for_collection(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk bulk-delete txn", e))?;
        if let Some(scope) = take_scope(&txn, key, tenant_id)? {
            drain_scope(&txn, scope)?;
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk bulk-delete commit", e))
    }
}

#[cfg(test)]
mod max_bound_surrogate_tests {
    use super::*;
    use crate::control::security::catalog::SystemCatalog;
    use nodedb_types::DatabaseId;

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).expect("open catalog");
        (dir, cat)
    }

    #[test]
    fn fresh_catalog_has_no_floor() {
        let (_dir, cat) = open();
        assert_eq!(cat.max_bound_surrogate().unwrap(), Surrogate::ZERO);
    }

    /// The floor must be the maximum across every database, tenant, and
    /// collection — not the last one in key order, which is what a naive
    /// reverse-table probe would return.
    #[test]
    fn floor_is_the_global_maximum_across_every_scope() {
        let (_dir, cat) = open();
        let put = |db: u64, coll: &str, tenant: u64, pk: &[u8], s: u32| {
            cat.put_surrogate(
                CollectionKey::from_bare(DatabaseId::new(db), coll),
                TenantId::new(tenant),
                pk,
                Surrogate::new(s),
            )
            .unwrap();
        };
        put(0, "zzz_last_in_key_order", 1, b"a", 3);
        put(0, "aaa_first_in_key_order", 1, b"b", 9_000);
        put(7, "other_db", 2, b"c", 41);
        assert_eq!(cat.max_bound_surrogate().unwrap(), Surrogate::new(9_000));
    }

    /// The whole point of the floor: a stale `surrogate_hwm` singleton must
    /// never let the allocator start below a surrogate a live row already
    /// holds.
    #[test]
    fn floor_outranks_a_stale_hwm_singleton() {
        let (_dir, cat) = open();
        cat.put_surrogate(
            CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
            TenantId::new(1),
            b"alice",
            Surrogate::new(500),
        )
        .unwrap();
        // The singleton lags because its flush is batched.
        cat.put_surrogate_hwm(100).unwrap();

        let seed = cat
            .get_surrogate_hwm()
            .unwrap()
            .max(cat.max_bound_surrogate().unwrap().as_u32());
        assert_eq!(
            seed, 500,
            "seeding from the singleton alone would re-issue 101..=500, \
             every one of which is already bound to a live row"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::DatabaseId;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    fn coll(name: &str) -> CollectionKey<'_> {
        CollectionKey::from_bare(DatabaseId::DEFAULT, name)
    }

    const T0: TenantId = TenantId::new(0);

    #[test]
    fn put_then_get_roundtrip() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(coll("users"), T0, b"alice", Surrogate::new(7))
            .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(coll("users"), T0, b"alice")
                .unwrap(),
            Some(Surrogate::new(7))
        );
        assert_eq!(
            cat.get_pk_for_surrogate(coll("users"), T0, Surrogate::new(7))
                .unwrap(),
            Some(b"alice".to_vec())
        );
    }

    /// One batch read answers every surrogate in order, `None` for an
    /// unbound one, and an unknown collection answers `None` throughout.
    #[test]
    fn batch_reverse_read_answers_in_order() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(coll("users"), T0, b"alice", Surrogate::new(7))
            .unwrap();
        cat.put_surrogate(coll("users"), T0, b"bob", Surrogate::new(9))
            .unwrap();
        let wanted = [Surrogate::new(9), Surrogate::new(8), Surrogate::new(7)];
        assert_eq!(
            cat.get_pks_for_surrogates(coll("users"), T0, &wanted)
                .unwrap(),
            vec![Some(b"bob".to_vec()), None, Some(b"alice".to_vec())]
        );
        assert_eq!(
            cat.get_pks_for_surrogates(coll("nobody"), T0, &wanted)
                .unwrap(),
            vec![None, None, None]
        );
        assert!(
            cat.get_pks_for_surrogates(coll("users"), T0, &[])
                .unwrap()
                .is_empty()
        );
    }

    /// An overwrite to a new surrogate leaves no reverse row under the old
    /// one, so the two directions stay inverse.
    #[test]
    fn an_overwrite_moves_the_reverse_row() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(coll("users"), T0, b"alice", Surrogate::new(7))
            .unwrap();
        cat.put_surrogate(coll("users"), T0, b"alice", Surrogate::new(8))
            .unwrap();
        assert_eq!(
            cat.get_pk_for_surrogate(coll("users"), T0, Surrogate::new(7))
                .unwrap(),
            None
        );
        assert_eq!(
            cat.get_pk_for_surrogate(coll("users"), T0, Surrogate::new(8))
                .unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn distinct_tenants_do_not_collide_on_same_pk() {
        let (_dir, cat) = open_catalog();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        cat.put_surrogate(coll("users"), t1, b"alice", Surrogate::new(10))
            .unwrap();
        cat.put_surrogate(coll("users"), t2, b"alice", Surrogate::new(20))
            .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(coll("users"), t1, b"alice")
                .unwrap(),
            Some(Surrogate::new(10))
        );
        assert_eq!(
            cat.get_surrogate_for_pk(coll("users"), t2, b"alice")
                .unwrap(),
            Some(Surrogate::new(20))
        );
    }

    #[test]
    fn missing_returns_none() {
        let (_dir, cat) = open_catalog();
        assert_eq!(
            cat.get_surrogate_for_pk(coll("users"), T0, b"nobody")
                .unwrap(),
            None
        );
    }

    #[test]
    fn delete_is_idempotent_and_removes_both_directions() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(coll("users"), T0, b"alice", Surrogate::new(7))
            .unwrap();
        cat.delete_surrogate(coll("users"), T0, b"alice").unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(coll("users"), T0, b"alice")
                .unwrap(),
            None
        );
        assert_eq!(
            cat.get_pk_for_surrogate(coll("users"), T0, Surrogate::new(7))
                .unwrap(),
            None
        );
        cat.delete_surrogate(coll("users"), T0, b"alice").unwrap();
    }

    #[test]
    fn scan_returns_only_named_collection() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(coll("users"), T0, b"alice", Surrogate::new(1))
            .unwrap();
        cat.put_surrogate(coll("users"), T0, b"bob", Surrogate::new(2))
            .unwrap();
        cat.put_surrogate(coll("orders"), T0, b"alice", Surrogate::new(3))
            .unwrap();
        // A different tenant's same-named collection must not leak into the scan.
        cat.put_surrogate(coll("users"), TenantId::new(9), b"carol", Surrogate::new(4))
            .unwrap();
        let got = cat
            .scan_surrogates_for_collection(coll("users"), T0)
            .unwrap();
        assert_eq!(
            got,
            vec![
                (b"alice".to_vec(), Surrogate::new(1)),
                (b"bob".to_vec(), Surrogate::new(2)),
            ]
        );
    }

    #[test]
    fn delete_all_wipes_collection_and_leaves_others_intact() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(coll("users"), T0, b"alice", Surrogate::new(1))
            .unwrap();
        cat.put_surrogate(coll("orders"), T0, b"o1", Surrogate::new(2))
            .unwrap();
        cat.delete_all_surrogates_for_collection(coll("users"), T0)
            .unwrap();
        assert!(
            cat.scan_surrogates_for_collection(coll("users"), T0)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            cat.get_pk_for_surrogate(coll("users"), T0, Surrogate::new(1))
                .unwrap(),
            None
        );
        assert_eq!(
            cat.get_surrogate_for_pk(coll("orders"), T0, b"o1").unwrap(),
            Some(Surrogate::new(2))
        );
        // double-delete is a no-op
        cat.delete_all_surrogates_for_collection(coll("users"), T0)
            .unwrap();
        // A re-created collection of the same name starts empty and binds.
        cat.put_surrogate(coll("users"), T0, b"alice", Surrogate::new(5))
            .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(coll("users"), T0, b"alice")
                .unwrap(),
            Some(Surrogate::new(5))
        );
    }
}
