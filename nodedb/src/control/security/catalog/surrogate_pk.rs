// SPDX-License-Identifier: BUSL-1.1

//! Surrogate ↔ PK catalog ops for the `_system.surrogate_pk{,_rev}_v3` tables.
//!
//! Forward + reverse mapping between user-visible primary keys and the
//! global `Surrogate` allocator. Every method writes both tables atomically
//! in a single redb write transaction so the two directions can never drift.
//!
//! The compound key is `(database_id, tenant_id, collection, pk_bytes)`
//! (forward) and `(database_id, tenant_id, collection, surrogate)` (reverse),
//! scoping the PK map to its database + tenant boundary.
//!
//! ## Migration
//!
//! `migrate_surrogate_pk()` reads all rows from the legacy bare
//! `_system.surrogate_pk` / `_system.surrogate_pk_rev` tables and rewrites
//! them under the v2 tables with `DatabaseId::DEFAULT` prepended.
//! `migrate_surrogate_pk_v3()` reads all rows from the v2 tables and rewrites
//! them under the v3 tables with the default user tenant (`1`) inserted as the
//! second key component. Both are idempotent: each skips if its target is
//! already non-empty.

use nodedb_types::{DatabaseId, Surrogate, TenantId};
use redb::{ReadableTable, ReadableTableMetadata};

#[allow(unused_imports)] // SURROGATE_PK_REV_LEGACY is used only in #[cfg(test)] helpers
use super::types::{
    SURROGATE_PK_LEGACY, SURROGATE_PK_REV_LEGACY, SURROGATE_PK_REV_V2, SURROGATE_PK_REV_V3,
    SURROGATE_PK_V2, SURROGATE_PK_V3, SystemCatalog, catalog_err,
};

impl SystemCatalog {
    /// Insert or overwrite a surrogate ↔ PK binding. Writes both the
    /// forward and reverse rows in one txn.
    ///
    /// Idempotent: re-binding the same `(database_id, tenant_id, collection,
    /// pk_bytes)` to the same surrogate is a no-op-on-disk overwrite.
    pub fn put_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
        surrogate: Surrogate,
    ) -> crate::Result<()> {
        let db_id = database_id.as_u64();
        let tid = tenant_id.as_u64();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk write txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK_V3)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            fwd.insert((db_id, tid, collection, pk_bytes), surrogate.as_u32())
                .map_err(|e| catalog_err("insert surrogate_pk", e))?;
            let mut rev = txn
                .open_table(SURROGATE_PK_REV_V3)
                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
            rev.insert((db_id, tid, collection, surrogate.as_u32()), pk_bytes)
                .map_err(|e| catalog_err("insert surrogate_pk_rev", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk commit", e))
    }

    /// Look up the surrogate previously bound to `(database_id, tenant_id,
    /// collection, pk_bytes)`. Returns `None` if no binding exists.
    pub fn get_surrogate_for_pk(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Surrogate>> {
        let db_id = database_id.as_u64();
        let tid = tenant_id.as_u64();
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk read txn", e))?;
        let table = txn
            .open_table(SURROGATE_PK_V3)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        match table
            .get((db_id, tid, collection, pk_bytes))
            .map_err(|e| catalog_err("get surrogate_pk", e))?
        {
            Some(v) => Ok(Some(Surrogate::new(v.value()))),
            None => Ok(None),
        }
    }

    /// Look up the PK previously bound to `(database_id, tenant_id, collection,
    /// surrogate)`. Returns `None` if no binding exists.
    pub fn get_pk_for_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        surrogate: Surrogate,
    ) -> crate::Result<Option<Vec<u8>>> {
        let db_id = database_id.as_u64();
        let tid = tenant_id.as_u64();
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk_rev read txn", e))?;
        let table = txn
            .open_table(SURROGATE_PK_REV_V3)
            .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
        match table
            .get((db_id, tid, collection, surrogate.as_u32()))
            .map_err(|e| catalog_err("get surrogate_pk_rev", e))?
        {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Remove a surrogate ↔ PK binding atomically. Idempotent.
    pub fn delete_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<()> {
        let db_id = database_id.as_u64();
        let tid = tenant_id.as_u64();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk delete txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK_V3)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            let removed = fwd
                .remove((db_id, tid, collection, pk_bytes))
                .map_err(|e| catalog_err("remove surrogate_pk", e))?;
            if let Some(v) = removed {
                let surrogate = v.value();
                let mut rev = txn
                    .open_table(SURROGATE_PK_REV_V3)
                    .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
                rev.remove((db_id, tid, collection, surrogate))
                    .map_err(|e| catalog_err("remove surrogate_pk_rev", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk delete commit", e))
    }

    /// Scan every binding for a `(database_id, tenant_id, collection)` triple.
    /// Returns `Vec<(pk_bytes, surrogate)>` in redb's natural key order.
    pub fn scan_surrogates_for_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> crate::Result<Vec<(Vec<u8>, Surrogate)>> {
        let db_id = database_id.as_u64();
        let tid = tenant_id.as_u64();
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk scan txn", e))?;
        let table = txn
            .open_table(SURROGATE_PK_V3)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        let mut out = Vec::new();
        // Range from the start of the `(db_id, tid, collection)` key prefix
        // (the empty pk is the smallest pk for that prefix) and stop as soon
        // as the iterator crosses out of the prefix, so only this triple's
        // rows are materialised.
        let iter = table
            .range((db_id, tid, collection, [].as_slice())..)
            .map_err(|e| catalog_err("range surrogate_pk", e))?;
        for row in iter {
            let (k, v) = row.map_err(|e| catalog_err("iter surrogate_pk row", e))?;
            let (row_db_id, row_tid, coll, pk) = k.value();
            if row_db_id != db_id || row_tid != tid || coll != collection {
                break;
            }
            out.push((pk.to_vec(), Surrogate::new(v.value())));
        }
        Ok(out)
    }

    /// Bulk-delete every surrogate binding for a `(database_id, tenant_id,
    /// collection)` triple. Drains both forward and reverse tables. Idempotent.
    pub fn delete_all_surrogates_for_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> crate::Result<()> {
        let to_remove = self.scan_surrogates_for_collection(database_id, tenant_id, collection)?;
        if to_remove.is_empty() {
            return Ok(());
        }
        let db_id = database_id.as_u64();
        let tid = tenant_id.as_u64();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk bulk-delete txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK_V3)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            let mut rev = txn
                .open_table(SURROGATE_PK_REV_V3)
                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
            for (pk, surrogate) in &to_remove {
                fwd.remove((db_id, tid, collection, pk.as_slice()))
                    .map_err(|e| catalog_err("bulk remove surrogate_pk", e))?;
                rev.remove((db_id, tid, collection, surrogate.as_u32()))
                    .map_err(|e| catalog_err("bulk remove surrogate_pk_rev", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk bulk-delete commit", e))
    }

    /// Idempotent migration: reads all rows from the legacy
    /// `_system.surrogate_pk` / `_system.surrogate_pk_rev` tables (bare
    /// `(collection, pk_bytes)` / `(collection, surrogate)` keys) and
    /// rewrites them under the v2 tables with `DatabaseId::DEFAULT` prepended.
    ///
    /// Skips if the v2 forward table is already non-empty (already-migrated
    /// boot). Safe to call on fresh boot (legacy table absent → no-op).
    pub fn migrate_surrogate_pk(&self) -> crate::Result<()> {
        // Gather legacy rows.
        let legacy_fwd: Vec<(String, Vec<u8>, u32)> = {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("migrate_surrogate_pk read txn", e))?;
            match txn.open_table(SURROGATE_PK_LEGACY) {
                Ok(table) => {
                    let iter = table
                        .iter()
                        .map_err(|e| catalog_err("migrate_surrogate_pk iter", e))?;
                    let mut rows = Vec::new();
                    for row in iter {
                        let (k, v) = row.map_err(|e| catalog_err("migrate_surrogate_pk row", e))?;
                        let (coll, pk) = k.value();
                        rows.push((coll.to_string(), pk.to_vec(), v.value()));
                    }
                    rows
                }
                Err(_) => Vec::new(),
            }
        };

        if legacy_fwd.is_empty() {
            return Ok(());
        }

        // Skip if v2 already populated.
        let v2_empty = {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("migrate_surrogate_pk v2 check txn", e))?;
            match txn.open_table(SURROGATE_PK_V2) {
                Ok(table) => table
                    .is_empty()
                    .map_err(|e| catalog_err("migrate_surrogate_pk v2 is_empty", e))?,
                Err(_) => true,
            }
        };
        if !v2_empty {
            return Ok(());
        }

        let db_id = DatabaseId::DEFAULT.as_u64();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("migrate_surrogate_pk write txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK_V2)
                .map_err(|e| catalog_err("migrate_surrogate_pk open fwd v2", e))?;
            let mut rev = txn
                .open_table(SURROGATE_PK_REV_V2)
                .map_err(|e| catalog_err("migrate_surrogate_pk open rev v2", e))?;
            for (coll, pk, surrogate_u32) in &legacy_fwd {
                fwd.insert((db_id, coll.as_str(), pk.as_slice()), *surrogate_u32)
                    .map_err(|e| catalog_err("migrate_surrogate_pk insert fwd", e))?;
                rev.insert((db_id, coll.as_str(), *surrogate_u32), pk.as_slice())
                    .map_err(|e| catalog_err("migrate_surrogate_pk insert rev", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("migrate_surrogate_pk commit", e))
    }

    /// Idempotent migration: reads all rows from the v2 tables
    /// (`(database_id, collection, pk_bytes)` / `(database_id, collection,
    /// surrogate)` keys) and rewrites them under the v3 tables with
    /// `tenant_id = 0` inserted as the second key component.
    ///
    /// Skips if the v3 forward table is already non-empty (already-migrated
    /// boot) and is a no-op when the v2 table is empty (fresh boot).
    pub fn migrate_surrogate_pk_v3(&self) -> crate::Result<()> {
        // Gather v2 forward rows.
        let v2_fwd: Vec<(u64, String, Vec<u8>, u32)> = {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("migrate_surrogate_pk_v3 read txn", e))?;
            match txn.open_table(SURROGATE_PK_V2) {
                Ok(table) => {
                    let iter = table
                        .iter()
                        .map_err(|e| catalog_err("migrate_surrogate_pk_v3 iter", e))?;
                    let mut rows = Vec::new();
                    for row in iter {
                        let (k, v) =
                            row.map_err(|e| catalog_err("migrate_surrogate_pk_v3 row", e))?;
                        let (db_id, coll, pk) = k.value();
                        rows.push((db_id, coll.to_string(), pk.to_vec(), v.value()));
                    }
                    rows
                }
                Err(_) => Vec::new(),
            }
        };

        if v2_fwd.is_empty() {
            return Ok(());
        }

        // Skip if v3 already populated.
        let v3_empty = {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("migrate_surrogate_pk_v3 check txn", e))?;
            match txn.open_table(SURROGATE_PK_V3) {
                Ok(table) => table
                    .is_empty()
                    .map_err(|e| catalog_err("migrate_surrogate_pk_v3 is_empty", e))?,
                Err(_) => true,
            }
        };
        if !v3_empty {
            return Ok(());
        }

        // Existing v2 rows were written tenant-blind by the default user
        // identity, which resolves to tenant 1 (see `trust_identity`). Backfill
        // that tenant so post-upgrade reads on the default identity still hit.
        let tid = TenantId::new(1).as_u64();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("migrate_surrogate_pk_v3 write txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK_V3)
                .map_err(|e| catalog_err("migrate_surrogate_pk_v3 open fwd v3", e))?;
            let mut rev = txn
                .open_table(SURROGATE_PK_REV_V3)
                .map_err(|e| catalog_err("migrate_surrogate_pk_v3 open rev v3", e))?;
            for (db_id, coll, pk, surrogate_u32) in &v2_fwd {
                fwd.insert((*db_id, tid, coll.as_str(), pk.as_slice()), *surrogate_u32)
                    .map_err(|e| catalog_err("migrate_surrogate_pk_v3 insert fwd", e))?;
                rev.insert((*db_id, tid, coll.as_str(), *surrogate_u32), pk.as_slice())
                    .map_err(|e| catalog_err("migrate_surrogate_pk_v3 insert rev", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("migrate_surrogate_pk_v3 commit", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::{
        SURROGATE_PK_LEGACY, SURROGATE_PK_REV_LEGACY, SURROGATE_PK_REV_V2, SURROGATE_PK_V2,
    };

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    const T0: TenantId = TenantId::new(0);

    #[test]
    fn put_then_get_roundtrip() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            T0,
            "users",
            b"alice",
            Surrogate::new(7),
        )
        .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, T0, "users", b"alice")
                .unwrap(),
            Some(Surrogate::new(7))
        );
        assert_eq!(
            cat.get_pk_for_surrogate(DatabaseId::DEFAULT, T0, "users", Surrogate::new(7))
                .unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn distinct_tenants_do_not_collide_on_same_pk() {
        let (_dir, cat) = open_catalog();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            t1,
            "users",
            b"alice",
            Surrogate::new(10),
        )
        .unwrap();
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            t2,
            "users",
            b"alice",
            Surrogate::new(20),
        )
        .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, t1, "users", b"alice")
                .unwrap(),
            Some(Surrogate::new(10))
        );
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, t2, "users", b"alice")
                .unwrap(),
            Some(Surrogate::new(20))
        );
    }

    #[test]
    fn missing_returns_none() {
        let (_dir, cat) = open_catalog();
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, T0, "users", b"nobody")
                .unwrap(),
            None
        );
    }

    #[test]
    fn delete_is_idempotent_and_removes_both_directions() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            T0,
            "users",
            b"alice",
            Surrogate::new(7),
        )
        .unwrap();
        cat.delete_surrogate(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, T0, "users", b"alice")
                .unwrap(),
            None
        );
        cat.delete_surrogate(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
    }

    #[test]
    fn scan_returns_only_named_collection() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            T0,
            "users",
            b"alice",
            Surrogate::new(1),
        )
        .unwrap();
        cat.put_surrogate(DatabaseId::DEFAULT, T0, "users", b"bob", Surrogate::new(2))
            .unwrap();
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            T0,
            "orders",
            b"alice",
            Surrogate::new(3),
        )
        .unwrap();
        // A different tenant's same-named collection must not leak into the scan.
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            TenantId::new(9),
            "users",
            b"carol",
            Surrogate::new(4),
        )
        .unwrap();
        let mut got = cat
            .scan_surrogates_for_collection(DatabaseId::DEFAULT, T0, "users")
            .unwrap();
        got.sort();
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
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            T0,
            "users",
            b"alice",
            Surrogate::new(1),
        )
        .unwrap();
        cat.put_surrogate(DatabaseId::DEFAULT, T0, "orders", b"o1", Surrogate::new(2))
            .unwrap();
        cat.delete_all_surrogates_for_collection(DatabaseId::DEFAULT, T0, "users")
            .unwrap();
        assert!(
            cat.scan_surrogates_for_collection(DatabaseId::DEFAULT, T0, "users")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, T0, "orders", b"o1")
                .unwrap(),
            Some(Surrogate::new(2))
        );
        // double-delete is a no-op
        cat.delete_all_surrogates_for_collection(DatabaseId::DEFAULT, T0, "users")
            .unwrap();
    }

    // ── Migration tests ───────────────────────────────────────────────────

    fn insert_legacy_fwd(cat: &SystemCatalog, coll: &str, pk: &[u8], surrogate: u32) {
        let txn = cat.db.begin_write().unwrap();
        {
            let mut t = txn.open_table(SURROGATE_PK_LEGACY).unwrap();
            t.insert((coll, pk), surrogate).unwrap();
            let mut r = txn.open_table(SURROGATE_PK_REV_LEGACY).unwrap();
            r.insert((coll, surrogate), pk).unwrap();
        }
        txn.commit().unwrap();
    }

    /// Read a v2 forward row directly (the v1→v2 migration target).
    fn get_v2_fwd(cat: &SystemCatalog, db_id: u64, coll: &str, pk: &[u8]) -> Option<u32> {
        let txn = cat.db.begin_read().unwrap();
        let table = txn.open_table(SURROGATE_PK_V2).unwrap();
        table.get((db_id, coll, pk)).unwrap().map(|v| v.value())
    }

    #[test]
    fn fresh_boot_migration_is_noop() {
        let (_dir, cat) = open_catalog();
        cat.migrate_surrogate_pk().unwrap();
        cat.migrate_surrogate_pk_v3().unwrap();
        assert!(
            cat.scan_surrogates_for_collection(DatabaseId::DEFAULT, T0, "users")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn pre_migration_boot_migrates_rows_v1_to_v2() {
        let (_dir, cat) = open_catalog();
        insert_legacy_fwd(&cat, "users", b"alice", 7);
        cat.migrate_surrogate_pk().unwrap();
        assert_eq!(
            get_v2_fwd(&cat, DatabaseId::DEFAULT.as_u64(), "users", b"alice"),
            Some(7)
        );
    }

    #[test]
    fn already_migrated_boot_is_idempotent_v1_to_v2() {
        let (_dir, cat) = open_catalog();
        // v2 row already exists, written under the default tenant in v3 …
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            T0,
            "users",
            b"alice",
            Surrogate::new(7),
        )
        .unwrap();
        // … and a v2 row to make `migrate_surrogate_pk` see a non-empty target.
        {
            let txn = cat.db.begin_write().unwrap();
            {
                let mut t = txn.open_table(SURROGATE_PK_V2).unwrap();
                t.insert(
                    (DatabaseId::DEFAULT.as_u64(), "users", b"alice".as_slice()),
                    7,
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }
        // also insert a legacy row
        insert_legacy_fwd(&cat, "users", b"bob", 8);
        // migration should be a no-op (v2 already non-empty)
        cat.migrate_surrogate_pk().unwrap();
        assert_eq!(
            get_v2_fwd(&cat, DatabaseId::DEFAULT.as_u64(), "users", b"bob"),
            None
        );
    }

    #[test]
    fn migrate_v3_rekeys_v2_rows_under_default_tenant() {
        let (_dir, cat) = open_catalog();
        // Seed a v2 row directly (as a pre-v3 boot would have).
        {
            let txn = cat.db.begin_write().unwrap();
            {
                let mut f = txn.open_table(SURROGATE_PK_V2).unwrap();
                f.insert(
                    (DatabaseId::DEFAULT.as_u64(), "users", b"alice".as_slice()),
                    7,
                )
                .unwrap();
                let mut r = txn.open_table(SURROGATE_PK_REV_V2).unwrap();
                r.insert(
                    (DatabaseId::DEFAULT.as_u64(), "users", 7u32),
                    b"alice".as_slice(),
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }
        cat.migrate_surrogate_pk_v3().unwrap();
        // Backfilled rows land under the default user tenant (1), matching the
        // default identity that wrote them pre-upgrade.
        let default_tenant = TenantId::new(1);
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, default_tenant, "users", b"alice")
                .unwrap(),
            Some(Surrogate::new(7))
        );
        assert_eq!(
            cat.get_pk_for_surrogate(DatabaseId::DEFAULT, default_tenant, "users", Surrogate::new(7))
                .unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn migrate_v3_is_idempotent() {
        let (_dir, cat) = open_catalog();
        // v3 already populated …
        cat.put_surrogate(
            DatabaseId::DEFAULT,
            T0,
            "users",
            b"alice",
            Surrogate::new(7),
        )
        .unwrap();
        // … and a stale v2 row that must NOT overwrite it.
        {
            let txn = cat.db.begin_write().unwrap();
            {
                let mut f = txn.open_table(SURROGATE_PK_V2).unwrap();
                f.insert(
                    (DatabaseId::DEFAULT.as_u64(), "users", b"alice".as_slice()),
                    99,
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }
        cat.migrate_surrogate_pk_v3().unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, T0, "users", b"alice")
                .unwrap(),
            Some(Surrogate::new(7))
        );
    }
}
