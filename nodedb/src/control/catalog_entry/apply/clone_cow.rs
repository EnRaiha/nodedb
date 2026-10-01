// SPDX-License-Identifier: BUSL-1.1

//! Apply clone copy-on-write catalog entries.
//!
//! Each row is keyed by the clone collection's database-qualified name. A
//! row is written only while that collection is still a clone: once it is
//! materialized, purged, or dropped, the rows it had are gone and a replay of
//! an older entry writes nothing. Every write is an upsert, so a re-delivery
//! leaves the same state.

use nodedb_types::DatabaseId;

use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::SystemCatalog;

/// The clone collection a copy-on-write entry names.
#[derive(Debug, Clone, Copy)]
pub struct CloneTarget<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
}

impl CloneTarget<'_> {
    /// The database-qualified key the copy-on-write tables use, or `None`
    /// when the collection is no longer a clone.
    fn live_key(&self, catalog: &SystemCatalog) -> crate::Result<Option<String>> {
        let database_id = DatabaseId::new(self.database_id);
        let is_clone = catalog
            .get_collection(database_id, self.tenant_id, self.collection)?
            .is_some_and(|coll| coll.cloned_from.is_some());
        Ok(is_clone.then(|| db_qualified(database_id, self.collection)))
    }
}

/// Apply `PutCloneCopyup`.
pub fn put_copyup(
    target: CloneTarget<'_>,
    source_surrogate: u32,
    target_surrogate: u32,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    if let Some(key) = target.live_key(catalog)? {
        catalog.put_clone_copyup(&key, source_surrogate, target_surrogate)?;
    }
    Ok(())
}

/// Apply `PutCloneTombstone`.
pub fn put_tombstone(
    target: CloneTarget<'_>,
    source_surrogate: u32,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    if let Some(key) = target.live_key(catalog)? {
        catalog.put_clone_tombstone(&key, source_surrogate)?;
    }
    Ok(())
}

/// Apply `PutKvCloneTombstone`.
pub fn put_kv_tombstone(
    target: CloneTarget<'_>,
    kv_key: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    if let Some(key) = target.live_key(catalog)? {
        catalog.put_kv_clone_tombstone(&key, kv_key)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::{CloneOrigin, CloneStatus, Lsn};

    use super::*;
    use crate::control::security::catalog::StoredCollection;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn clone_collection(db: DatabaseId) -> StoredCollection {
        let mut coll = StoredCollection::stamped_for_test(1, "orders", "admin");
        coll.database_id = db;
        coll.cloned_from = Some(CloneOrigin {
            source_database: DatabaseId::new(1024),
            source_collection: "orders".into(),
            as_of_lsn: Lsn::new(1),
            clone_created_at: Lsn::new(2),
            kv_surrogate_ceiling: None,
        });
        coll
    }

    /// Rows land while the collection is a clone, twice without change, and
    /// never once it is materialized.
    #[test]
    fn writes_only_while_the_collection_is_a_clone() {
        let (_dir, catalog) = open_catalog();
        let db = DatabaseId::new(1030);
        let mut coll = clone_collection(db);
        catalog.put_collection(db, &coll).unwrap();
        let target = CloneTarget {
            database_id: db.as_u64(),
            tenant_id: 1,
            collection: "orders",
        };
        let key = db_qualified(db, "orders");

        for _ in 0..2 {
            put_tombstone(target, 7, &catalog).unwrap();
            put_kv_tombstone(target, "k1", &catalog).unwrap();
            put_copyup(target, 9, 90, &catalog).unwrap();
        }
        assert!(catalog.is_clone_tombstoned(&key, 7).unwrap());
        assert!(
            catalog
                .list_kv_clone_tombstones(&key)
                .unwrap()
                .contains("k1")
        );
        assert!(catalog.get_clone_copyup(&key, 9).unwrap().is_some());

        coll.cloned_from = None;
        coll.clone_status = CloneStatus::Materialized;
        catalog.put_collection(db, &coll).unwrap();
        put_tombstone(target, 8, &catalog).unwrap();
        assert!(!catalog.is_clone_tombstoned(&key, 8).unwrap());
    }
}
