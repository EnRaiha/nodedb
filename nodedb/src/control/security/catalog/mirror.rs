// SPDX-License-Identifier: BUSL-1.1

//! Catalog persistence for the mirror subsystem.
//!
//! Two redb tables are managed here:
//!
//! - `_system.mirror_collection_map` — maps `(mirror_database_id, source_collection_name)` →
//!   `local_collection_name` for each collection replicated from the source.
//! - `_system.mirror_lag` — stores `MirrorLagRecord` per mirror database (last applied LSN
//!   and wall-clock timestamp), used for observability and `BoundedStaleness` gating.
//!
//! DDL apply atomicity rule: every caller that updates the collection map **must** also
//! update the lag record in the **same** redb write transaction. The
//! `apply_ddl_entry_atomic` method on `SystemCatalog` enforces this by accepting both
//! mutations together.

use nodedb_types::{DatabaseId, Lsn, MirrorLagRecord};
use redb::{ReadableDatabase, ReadableTable};

use super::types::{MIRROR_COLLECTION_MAP, MIRROR_LAG, SystemCatalog, catalog_err};

impl SystemCatalog {
    // ── mirror_collection_map ─────────────────────────────────────────────────

    /// Look up the local collection name for a given `(mirror_database_id, source_collection_name)`.
    ///
    /// Returns `None` if no mapping has been recorded yet (first DDL entry for this collection).
    pub fn get_mirror_collection_mapping(
        &self,
        mirror_db_id: DatabaseId,
        source_collection_name: &str,
    ) -> crate::Result<Option<String>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("mirror_collection_map begin_read", e))?;
        let table = txn
            .open_table(MIRROR_COLLECTION_MAP)
            .map_err(|e| catalog_err("open mirror_collection_map", e))?;
        let key = (mirror_db_id.as_u64(), source_collection_name);
        match table
            .get(key)
            .map_err(|e| catalog_err("get mirror_collection_map", e))?
        {
            Some(bytes) => {
                let local_name: String = zerompk::from_msgpack(bytes.value()).map_err(|e| {
                    catalog_err(
                        &format!(
                            "deserialize mirror_collection_map entry for db={} src={}",
                            mirror_db_id.as_u64(),
                            source_collection_name
                        ),
                        e,
                    )
                })?;
                Ok(Some(local_name))
            }
            None => Ok(None),
        }
    }

    /// List all `(source_collection_name, local_collection_name)` pairs for a mirror database.
    pub fn list_mirror_collection_mappings(
        &self,
        mirror_db_id: DatabaseId,
    ) -> crate::Result<Vec<(String, String)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("mirror_collection_map begin_read (list)", e))?;
        let table = txn
            .open_table(MIRROR_COLLECTION_MAP)
            .map_err(|e| catalog_err("open mirror_collection_map (list)", e))?;

        let db_u64 = mirror_db_id.as_u64();
        let mut result = Vec::new();
        for entry in table
            .range((db_u64, "")..)
            .map_err(|e| catalog_err("range mirror_collection_map", e))?
        {
            let (key, val) = entry.map_err(|e| catalog_err("iter mirror_collection_map", e))?;
            let (entry_db_id, src_name) = key.value();
            if entry_db_id != db_u64 {
                break;
            }
            let local_name: String = zerompk::from_msgpack(val.value())
                .map_err(|e| catalog_err("deserialize mirror_collection_map list entry", e))?;
            result.push((src_name.to_string(), local_name));
        }
        Ok(result)
    }

    /// Remove all collection map entries for a mirror database.
    ///
    /// Called during `DROP DATABASE` on a mirror after the link has been torn down.
    pub fn delete_mirror_collection_map(&self, mirror_db_id: DatabaseId) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("mirror_collection_map begin_write (delete)", e))?;
        {
            let mut table = txn
                .open_table(MIRROR_COLLECTION_MAP)
                .map_err(|e| catalog_err("open mirror_collection_map (delete)", e))?;
            let db_u64 = mirror_db_id.as_u64();
            // Collect keys first to avoid mutating while iterating.
            let keys: Vec<(u64, String)> = table
                .range((db_u64, "")..)
                .map_err(|e| catalog_err("range mirror_collection_map (delete)", e))?
                .take_while(|r| {
                    r.as_ref()
                        .map(|(k, _)| k.value().0 == db_u64)
                        .unwrap_or(false)
                })
                .map(|r| r.map(|(k, _)| (k.value().0, k.value().1.to_string())))
                .collect::<Result<_, _>>()
                .map_err(|e| catalog_err("collect mirror_collection_map keys (delete)", e))?;
            for (db_id, src_name) in &keys {
                table
                    .remove((*db_id, src_name.as_str()))
                    .map_err(|e| catalog_err("remove mirror_collection_map entry", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("mirror_collection_map delete commit", e))
    }

    // ── mirror_lag ────────────────────────────────────────────────────────────

    /// Load the `MirrorLagRecord` for a mirror database.
    ///
    /// Returns `None` when the mirror has not yet applied any Raft entry
    /// (i.e. still bootstrapping from snapshot with no log catchup started).
    pub fn get_mirror_lag(
        &self,
        mirror_db_id: DatabaseId,
    ) -> crate::Result<Option<MirrorLagRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("mirror_lag begin_read", e))?;
        let table = txn
            .open_table(MIRROR_LAG)
            .map_err(|e| catalog_err("open mirror_lag", e))?;
        match table
            .get(mirror_db_id.as_u64())
            .map_err(|e| catalog_err("get mirror_lag", e))?
        {
            Some(bytes) => {
                let record: MirrorLagRecord =
                    zerompk::from_msgpack(bytes.value()).map_err(|e| {
                        catalog_err(
                            &format!("deserialize mirror_lag db={}", mirror_db_id.as_u64()),
                            e,
                        )
                    })?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Persist a `MirrorLagRecord` for a mirror database.
    ///
    /// This is a standalone write — use `apply_ddl_entry_atomic` when the lag
    /// update must be atomic with a collection-map mutation.
    pub fn put_mirror_lag(
        &self,
        mirror_db_id: DatabaseId,
        record: &MirrorLagRecord,
    ) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(record).map_err(|e| catalog_err("serialize mirror_lag", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("mirror_lag begin_write", e))?;
        {
            let mut table = txn
                .open_table(MIRROR_LAG)
                .map_err(|e| catalog_err("open mirror_lag (put)", e))?;
            table
                .insert(mirror_db_id.as_u64(), bytes.as_slice())
                .map_err(|e| catalog_err("insert mirror_lag", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("mirror_lag commit", e))
    }

    /// Remove the lag record for a mirror database.
    ///
    /// Called during `DROP DATABASE` on a mirror.
    pub fn delete_mirror_lag(&self, mirror_db_id: DatabaseId) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("mirror_lag begin_write (delete)", e))?;
        {
            let mut table = txn
                .open_table(MIRROR_LAG)
                .map_err(|e| catalog_err("open mirror_lag (delete)", e))?;
            table
                .remove(mirror_db_id.as_u64())
                .map_err(|e| catalog_err("remove mirror_lag", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("mirror_lag delete commit", e))
    }

    // ── Atomic DDL-apply transaction ──────────────────────────────────────────

    /// Apply a DDL Raft entry atomically: update (or insert) the collection map
    /// entry for `source_collection_name → local_collection_name` and advance
    /// `mirror_lag` to the new `last_applied_lsn` / `last_apply_ms`, all inside
    /// a single redb write transaction.
    ///
    /// If `last_applied_lsn` is already ≥ `entry_lsn`, the operation is a
    /// no-op (idempotent replay after restart).
    ///
    /// Returns `true` if the entry was applied, `false` if it was skipped
    /// because it had already been applied.
    pub fn apply_ddl_entry_atomic(
        &self,
        mirror_db_id: DatabaseId,
        entry_lsn: Lsn,
        entry_apply_ms: u64,
        source_collection_name: &str,
        local_collection_name: &str,
    ) -> crate::Result<bool> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("apply_ddl_entry_atomic begin_write", e))?;

        let applied = {
            // Open lag table as mutable once: we read current LSN for the
            // idempotency check, then write the updated record if needed.
            // Opening the same redb table twice in one write txn is an error.
            let mut lag_table = txn
                .open_table(MIRROR_LAG)
                .map_err(|e| catalog_err("open mirror_lag (atomic)", e))?;

            let current_applied = match lag_table
                .get(mirror_db_id.as_u64())
                .map_err(|e| catalog_err("get mirror_lag (atomic idempotency)", e))?
            {
                Some(bytes) => {
                    let rec: MirrorLagRecord =
                        zerompk::from_msgpack(bytes.value()).map_err(|e| {
                            catalog_err("deserialize mirror_lag (atomic idempotency)", e)
                        })?;
                    rec.last_applied_lsn
                }
                None => Lsn::new(0),
            };

            if current_applied >= entry_lsn {
                // Already applied — idempotent no-op.
                false
            } else {
                // Update collection map.
                let local_bytes = zerompk::to_msgpack_vec(&local_collection_name.to_string())
                    .map_err(|e| catalog_err("serialize local_collection_name", e))?;
                let mut map_table = txn
                    .open_table(MIRROR_COLLECTION_MAP)
                    .map_err(|e| catalog_err("open mirror_collection_map (atomic)", e))?;
                map_table
                    .insert(
                        (mirror_db_id.as_u64(), source_collection_name),
                        local_bytes.as_slice(),
                    )
                    .map_err(|e| catalog_err("insert mirror_collection_map (atomic)", e))?;

                // Advance lag record using the same mutable handle.
                let new_lag = MirrorLagRecord {
                    last_applied_lsn: entry_lsn,
                    last_apply_ms: entry_apply_ms,
                };
                let lag_bytes = zerompk::to_msgpack_vec(&new_lag)
                    .map_err(|e| catalog_err("serialize mirror_lag (atomic)", e))?;
                lag_table
                    .insert(mirror_db_id.as_u64(), lag_bytes.as_slice())
                    .map_err(|e| catalog_err("insert mirror_lag (atomic)", e))?;

                true
            }
        };

        txn.commit()
            .map_err(|e| catalog_err("apply_ddl_entry_atomic commit", e))?;
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nodedb_types::{DatabaseId, Lsn, MirrorLagRecord};
    use tempfile::TempDir;

    use super::*;

    fn open_tmp_catalog(tmp: &TempDir) -> SystemCatalog {
        let path: PathBuf = tmp.path().join("system.redb");
        SystemCatalog::open(&path).expect("open catalog")
    }

    #[test]
    fn mirror_lag_round_trip() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1024);
        let record = MirrorLagRecord {
            last_applied_lsn: Lsn::new(42),
            last_apply_ms: 1_700_000_000_000,
        };
        catalog.put_mirror_lag(db_id, &record).unwrap();
        let loaded = catalog.get_mirror_lag(db_id).unwrap().unwrap();
        assert_eq!(loaded.last_applied_lsn, Lsn::new(42));
        assert_eq!(loaded.last_apply_ms, 1_700_000_000_000);
    }

    #[test]
    fn mirror_lag_returns_none_for_unknown_db() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(9999);
        assert!(catalog.get_mirror_lag(db_id).unwrap().is_none());
    }

    #[test]
    fn apply_ddl_entry_atomic_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1024);
        let lsn = Lsn::new(10);

        // First apply: should succeed.
        let applied = catalog
            .apply_ddl_entry_atomic(db_id, lsn, 1_000, "users", "users")
            .unwrap();
        assert!(applied, "first apply should return true");

        // Second apply with same LSN: idempotent no-op.
        let applied2 = catalog
            .apply_ddl_entry_atomic(db_id, lsn, 2_000, "users", "users")
            .unwrap();
        assert!(!applied2, "second apply of same LSN should return false");

        // Lag record must still reflect the first apply's timestamp.
        let lag = catalog.get_mirror_lag(db_id).unwrap().unwrap();
        assert_eq!(lag.last_apply_ms, 1_000);
    }

    #[test]
    fn apply_ddl_entry_atomic_advances_lag() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1025);

        catalog
            .apply_ddl_entry_atomic(db_id, Lsn::new(5), 500, "orders", "orders")
            .unwrap();
        catalog
            .apply_ddl_entry_atomic(db_id, Lsn::new(7), 700, "products", "products")
            .unwrap();

        let lag = catalog.get_mirror_lag(db_id).unwrap().unwrap();
        assert_eq!(lag.last_applied_lsn, Lsn::new(7));
        assert_eq!(lag.last_apply_ms, 700);

        let mappings = catalog.list_mirror_collection_mappings(db_id).unwrap();
        assert_eq!(mappings.len(), 2);
    }

    #[test]
    fn delete_mirror_collection_map_removes_entries() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1026);

        catalog
            .apply_ddl_entry_atomic(db_id, Lsn::new(1), 100, "col_a", "col_a")
            .unwrap();
        catalog
            .apply_ddl_entry_atomic(db_id, Lsn::new(2), 200, "col_b", "col_b")
            .unwrap();

        catalog.delete_mirror_collection_map(db_id).unwrap();
        let mappings = catalog.list_mirror_collection_mappings(db_id).unwrap();
        assert!(mappings.is_empty());
    }
}
