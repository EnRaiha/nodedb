// SPDX-License-Identifier: BUSL-1.1

//! Idempotent redb key-rewrite migrations for the sparse / document engine.
//!
//! Each migration reads a legacy (pre-database-scoping) table and rewrites
//! every row into its database-scoped `_v2` companion, prepending
//! `DatabaseId::DEFAULT` (0) as the first key component. Migrations are:
//!
//! * **No-op on fresh boot** — the legacy table is absent or empty.
//! * **Idempotent** — once the `_v2` table is non-empty the rewrite is skipped,
//!   so re-running on every core startup is safe.
//! * **Atomic** — the rewrite commits in a single write transaction.
//!
//! redb has no `drop_table`, so the legacy rows remain in place after a
//! migration (orphaned, harmless); live paths only ever touch the `_v2`
//! tables. Old data is preserved as `DatabaseId::DEFAULT`, so it stays
//! readable as the default database.

use redb::{ReadableTableMetadata, TableDefinition};

use super::btree::{DOCUMENTS, DOCUMENTS_LEGACY, INDEXES, INDEXES_LEGACY, SparseEngine, redb_err};
use super::btree_versioned::doc::DOCUMENTS_VERSIONED;
use super::btree_versioned::index::INDEXES_VERSIONED;

/// Legacy (pre-database-scoping) versioned document table.
const DOCUMENTS_VERSIONED_LEGACY: TableDefinition<&str, &[u8]> =
    TableDefinition::new("documents_versioned");

/// Legacy (pre-database-scoping) versioned index table.
const INDEXES_VERSIONED_LEGACY: TableDefinition<&str, &[u8]> =
    TableDefinition::new("indexes_versioned");

impl SparseEngine {
    /// Rewrite legacy `documents` rows into `documents_v2`.
    pub fn migrate_documents_v2(&self) -> crate::Result<()> {
        self.migrate_string_table(DOCUMENTS_LEGACY, DOCUMENTS, "migrate_documents_v2")
    }

    /// Rewrite legacy `indexes` rows into `indexes_v2`.
    pub fn migrate_indexes_v2(&self) -> crate::Result<()> {
        self.migrate_string_table(INDEXES_LEGACY, INDEXES, "migrate_indexes_v2")
    }

    /// Rewrite legacy `documents_versioned` rows into `documents_versioned_v2`.
    ///
    /// The version separator (`\x00`) lives in the key *after* the
    /// `{tenant}:{coll}:{doc_id}` prefix, so prepending `{db}:` to the front
    /// preserves the existing `(doc_id, sys_from)` ordering within every group.
    pub fn migrate_documents_versioned_v2(&self) -> crate::Result<()> {
        self.migrate_string_table(
            DOCUMENTS_VERSIONED_LEGACY,
            DOCUMENTS_VERSIONED,
            "migrate_documents_versioned_v2",
        )
    }

    /// Rewrite legacy `indexes_versioned` rows into `indexes_versioned_v2`.
    pub fn migrate_indexes_versioned_v2(&self) -> crate::Result<()> {
        self.migrate_string_table(
            INDEXES_VERSIONED_LEGACY,
            INDEXES_VERSIONED,
            "migrate_indexes_versioned_v2",
        )
    }

    /// Shared rewrite logic for a `&str → &[u8]` redb table whose keys gain a
    /// leading `{database_id}:` component. The legacy and v2 tables differ only
    /// in name; the v2 table name carries the version suffix.
    fn migrate_string_table(
        &self,
        legacy: TableDefinition<&str, &[u8]>,
        v2: TableDefinition<&str, &[u8]>,
        ctx: &str,
    ) -> crate::Result<()> {
        // Gather legacy rows.
        let rows: Vec<(String, Vec<u8>)> = {
            let txn = self.db.begin_read().map_err(|e| redb_err(ctx, e))?;
            match txn.open_table(legacy) {
                Ok(table) => {
                    let iter = table.range::<&str>(..).map_err(|e| redb_err(ctx, e))?;
                    let mut out = Vec::new();
                    for entry in iter {
                        let (k, v) = entry.map_err(|e| redb_err(ctx, e))?;
                        out.push((k.value().to_string(), v.value().to_vec()));
                    }
                    out
                }
                Err(_) => Vec::new(),
            }
        };

        if rows.is_empty() {
            return Ok(());
        }

        // Skip if v2 already populated (already migrated).
        let v2_empty = {
            let txn = self.db.begin_read().map_err(|e| redb_err(ctx, e))?;
            match txn.open_table(v2) {
                Ok(table) => table.is_empty().map_err(|e| redb_err(ctx, e))?,
                Err(_) => true,
            }
        };
        if !v2_empty {
            return Ok(());
        }

        let db_id = nodedb_types::DatabaseId::DEFAULT.as_u64();
        let txn = self.db.begin_write().map_err(|e| redb_err(ctx, e))?;
        {
            let mut table = txn.open_table(v2).map_err(|e| redb_err(ctx, e))?;
            for (old_key, value) in &rows {
                let new_key = format!("{db_id}:{old_key}");
                table
                    .insert(new_key.as_str(), value.as_slice())
                    .map_err(|e| redb_err(ctx, e))?;
            }
        }
        txn.commit().map_err(|e| redb_err(ctx, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::Database;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (engine, dir)
    }

    /// Seed a legacy un-scoped row directly into the old `documents` table.
    fn seed_legacy_doc(engine: &SparseEngine, key: &str, value: &[u8]) {
        let txn = engine.db().begin_write().unwrap();
        {
            let mut table = txn.open_table(DOCUMENTS_LEGACY).unwrap();
            table.insert(key, value).unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn migrate_rewrites_legacy_under_default_database() {
        let (engine, _dir) = open_temp();
        // Legacy key shape: "{tenant}:{coll}:{doc_id}".
        seed_legacy_doc(&engine, "1:users:u1", b"alice");

        engine.migrate_documents_v2().unwrap();

        // Readable under DatabaseId::DEFAULT (0).
        assert_eq!(
            engine.get(0, 1, "users", "u1").unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn migrate_is_idempotent() {
        let (engine, _dir) = open_temp();
        seed_legacy_doc(&engine, "1:users:u1", b"alice");
        engine.migrate_documents_v2().unwrap();

        // A live write into a different database must survive a re-run.
        engine.put(7, 1, "users", "u9", b"db7").unwrap();
        engine.migrate_documents_v2().unwrap();

        assert_eq!(
            engine.get(0, 1, "users", "u1").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(7, 1, "users", "u9").unwrap(),
            Some(b"db7".to_vec())
        );
    }

    #[test]
    fn migrate_is_noop_on_fresh_boot() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("s.redb")).unwrap();
        let engine = SparseEngine {
            db: std::sync::Arc::new(db),
        };
        // No legacy table at all — must not error.
        engine.migrate_documents_v2().unwrap();
        engine.migrate_indexes_v2().unwrap();
    }
}
