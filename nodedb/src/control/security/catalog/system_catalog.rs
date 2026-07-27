// SPDX-License-Identifier: BUSL-1.1

//! SystemCatalog: redb-backed persistent catalog database.
//!
//! Opens or creates the system.redb file, initializes all tables,
//! and provides raw WASM module storage methods.

use std::path::Path;
use std::sync::Arc;

use redb::Database;
use tracing::info;

use super::types::*;

/// Cloneable handle to the redb-backed system catalog. Clones share one
/// underlying `redb::Database` (one file open), so the catalog can be shared
/// between the credential store and the sync producer registry without a
/// second `Database::create` on the same path (which redb rejects).
#[derive(Clone)]
pub struct SystemCatalog {
    pub(super) db: Arc<Database>,
    pub(super) crdt_signing_root: Arc<std::sync::RwLock<Option<[u8; 32]>>>,
    #[cfg(test)]
    pub(super) fail_next_user_counter_write: Arc<std::sync::atomic::AtomicBool>,
}

impl SystemCatalog {
    /// Open or create the system catalog at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| catalog_err("open", e))?;

        if Self::ensure_bootstrapped(&db)? {
            info!(path = %path.display(), "system catalog opened (bootstrapped)");
        } else {
            info!(path = %path.display(), "system catalog opened");
        }

        Ok(Self {
            db: Arc::new(db),
            crdt_signing_root: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(test)]
            fail_next_user_counter_write: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Open a non-persistent system catalog backed by an in-memory redb
    /// database. Used by in-process credential stores that need a real,
    /// fully-bootstrapped catalog without touching the filesystem.
    pub fn open_in_memory() -> crate::Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| catalog_err("open in-memory", e))?;
        Self::ensure_bootstrapped(&db)?;
        Ok(Self {
            db: Arc::new(db),
            crdt_signing_root: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(test)]
            fail_next_user_counter_write: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    #[cfg(test)]
    pub(crate) fn fail_next_user_counter_write_for_test(&self) {
        self.fail_next_user_counter_write
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Bootstrap every `_system.*` table from the canonical registry —
    /// but only if at least one is actually missing. Probing read-only
    /// first keeps `open` byte-idempotent on an already-bootstrapped
    /// catalog: a write transaction + commit stamps a fresh meta/commit
    /// page on redb every time, so an unconditional bootstrap rewrites
    /// `system.redb` on every boot (changing its size/md5) even when
    /// nothing changed — and a boot that then fails its integrity check
    /// would have mutated persistent catalog state on its way out.
    /// Opening a table in a write transaction creates it if absent; the
    /// registry is the single source of truth, so a table cannot be
    /// read in production code without being bootstrapped here. Returns
    /// `true` when a bootstrap write actually ran.
    fn ensure_bootstrapped(db: &Database) -> crate::Result<bool> {
        let needs_bootstrap = match db.begin_read() {
            Ok(read_txn) => super::bootstrap_tables::BOOTSTRAP_TABLES
                .iter()
                .any(|table| (table.probe)(&read_txn).is_err()),
            // A read transaction on a brand-new database can fail before
            // the first commit; treat that as "bootstrap needed".
            Err(_) => true,
        };
        if needs_bootstrap {
            let write_txn = db.begin_write().map_err(|e| catalog_err("init txn", e))?;
            for table in super::bootstrap_tables::BOOTSTRAP_TABLES {
                (table.create)(&write_txn)
                    .map_err(|e| catalog_err(&format!("init {} table", table.label), e))?;
            }
            write_txn
                .commit()
                .map_err(|e| catalog_err("init commit", e))?;
        }
        Ok(needs_bootstrap)
    }

    /// Execute a write transaction on the WASM_MODULES table.
    fn wasm_write<F, T>(&self, op: &str, f: F) -> crate::Result<T>
    where
        F: FnOnce(&mut redb::Table<&str, &[u8]>) -> crate::Result<T>,
    {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err(&format!("{op} txn"), e))?;
        let result = {
            let mut table = txn
                .open_table(WASM_MODULES)
                .map_err(|e| catalog_err(&format!("{op} open"), e))?;
            f(&mut table)?
        };
        txn.commit()
            .map_err(|e| catalog_err(&format!("{op} commit"), e))?;
        Ok(result)
    }

    /// Store raw bytes under a string key in the WASM_MODULES table.
    pub fn put_raw(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("put_raw key", e))?;
        self.wasm_write("put_raw", |table| {
            table
                .insert(key_str, value)
                .map_err(|e| catalog_err("put_raw insert", e))?;
            Ok(())
        })
    }

    /// Load raw bytes by string key from the WASM_MODULES table.
    pub fn get_raw(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("get_raw key", e))?;
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("get_raw txn", e))?;
        let table = txn
            .open_table(WASM_MODULES)
            .map_err(|e| catalog_err("get_raw open", e))?;
        match table
            .get(key_str)
            .map_err(|e| catalog_err("get_raw get", e))?
        {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Delete raw bytes by string key from the WASM_MODULES table.
    pub fn delete_raw(&self, key: &[u8]) -> crate::Result<()> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("delete_raw key", e))?;
        self.wasm_write("delete_raw", |table| {
            table
                .remove(key_str)
                .map_err(|e| catalog_err("delete_raw remove", e))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::auth_types::StoredUser;
    use super::*;

    #[test]
    fn open_and_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).unwrap();

        let user = StoredUser {
            user_id: 1,
            username: "alice".into(),
            tenant_id: 1,
            password_hash: "$argon2id$test".into(),
            scram_salt: vec![1, 2, 3, 4],
            scram_salted_password: vec![5, 6, 7, 8],
            roles: vec!["readwrite".into()],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: 0,
            default_database_id: 0,
            accessible_databases: vec![],
        };

        catalog.put_user(&user).unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].username, "alice");
        assert_eq!(loaded[0].tenant_id, 1);
    }

    #[test]
    fn delete_user() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();

        let user = StoredUser {
            user_id: 1,
            username: "bob".into(),
            tenant_id: 1,
            password_hash: "hash".into(),
            scram_salt: vec![],
            scram_salted_password: vec![],
            roles: vec![],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: 0,
            default_database_id: 0,
            accessible_databases: vec![],
        };

        catalog.put_user(&user).unwrap();
        catalog.delete_user("bob").unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn bootstrap_creates_every_registered_table() {
        // A fresh catalog must already contain every table in the
        // bootstrap registry, so boot-time readers (integrity walk,
        // continuous-aggregate replay, …) open existing empty tables
        // instead of hitting "table does not exist". Re-opening each
        // entry read-only would fail with `TableDoesNotExist` if the
        // init path ever stopped iterating the registry.
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let txn = catalog.db.begin_read().unwrap();
        for table in super::super::bootstrap_tables::BOOTSTRAP_TABLES {
            (table.probe)(&txn)
                .unwrap_or_else(|e| panic!("table `{}` missing after bootstrap: {e}", table.label));
        }
    }

    #[test]
    fn reopening_a_bootstrapped_catalog_does_not_mutate_the_file() {
        // `SystemCatalog::open` must be byte-idempotent on an
        // already-bootstrapped catalog: opening it to *read* must not
        // rewrite `system.redb`. Today `open` unconditionally runs a
        // write transaction (iterating BOOTSTRAP_TABLES) and commits it,
        // so redb writes a fresh meta page / reclaims pages on every
        // boot — the file's bytes (and md5, and size) change even when
        // nothing else does. That is the "system.redb silently modified
        // during a broken boot" symptom: a boot that then fails its
        // integrity check has still mutated persistent catalog state, so
        // operators can't verify a backup by hash or trust "did the
        // upgrade touch my catalog?". `open` should probe the registry
        // with a read transaction and only escalate to a write txn if a
        // table is actually missing.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        // First open: bootstrap happens here, file reaches steady state.
        drop(SystemCatalog::open(&path).unwrap());
        let before = std::fs::read(&path).unwrap();

        // Second open of the same, fully-bootstrapped catalog — a pure
        // "bring the catalog up to read it" boot. Must not touch a byte.
        drop(SystemCatalog::open(&path).unwrap());
        let after = std::fs::read(&path).unwrap();

        let first_diff = before
            .iter()
            .zip(after.iter())
            .position(|(a, b)| a != b)
            .map(|i| i.to_string())
            .unwrap_or_else(|| "len".to_string());
        assert!(
            before == after,
            "re-opening an already-bootstrapped catalog rewrote system.redb \
             (len {} → {}, first differing offset: {first_diff}): `open` runs \
             an unconditional write transaction + commit, so redb stamps a \
             fresh meta/commit page on every boot. Opening the catalog to \
             read it must not mutate it — probe the bootstrap registry \
             read-only and only open a write transaction when a table is \
             genuinely absent.",
            before.len(),
            after.len(),
        );
    }

    #[test]
    fn next_user_id_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = SystemCatalog::open(&path).unwrap();
            assert_eq!(catalog.load_next_user_id().unwrap(), 1);
            catalog.save_next_user_id(42).unwrap();
        }

        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.load_next_user_id().unwrap(), 42);
    }

    #[test]
    fn survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .put_user(&StoredUser {
                    user_id: 5,
                    username: "persistent".into(),
                    tenant_id: 3,
                    password_hash: "hash".into(),
                    scram_salt: vec![1],
                    scram_salted_password: vec![2],
                    roles: vec!["readonly".into(), "monitor".into()],
                    is_superuser: false,
                    is_active: true,
                    is_service_account: false,
                    created_at: 0,
                    updated_at: 0,
                    password_expires_at: 0,
                    must_change_password: false,
                    password_changed_at: 0,
                    default_database_id: 0,
                    accessible_databases: vec![],
                })
                .unwrap();
        }

        let catalog = SystemCatalog::open(&path).unwrap();
        let users = catalog.load_all_users().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "persistent");
        assert_eq!(users[0].user_id, 5);
        assert_eq!(users[0].roles, vec!["readonly", "monitor"]);
    }
}
