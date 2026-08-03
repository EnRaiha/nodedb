// SPDX-License-Identifier: BUSL-1.1

//! `ClusterCatalog` struct, `open`, and metadata persistence
//! (cluster id, CA cert, bootstrap check).
//!
//! `open` is the single entry point that walks the format-version
//! key and delegates to the migration runner — no other file in
//! this module touches that key directly.

use std::path::Path;

use redb::ReadableDatabase;
use tracing::info;

use crate::error::Result;

use super::migration::migrate_if_needed;
use super::schema::{
    CATALOG_FORMAT_VERSION, GHOST_TABLE, KEY_CA_CERT, KEY_CLUSTER_EPOCH, KEY_CLUSTER_ID,
    KEY_FORMAT_VERSION, METADATA_TABLE, MIGRATION_STATE_TABLE, ROUTING_TABLE, TOPOLOGY_TABLE,
    catalog_err,
};

/// Persistent cluster catalog backed by redb.
pub struct ClusterCatalog {
    pub(super) db: redb::Database,
}

impl ClusterCatalog {
    /// Open or create the cluster catalog at the given path.
    ///
    /// Delegates to [`super::migration::migrate_if_needed`] after
    /// the redb tables are in place. Fresh catalogs get stamped
    /// with the current format version; catalogs stamped with a
    /// higher version than this binary supports are refused with
    /// a clear error (preventing silent corruption on an
    /// accidental downgrade). Future schema changes land as
    /// explicit migration arms in `migration.rs`.
    pub fn open(path: &Path) -> Result<Self> {
        let db =
            redb::Database::create(path).map_err(|e| crate::error::ClusterError::Transport {
                detail: format!("open cluster catalog {}: {e}", path.display()),
            })?;

        // Ensure every table exists before we try to read from any of
        // them. redb requires tables to be created inside a write txn
        // before they can be opened read-only.
        let txn = db.begin_write().map_err(catalog_err)?;
        {
            let _ = txn.open_table(TOPOLOGY_TABLE).map_err(catalog_err)?;
            let _ = txn.open_table(ROUTING_TABLE).map_err(catalog_err)?;
            let _ = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            let _ = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;
            let _ = txn.open_table(MIGRATION_STATE_TABLE).map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;

        // Stamp or validate the on-disk format version. Every
        // branch is funnelled through `migrate_if_needed` so there
        // is a single place to add future migration arms.
        migrate_if_needed(&db)?;

        info!(
            path = %path.display(),
            format_version = CATALOG_FORMAT_VERSION,
            "cluster catalog opened"
        );

        Ok(Self { db })
    }

    // ── Metadata ────────────────────────────────────────────────────

    /// Store the cluster ID (generated at bootstrap, immutable).
    pub fn save_cluster_id(&self, cluster_id: u64) -> Result<()> {
        let bytes = cluster_id.to_le_bytes();
        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            table
                .insert(KEY_CLUSTER_ID, bytes.as_slice())
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load the cluster ID. Returns None if not yet bootstrapped.
    pub fn load_cluster_id(&self) -> Result<Option<u64>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;

        match table.get(KEY_CLUSTER_ID).map_err(catalog_err)? {
            Some(guard) => {
                let bytes = guard.value();
                if bytes.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(bytes);
                    let id = u64::from_le_bytes(arr);
                    Ok(Some(id))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Check if this catalog has been bootstrapped (has a cluster_id).
    pub fn is_bootstrapped(&self) -> Result<bool> {
        self.load_cluster_id().map(|id| id.is_some())
    }

    /// Persist the cluster epoch (the leader-bumped monotonic fence
    /// token stamped on every Raft RPC). Overwrites any prior value.
    pub fn save_cluster_epoch(&self, epoch: u64) -> Result<()> {
        let bytes = epoch.to_le_bytes();
        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            table
                .insert(KEY_CLUSTER_EPOCH, bytes.as_slice())
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load the persisted cluster epoch. Returns `None` on a catalog
    /// that has never written one (callers treat that as 0).
    pub fn load_cluster_epoch(&self) -> Result<Option<u64>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
        match table.get(KEY_CLUSTER_EPOCH).map_err(catalog_err)? {
            Some(guard) => {
                let bytes = guard.value();
                if bytes.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(bytes);
                    Ok(Some(u64::from_le_bytes(arr)))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    // ── TLS Certificates ────────────────────────────────────────────

    /// Store the cluster CA certificate (DER-encoded).
    pub fn save_ca_cert(&self, ca_cert_der: &[u8]) -> Result<()> {
        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            table
                .insert(KEY_CA_CERT, ca_cert_der)
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load the cluster CA certificate. Returns None if not bootstrapped.
    pub fn load_ca_cert(&self) -> Result<Option<Vec<u8>>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
        match table.get(KEY_CA_CERT).map_err(catalog_err)? {
            Some(guard) => Ok(Some(guard.value().to_vec())),
            None => Ok(None),
        }
    }
}

/// Read the current catalog format version from the metadata table,
/// or `None` if the key hasn't been written yet.
///
/// Shared helper used by both `open` and the migration runner.
pub(super) fn read_format_version(db: &redb::Database) -> Result<Option<u32>> {
    let txn = db.begin_read().map_err(catalog_err)?;
    let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
    match table.get(KEY_FORMAT_VERSION).map_err(catalog_err)? {
        Some(guard) => {
            let bytes = guard.value();
            if bytes.len() != 4 {
                return Ok(None);
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(bytes);
            Ok(Some(u32::from_le_bytes(arr)))
        }
        None => Ok(None),
    }
}

/// Stamp the catalog with the given format version.
///
/// Called from the migration runner — both on fresh catalogs
/// (to record the initial version) and, in the future, after a
/// successful upgrade arm.
pub(super) fn write_format_version(db: &redb::Database, version: u32) -> Result<()> {
    let bytes = version.to_le_bytes();
    let txn = db.begin_write().map_err(catalog_err)?;
    {
        let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
        table
            .insert(KEY_FORMAT_VERSION, bytes.as_slice())
            .map_err(catalog_err)?;
    }
    txn.commit().map_err(catalog_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[test]
    fn cluster_id_persistence() {
        let (_dir, catalog) = temp_catalog();

        assert!(!catalog.is_bootstrapped().unwrap());
        assert_eq!(catalog.load_cluster_id().unwrap(), None);

        catalog.save_cluster_id(42).unwrap();
        assert!(catalog.is_bootstrapped().unwrap());
        assert_eq!(catalog.load_cluster_id().unwrap(), Some(42));
    }

    #[test]
    fn fresh_catalog_is_stamped_with_current_format_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        {
            let _ = ClusterCatalog::open(&path).unwrap();
        }
        // Reopen via a bare redb handle and verify the version key
        // is present.
        let db = redb::Database::create(&path).unwrap();
        let version = read_format_version(&db).unwrap();
        assert_eq!(version, Some(CATALOG_FORMAT_VERSION));
    }

    #[test]
    fn reopening_current_catalog_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let _ = ClusterCatalog::open(&path).unwrap();
        // Second open must succeed without triggering any migration
        // error and without changing the stamped version.
        let _ = ClusterCatalog::open(&path).unwrap();
        let db = redb::Database::create(&path).unwrap();
        assert_eq!(
            read_format_version(&db).unwrap(),
            Some(CATALOG_FORMAT_VERSION)
        );
    }

    #[test]
    fn future_format_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        // Create a catalog, then manually stamp a future version to
        // simulate a downgrade attempt.
        {
            let _ = ClusterCatalog::open(&path).unwrap();
        }
        {
            let db = redb::Database::create(&path).unwrap();
            write_format_version(&db, CATALOG_FORMAT_VERSION + 1).unwrap();
        }
        // Reopening must refuse with a clear error. Match on the
        // `Err` variant directly so we don't require
        // `ClusterCatalog: Debug` for `unwrap_err`.
        match ClusterCatalog::open(&path) {
            Ok(_) => panic!("expected downgrade refusal, got Ok"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("newer than supported"),
                    "expected a clear downgrade refusal, got: {msg}"
                );
            }
        }
    }
}
