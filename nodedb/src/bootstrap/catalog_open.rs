// SPDX-License-Identifier: BUSL-1.1

//! Catalog handle for boot steps that only read.
//!
//! redb commits its allocator state when a read-write `Database` drops, so a
//! boot step holding a read-write handle rewrites `system.redb` even when it
//! only read from it. These steps take [`ReadOnlySystemCatalog`] instead, and
//! fall back to a read-write open only when redb reports the file needs
//! repair — repairing has to write, and refusing to boot would be worse.

use std::path::Path;

use nodedb_types::DatabaseId;
use tracing::warn;

use crate::bootstrap::constraint_reconcile::CollectionSource;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::security::catalog::{
    ReadOnlyOpenError, ReadOnlySystemCatalog, StoredCollection, SystemCatalog,
};

/// A catalog opened for a read-only boot step.
pub(crate) enum CatalogForRead {
    /// The normal path: cannot modify the file.
    ReadOnly(ReadOnlySystemCatalog),
    /// The catalog needed repair, so it had to be opened read-write.
    Repaired(SystemCatalog),
}

impl CatalogForRead {
    /// Open `path` for reading. Returns `None` when there is no catalog yet or
    /// it could not be opened at all — callers treat that as "nothing to load".
    pub(crate) fn open(path: &Path) -> Option<Self> {
        match ReadOnlySystemCatalog::open(path) {
            Ok(Some(catalog)) => Some(Self::ReadOnly(catalog)),
            Ok(None) => None,
            Err(ReadOnlyOpenError::RepairRequired) => {
                warn!(
                    path = %path.display(),
                    "catalog was not shut down cleanly; opening read-write to repair it"
                );
                match SystemCatalog::open(path) {
                    Ok(catalog) => Some(Self::Repaired(catalog)),
                    Err(error) => {
                        warn!(error = %error, "failed to open catalog for repair");
                        None
                    }
                }
            }
            Err(error) => {
                warn!(error = %error, "failed to open catalog read-only");
                None
            }
        }
    }

    pub(crate) fn load_all_arrays(&self) -> crate::Result<Vec<ArrayCatalogEntry>> {
        match self {
            Self::ReadOnly(c) => c.load_all_arrays(),
            Self::Repaired(c) => c.load_all_arrays(),
        }
    }

    pub(crate) fn load_wal_tombstones(&self) -> crate::Result<nodedb_wal::TombstoneSet> {
        match self {
            Self::ReadOnly(c) => c.load_wal_tombstones(),
            Self::Repaired(c) => c.load_wal_tombstones(),
        }
    }

    pub(crate) fn list_all_vector_index_params(
        &self,
    ) -> crate::Result<Vec<nodedb_types::StoredVectorIndexParams>> {
        match self {
            Self::ReadOnly(c) => c.list_all_vector_index_params(),
            Self::Repaired(c) => c.list_all_vector_index_params(),
        }
    }
}

impl CollectionSource for CatalogForRead {
    fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>> {
        match self {
            Self::ReadOnly(c) => c.list_database_ids(),
            Self::Repaired(c) => c.list_database_ids(),
        }
    }

    fn collections_in(&self, database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>> {
        match self {
            Self::ReadOnly(c) => c.collections_in(database_id),
            Self::Repaired(c) => c.collections_in(database_id),
        }
    }

    fn collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>> {
        match self {
            Self::ReadOnly(c) => c.collections_for_tenant(database_id, tenant_id),
            Self::Repaired(c) => c.collections_for_tenant(database_id, tenant_id),
        }
    }
}
