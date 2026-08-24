// SPDX-License-Identifier: BUSL-1.1

//! Persistence backend for the local SWIM incarnation.
//!
//! The incarnation is the node's monotonic self-epoch. Persisting it
//! closes the fast-restart rejoin gap: a node that crashes while the
//! cluster holds `Dead(A, N)` can restart at `N + 1` and dominate every
//! lingering rumour with its very first announcement — no probabilistic
//! refutation round-trip required.
//!
//! Backends:
//! - [`CatalogIncarnationStore`] — production; writes into the redb
//!   catalog metadata table (same pattern as the cluster epoch).
//! - [`MemIncarnationStore`] — tests only.

use std::sync::Arc;

use crate::error::Result;

/// Persistence for the local SWIM incarnation.
pub trait IncarnationStore: Send + Sync {
    /// Durably record the current incarnation (called on every
    /// self-refutation bump).
    fn save(&self, incarnation: u64) -> Result<()>;
    /// Read the last persisted incarnation; `None` if never written.
    fn load(&self) -> Result<Option<u64>>;
}

/// Catalog-backed store (production path).
pub struct CatalogIncarnationStore {
    /// Weak on purpose. The catalog is a redb database holding an exclusive
    /// file lock, and this store lives inside the failure detector, which
    /// outlives the shutdown sequence that is supposed to close it. An `Arc`
    /// here keeps the database open past shutdown, and the next process to
    /// open the same directory fails with "Database already open" — a node
    /// that stops and restarts against its own data directory cannot come
    /// back. Holding a `Weak` lets the catalog close on schedule.
    catalog: std::sync::Weak<crate::catalog::ClusterCatalog>,
}

impl CatalogIncarnationStore {
    pub fn new(catalog: &Arc<crate::catalog::ClusterCatalog>) -> Self {
        Self {
            catalog: Arc::downgrade(catalog),
        }
    }
}

impl IncarnationStore for CatalogIncarnationStore {
    /// Persist the incarnation, or do nothing if the catalog has already been
    /// closed. A bump arriving during shutdown has nothing left to protect:
    /// the node is going away, and the next start reads whatever the last
    /// completed write left behind.
    fn save(&self, incarnation: u64) -> Result<()> {
        match self.catalog.upgrade() {
            Some(catalog) => catalog.save_swim_incarnation(incarnation),
            None => Ok(()),
        }
    }

    /// Read the persisted incarnation. A closed catalog reads as "never
    /// written", which is the same answer a fresh node gets.
    fn load(&self) -> Result<Option<u64>> {
        match self.catalog.upgrade() {
            Some(catalog) => catalog.load_swim_incarnation(),
            None => Ok(None),
        }
    }
}

/// In-memory store for deterministic tests.
#[cfg(test)]
pub struct MemIncarnationStore {
    value: std::sync::Mutex<Option<u64>>,
}

#[cfg(test)]
impl MemIncarnationStore {
    pub fn new() -> Self {
        Self {
            value: std::sync::Mutex::new(None),
        }
    }
}

#[cfg(test)]
impl Default for MemIncarnationStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl IncarnationStore for MemIncarnationStore {
    fn save(&self, incarnation: u64) -> Result<()> {
        *self.value.lock().unwrap_or_else(|p| p.into_inner()) = Some(incarnation);
        Ok(())
    }

    fn load(&self) -> Result<Option<u64>> {
        Ok(*self.value.lock().unwrap_or_else(|p| p.into_inner()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_store_roundtrips() {
        let store = MemIncarnationStore::new();
        assert_eq!(store.load().unwrap(), None);
        store.save(7).unwrap();
        assert_eq!(store.load().unwrap(), Some(7));
        store.save(9).unwrap();
        assert_eq!(store.load().unwrap(), Some(9));
    }
}
