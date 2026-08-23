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
    catalog: Arc<crate::catalog::ClusterCatalog>,
}

impl CatalogIncarnationStore {
    pub fn new(catalog: Arc<crate::catalog::ClusterCatalog>) -> Self {
        Self { catalog }
    }
}

impl IncarnationStore for CatalogIncarnationStore {
    fn save(&self, incarnation: u64) -> Result<()> {
        self.catalog.save_swim_incarnation(incarnation)
    }

    fn load(&self) -> Result<Option<u64>> {
        self.catalog.load_swim_incarnation()
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
