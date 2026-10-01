// SPDX-License-Identifier: BUSL-1.1

//! Database hwm persistence trait and its `SystemCatalog` impl.
//!
//! The trait separates the registry's allocation logic from the storage
//! layer so tests can substitute an in-memory impl.

use crate::control::security::catalog::SystemCatalog;

/// Pluggable persistence boundary for `DatabaseRegistry`. Production uses
/// the `SystemCatalog` impl over `_system.database_hwm`.
pub trait DatabaseHwmPersist: Send + Sync {
    /// Persist the hwm and the log index of the reservation that produced
    /// it, atomically.
    fn checkpoint_reserve(&self, hwm: u64, reserve_index: u64) -> crate::Result<()>;

    /// Load the persisted hwm, or `0` on a fresh catalog.
    fn load(&self) -> crate::Result<u64>;

    /// Load the applied-reservation cursor, or `0` on a fresh catalog.
    fn load_reserve_index(&self) -> crate::Result<u64>;
}

impl DatabaseHwmPersist for SystemCatalog {
    fn checkpoint_reserve(&self, hwm: u64, reserve_index: u64) -> crate::Result<()> {
        self.put_database_reserve_state(hwm, reserve_index)
    }

    fn load(&self) -> crate::Result<u64> {
        self.get_database_hwm()
    }

    fn load_reserve_index(&self) -> crate::Result<u64> {
        self.get_database_reserve_index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        assert_eq!(catalog.load().unwrap(), 0);
        catalog.checkpoint_reserve(1025, 9).unwrap();
        assert_eq!(catalog.load().unwrap(), 1025);
        assert_eq!(catalog.load_reserve_index().unwrap(), 9);
    }
}
