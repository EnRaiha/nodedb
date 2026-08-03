// SPDX-License-Identifier: BUSL-1.1

//! `_system.sync_producer_hwm` — singleton high-watermark for the monotonic
//! producer-id allocator. Mirrors the layout of `_system.surrogate_hwm` (which
//! uses `u32`).

use crate::control::security::catalog::types::{SystemCatalog, catalog_err};
use redb::ReadableDatabase;

/// Singleton high-watermark for the producer-id allocator.
///
/// Key: `"global"` (the only row).
/// Value: highest `producer_id` ever issued (0 = no allocations yet).
pub const SYNC_PRODUCER_HWM: redb::TableDefinition<&str, u64> =
    redb::TableDefinition::new("_system.sync_producer_hwm");

/// Singleton row key used in `_system.sync_producer_hwm`.
const HWM_KEY: &str = "global";

impl SystemCatalog {
    /// Persist the producer-id allocator high-watermark.  Overwrites the
    /// singleton row.
    pub fn put_producer_hwm(&self, hwm: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("producer_hwm write txn", e))?;
        {
            let mut table = txn
                .open_table(SYNC_PRODUCER_HWM)
                .map_err(|e| catalog_err("open sync_producer_hwm", e))?;
            table
                .insert(HWM_KEY, hwm)
                .map_err(|e| catalog_err("insert sync_producer_hwm", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("sync_producer_hwm commit", e))
    }

    /// Load the persisted producer-id hwm, or `0` if none recorded yet
    /// (fresh database).
    pub fn get_producer_hwm(&self) -> crate::Result<u64> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("producer_hwm read txn", e))?;
        let table = txn
            .open_table(SYNC_PRODUCER_HWM)
            .map_err(|e| catalog_err("open sync_producer_hwm", e))?;
        match table
            .get(HWM_KEY)
            .map_err(|e| catalog_err("get sync_producer_hwm", e))?
        {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    #[test]
    fn fresh_hwm_returns_zero() {
        let (_dir, cat) = open();
        assert_eq!(cat.get_producer_hwm().unwrap(), 0);
    }

    #[test]
    fn put_hwm_then_get_roundtrip() {
        let (_dir, cat) = open();
        cat.put_producer_hwm(42).unwrap();
        assert_eq!(cat.get_producer_hwm().unwrap(), 42);
        cat.put_producer_hwm(1_000_000_000).unwrap();
        assert_eq!(cat.get_producer_hwm().unwrap(), 1_000_000_000);
    }

    #[test]
    fn hwm_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.put_producer_hwm(7777).unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        assert_eq!(cat.get_producer_hwm().unwrap(), 7777);
    }
}
