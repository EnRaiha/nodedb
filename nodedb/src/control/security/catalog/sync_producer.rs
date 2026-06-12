// SPDX-License-Identifier: BUSL-1.1

//! Sync-producer registry catalog ops for the `_system.sync_producer_hwm` and
//! `_system.sync_producers` tables.
//!
//! Two tables live here:
//!
//! * `_system.sync_producer_hwm` — singleton `"global"` → `u64` high-watermark
//!   for the monotonic producer-id allocator.  Mirrors the layout of
//!   `_system.surrogate_hwm` (which uses `u32`).
//!
//! * `_system.sync_producers` — `lite_id (str)` → MessagePack-serialized
//!   `StoredProducerRegistration`.  One row per registered Lite client.
//!
//! All writes go through single-statement redb write transactions for
//! crash-safety, matching the pattern used by every other `_system.*` table.

use super::types::{SystemCatalog, catalog_err};

// ── Table definitions ─────────────────────────────────────────────────────────

/// Singleton high-watermark for the producer-id allocator.
///
/// Key: `"global"` (the only row).
/// Value: highest `producer_id` ever issued (0 = no allocations yet).
pub const SYNC_PRODUCER_HWM: redb::TableDefinition<&str, u64> =
    redb::TableDefinition::new("_system.sync_producer_hwm");

/// Per-Lite-client producer registration rows.
///
/// Key:   `lite_id` (opaque string from the Lite handshake; typically a
///         UUID or device fingerprint).
/// Value: MessagePack-serialized [`StoredProducerRegistration`].
pub const SYNC_PRODUCERS: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("_system.sync_producers");

/// Singleton row key used in `_system.sync_producer_hwm`.
const HWM_KEY: &str = "global";

// ── Catalog record ────────────────────────────────────────────────────────────

/// Persisted state for a single Lite client's sync producer.
///
/// Handshake wiring (Stage 4) and Raft replication of `register` / `fence`
/// (Stage 5) are explicit follow-ups; this record is the persistence layer
/// only.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq)]
#[msgpack(map, allow_unknown_fields)]
pub struct StoredProducerRegistration {
    /// Stable, monotonic, per-database u64 identity for this Lite client's
    /// write stream.  Allocated from `_system.sync_producer_hwm` and never
    /// reused.
    pub producer_id: u64,

    /// Fencing epoch, advanced by `fence()` calls.  Any token issued with a
    /// lower epoch is considered stale and must be rejected.  Starts at 0 on
    /// first registration.
    pub current_epoch: u64,

    /// Internal tenant that owns this registration.
    pub tenant_id: u64,

    /// Unix-millisecond timestamp when this registration was first created.
    pub created_ms: i64,
}

// ── Hwm catalog ops ────────────────────────────────────────────────────────────

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

// ── Producer registration catalog ops ────────────────────────────────────────

impl SystemCatalog {
    /// Persist a producer registration row, creating or overwriting it.
    ///
    /// Idempotent: re-inserting the same `lite_id` with the same record
    /// overwrites the existing row on disk (no-op at the application layer).
    pub fn put_producer_registration(
        &self,
        lite_id: &str,
        reg: &StoredProducerRegistration,
    ) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(reg)
            .map_err(|e| catalog_err("serialize producer_registration", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("sync_producers write txn", e))?;
        {
            let mut table = txn
                .open_table(SYNC_PRODUCERS)
                .map_err(|e| catalog_err("open sync_producers", e))?;
            table
                .insert(lite_id, bytes.as_slice())
                .map_err(|e| catalog_err("insert sync_producers", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("sync_producers commit", e))
    }

    /// Load the registration row for `lite_id`, or `None` if not found.
    pub fn get_producer_registration(
        &self,
        lite_id: &str,
    ) -> crate::Result<Option<StoredProducerRegistration>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("sync_producers read txn", e))?;
        let table = txn
            .open_table(SYNC_PRODUCERS)
            .map_err(|e| catalog_err("open sync_producers", e))?;
        match table
            .get(lite_id)
            .map_err(|e| catalog_err("get sync_producers", e))?
        {
            None => Ok(None),
            Some(v) => {
                let reg: StoredProducerRegistration = zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("deserialize producer_registration", e))?;
                Ok(Some(reg))
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    // ── hwm tests ──

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

    // ── registration tests ──

    fn reg(producer_id: u64, epoch: u64, tenant_id: u64) -> StoredProducerRegistration {
        StoredProducerRegistration {
            producer_id,
            current_epoch: epoch,
            tenant_id,
            created_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn put_then_get_roundtrip() {
        let (_dir, cat) = open();
        let r = reg(1, 0, 99);
        cat.put_producer_registration("device-abc", &r).unwrap();
        let got = cat
            .get_producer_registration("device-abc")
            .unwrap()
            .unwrap();
        assert_eq!(got, r);
    }

    #[test]
    fn missing_lite_id_returns_none() {
        let (_dir, cat) = open();
        assert!(cat.get_producer_registration("nobody").unwrap().is_none());
    }

    #[test]
    fn put_is_idempotent_overwrite() {
        let (_dir, cat) = open();
        cat.put_producer_registration("dev", &reg(1, 0, 1)).unwrap();
        cat.put_producer_registration("dev", &reg(1, 1, 1)).unwrap();
        let got = cat.get_producer_registration("dev").unwrap().unwrap();
        assert_eq!(got.current_epoch, 1);
    }

    #[test]
    fn registrations_persist_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.put_producer_registration("dev-1", &reg(10, 3, 5))
                .unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        let got = cat.get_producer_registration("dev-1").unwrap().unwrap();
        assert_eq!(got.producer_id, 10);
        assert_eq!(got.current_epoch, 3);
        assert_eq!(got.tenant_id, 5);
    }
}
