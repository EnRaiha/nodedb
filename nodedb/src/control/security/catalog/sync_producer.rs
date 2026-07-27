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

use redb::ReadableTable as _;
use sha2::{Digest as _, Sha256};

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

/// Per-user HMAC base keys used to sign externally synchronized CRDT deltas.
/// Keys are disclosed only to the authenticated user over the TLS sync
/// handshake and are scoped by tenant plus immutable internal user id.
pub const CRDT_SIGNING_KEYS: redb::TableDefinition<(u64, u64), &[u8]> =
    redb::TableDefinition::new("_system.crdt_signing_keys");

/// Non-secret fingerprint that binds the catalog to its WAL-wrapped signing
/// root and makes a missing/incorrect root fail closed on restart.
pub const CRDT_SIGNING_ROOT_METADATA: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("_system.crdt_signing_root_metadata");

/// Durable apply-side image of Raft-replicated join-token lifecycle state.
pub const JOIN_TOKEN_STATES: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("_system.join_token_states");

/// Active Raft-replicated enrollment exceptions, keyed by certificate SPKI.
pub const ENROLLMENT_PREAUTHORIZATIONS: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("_system.enrollment_preauthorizations");

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct StoredJoinTokenState {
    lifecycle: u8,
    node_addr: Option<String>,
    expires_at_ms: u64,
    attempt: u32,
    consumed_at_ms: u64,
    lease_id: Option<[u8; 16]>,
    lease_expires_at_ms: u64,
    recovery_bundle: Vec<u8>,
}

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

    /// Immutable internal user that owns this registration.
    pub user_id: u64,

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
    /// Install the at-rest-protected root used to derive per-user signing
    /// keys. Legacy raw key rows are deleted immediately so catalog bytes can
    /// never retain a plaintext signing secret after secure startup.
    pub fn configure_crdt_signing_root(&self, root: Option<[u8; 32]>) -> crate::Result<()> {
        if let Some(root) = root {
            let fingerprint: [u8; 32] = Sha256::digest(root).into();
            let txn = self
                .db
                .begin_write()
                .map_err(|e| catalog_err("crdt signing-root metadata write txn", e))?;
            {
                let mut table = txn
                    .open_table(CRDT_SIGNING_ROOT_METADATA)
                    .map_err(|e| catalog_err("open crdt signing-root metadata", e))?;
                if let Some(stored) = table
                    .get("fingerprint")
                    .map_err(|e| catalog_err("read crdt signing-root fingerprint", e))?
                    && stored.value() != fingerprint
                {
                    return Err(crate::Error::Config {
                        detail: "WAL-wrapped CRDT signing root does not match the durable catalog fingerprint".into(),
                    });
                }
                table
                    .insert("fingerprint", fingerprint.as_slice())
                    .map_err(|e| catalog_err("persist crdt signing-root fingerprint", e))?;
            }
            txn.commit()
                .map_err(|e| catalog_err("crdt signing-root metadata commit", e))?;
        }
        let legacy_keys = {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("crdt_signing_keys migration read txn", e))?;
            let table = txn
                .open_table(CRDT_SIGNING_KEYS)
                .map_err(|e| catalog_err("open crdt_signing_keys", e))?;
            let mut keys = Vec::new();
            for row in table
                .iter()
                .map_err(|e| catalog_err("iterate crdt_signing_keys", e))?
            {
                let (key, _) = row.map_err(|e| catalog_err("read crdt_signing_keys", e))?;
                keys.push(key.value());
            }
            keys
        };
        if !legacy_keys.is_empty() {
            let txn = self
                .db
                .begin_write()
                .map_err(|e| catalog_err("crdt_signing_keys migration write txn", e))?;
            {
                let mut table = txn
                    .open_table(CRDT_SIGNING_KEYS)
                    .map_err(|e| catalog_err("open crdt_signing_keys", e))?;
                for key in legacy_keys {
                    table
                        .remove(key)
                        .map_err(|e| catalog_err("remove legacy crdt_signing_key", e))?;
                }
            }
            txn.commit()
                .map_err(|e| catalog_err("crdt_signing_keys migration commit", e))?;
        }
        *self
            .crdt_signing_root
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = root;
        Ok(())
    }

    fn derive_crdt_signing_key(&self, tenant_id: u64, user_id: u64) -> crate::Result<[u8; 32]> {
        let root = self
            .crdt_signing_root
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .ok_or_else(|| crate::Error::Config {
                detail: "CRDT signing root unavailable; enable WAL encryption".into(),
            })?;
        let mut context = [0u8; 16];
        context[..8].copy_from_slice(&tenant_id.to_le_bytes());
        context[8..].copy_from_slice(&user_id.to_le_bytes());
        let hkdf = hkdf::Hkdf::<sha2::Sha256>::new(Some(b"nodedb-crdt-user-key-v1"), &root);
        let mut key = [0u8; 32];
        hkdf.expand(&context, &mut key)
            .map_err(|_| crate::Error::Internal {
                detail: "CRDT signing key derivation failed".into(),
            })?;
        Ok(key)
    }

    /// Derive a stable per-user key without persisting it in the catalog.
    pub fn get_or_create_crdt_signing_key(
        &self,
        tenant_id: u64,
        user_id: u64,
    ) -> crate::Result<[u8; 32]> {
        self.derive_crdt_signing_key(tenant_id, user_id)
    }

    /// Derive the existing per-user key when the secure root is configured.
    pub fn get_crdt_signing_key(
        &self,
        tenant_id: u64,
        user_id: u64,
    ) -> crate::Result<Option<[u8; 32]>> {
        self.derive_crdt_signing_key(tenant_id, user_id).map(Some)
    }

    /// Persist the apply-side image of one committed join-token transition.
    pub fn put_join_token_state(
        &self,
        state: &nodedb_cluster::JoinTokenState,
    ) -> crate::Result<()> {
        let (lifecycle, node_addr, consumed_at_ms, lease_id, lease_expires_at_ms, recovery_bundle) =
            match &state.lifecycle {
                nodedb_cluster::JoinTokenLifecycle::Issued => (0, None, 0, None, 0, Vec::new()),
                nodedb_cluster::JoinTokenLifecycle::InFlight {
                    node_addr,
                    lease_id,
                    lease_expires_at_ms,
                } => (
                    1,
                    Some(node_addr.to_string()),
                    0,
                    Some(*lease_id),
                    *lease_expires_at_ms,
                    Vec::new(),
                ),
                nodedb_cluster::JoinTokenLifecycle::Consumed {
                    node_addr,
                    lease_id,
                    ts_ms,
                    recovery_bundle,
                } => (
                    2,
                    Some(node_addr.to_string()),
                    *ts_ms,
                    Some(*lease_id),
                    0,
                    recovery_bundle.clone(),
                ),
                nodedb_cluster::JoinTokenLifecycle::Expired => (3, None, 0, None, 0, Vec::new()),
                nodedb_cluster::JoinTokenLifecycle::Aborted => (4, None, 0, None, 0, Vec::new()),
            };
        let bytes = zerompk::to_msgpack_vec(&StoredJoinTokenState {
            lifecycle,
            node_addr,
            expires_at_ms: state.expires_at_ms,
            attempt: state.attempt,
            consumed_at_ms,
            lease_id,
            lease_expires_at_ms,
            recovery_bundle,
        })
        .map_err(|e| catalog_err("serialize join token state", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("join_token_states write txn", e))?;
        {
            let mut table = txn
                .open_table(JOIN_TOKEN_STATES)
                .map_err(|e| catalog_err("open join_token_states", e))?;
            table
                .insert(state.token_hash.as_slice(), bytes.as_slice())
                .map_err(|e| catalog_err("insert join_token_states", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("join_token_states commit", e))
    }

    /// Load the durable token mirror before metadata-Raft replay begins.
    pub fn list_join_token_states(&self) -> crate::Result<Vec<nodedb_cluster::JoinTokenState>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("join_token_states read txn", e))?;
        let table = txn
            .open_table(JOIN_TOKEN_STATES)
            .map_err(|e| catalog_err("open join_token_states", e))?;
        let mut states = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iterate join_token_states", e))?
        {
            let (hash, value) = row.map_err(|e| catalog_err("read join_token_states", e))?;
            let token_hash: [u8; 32] =
                hash.value().try_into().map_err(|_| crate::Error::Storage {
                    engine: "catalog".into(),
                    detail: "invalid join-token hash length".into(),
                })?;
            let stored: StoredJoinTokenState = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize join token state", e))?;
            let node_addr = stored
                .node_addr
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(|e| crate::Error::Storage {
                    engine: "catalog".into(),
                    detail: format!("invalid join-token address: {e}"),
                })?;
            let lifecycle = match (stored.lifecycle, node_addr, stored.lease_id) {
                (0, _, _) => nodedb_cluster::JoinTokenLifecycle::Issued,
                (1, Some(node_addr), Some(lease_id)) => {
                    nodedb_cluster::JoinTokenLifecycle::InFlight {
                        node_addr,
                        lease_id,
                        lease_expires_at_ms: stored.lease_expires_at_ms,
                    }
                }
                (2, Some(node_addr), Some(lease_id)) => {
                    nodedb_cluster::JoinTokenLifecycle::Consumed {
                        node_addr,
                        lease_id,
                        ts_ms: stored.consumed_at_ms,
                        recovery_bundle: stored.recovery_bundle,
                    }
                }
                (3, _, _) => nodedb_cluster::JoinTokenLifecycle::Expired,
                (4, _, _) => nodedb_cluster::JoinTokenLifecycle::Aborted,
                _ => {
                    return Err(crate::Error::Storage {
                        engine: "catalog".into(),
                        detail: "invalid join-token lifecycle record".into(),
                    });
                }
            };
            states.push(nodedb_cluster::JoinTokenState {
                token_hash,
                lifecycle,
                expires_at_ms: stored.expires_at_ms,
                attempt: stored.attempt,
            });
        }
        Ok(states)
    }

    /// Persist an active enrollment exception before exposing it in memory.
    pub fn put_enrollment_preauthorization(
        &self,
        spki: &[u8; 32],
        expires_at_ms: u64,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("enrollment preauthorization write txn", e))?;
        {
            let mut table = txn
                .open_table(ENROLLMENT_PREAUTHORIZATIONS)
                .map_err(|e| catalog_err("open enrollment preauthorizations", e))?;
            table
                .insert(spki.as_slice(), expires_at_ms)
                .map_err(|e| catalog_err("insert enrollment preauthorization", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("enrollment preauthorization commit", e))
    }

    /// Remove a revoked enrollment exception durably.
    pub fn remove_enrollment_preauthorization(&self, spki: &[u8; 32]) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("enrollment preauthorization delete txn", e))?;
        {
            let mut table = txn
                .open_table(ENROLLMENT_PREAUTHORIZATIONS)
                .map_err(|e| catalog_err("open enrollment preauthorizations", e))?;
            table
                .remove(spki.as_slice())
                .map_err(|e| catalog_err("remove enrollment preauthorization", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("enrollment preauthorization delete commit", e))
    }

    /// Load nonexpired enrollment exceptions for transport rehydration.
    pub fn list_enrollment_preauthorizations(
        &self,
        now_ms: u64,
    ) -> crate::Result<Vec<([u8; 32], u64)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("enrollment preauthorization read txn", e))?;
        let table = txn
            .open_table(ENROLLMENT_PREAUTHORIZATIONS)
            .map_err(|e| catalog_err("open enrollment preauthorizations", e))?;
        let mut entries = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iterate enrollment preauthorizations", e))?
        {
            let (spki, expiry) =
                row.map_err(|e| catalog_err("read enrollment preauthorization", e))?;
            let expires_at_ms = expiry.value();
            if expires_at_ms > now_ms {
                let spki: [u8; 32] =
                    spki.value().try_into().map_err(|_| crate::Error::Storage {
                        engine: "catalog".into(),
                        detail: "invalid enrollment SPKI length".into(),
                    })?;
                entries.push((spki, expires_at_ms));
            }
        }
        Ok(entries)
    }

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
    use redb::ReadableTableMetadata as _;

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
            user_id: 7,
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
    fn crdt_signing_key_is_stable_and_tenant_user_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let root = [0x5a; 32];
        let first = {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.configure_crdt_signing_root(Some(root)).unwrap();
            let first = cat.get_or_create_crdt_signing_key(5, 9).unwrap();
            assert_eq!(cat.get_or_create_crdt_signing_key(5, 9).unwrap(), first);
            assert_ne!(cat.get_or_create_crdt_signing_key(5, 10).unwrap(), first);
            first
        };
        let cat = SystemCatalog::open(&path).unwrap();
        cat.configure_crdt_signing_root(Some(root)).unwrap();
        assert_eq!(cat.get_crdt_signing_key(5, 9).unwrap(), Some(first));
        let txn = cat.db.begin_read().unwrap();
        let table = txn.open_table(CRDT_SIGNING_KEYS).unwrap();
        assert_eq!(table.len().unwrap(), 0, "raw signing keys must not persist");
    }

    #[test]
    fn signing_root_fingerprint_rejects_silent_root_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .configure_crdt_signing_root(Some([0x11; 32]))
                .unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        let error = catalog
            .configure_crdt_signing_root(Some([0x22; 32]))
            .unwrap_err();
        assert!(matches!(error, crate::Error::Config { .. }));
    }

    #[test]
    fn consumed_join_token_state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let state = nodedb_cluster::JoinTokenState {
            token_hash: [7; 32],
            lifecycle: nodedb_cluster::JoinTokenLifecycle::Consumed {
                node_addr: "127.0.0.1:9000".parse().unwrap(),
                lease_id: [8; 16],
                ts_ms: 55,
                recovery_bundle: vec![9, 10],
            },
            expires_at_ms: 99,
            attempt: 1,
        };
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.put_join_token_state(&state).unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        let loaded = cat.list_join_token_states().unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(matches!(
            loaded[0].lifecycle,
            nodedb_cluster::JoinTokenLifecycle::Consumed { ts_ms: 55, .. }
        ));
    }

    #[test]
    fn enrollment_preauthorization_survives_reopen_and_revoke() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let spki = [0x71; 32];
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.put_enrollment_preauthorization(&spki, 50_000).unwrap();
        }
        {
            let cat = SystemCatalog::open(&path).unwrap();
            assert_eq!(
                cat.list_enrollment_preauthorizations(10_000).unwrap(),
                vec![(spki, 50_000)]
            );
            cat.remove_enrollment_preauthorization(&spki).unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        assert!(
            cat.list_enrollment_preauthorizations(10_000)
                .unwrap()
                .is_empty()
        );
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
