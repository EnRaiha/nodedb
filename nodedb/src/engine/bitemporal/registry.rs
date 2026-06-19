// SPDX-License-Identifier: BUSL-1.1

//! Per-collection bitemporal audit-retention registry.
//!
//! Thread-safe `RwLock`-backed registry of
//! `(tenant_id, collection) -> (engine_kind, BitemporalRetention)` entries.
//! DDL populates this on `CREATE COLLECTION ... WITH BITEMPORAL RETENTION`;
//! the enforcement loop reads it to decide which `MetaOp::TemporalPurge*`
//! to dispatch and what cutoff to apply.
//!
//! Enforcement of `audit_retain_ms < minimum_audit_retain_ms` happens at
//! [`BitemporalRetentionRegistry::register`] time — the registry is the
//! choke point between DDL and the scheduler, so rejecting below-floor
//! configs here means neither the Control Plane planner nor the Data
//! Plane ever sees a policy that violates the compliance floor.

use std::collections::HashMap;
use std::sync::RwLock;

use nodedb_types::config::BitemporalRetention;
use nodedb_wal::TemporalPurgeEngine;

use crate::types::{DatabaseId, TenantId};

/// Which bitemporal-capable engine backs a collection. Determines which
/// `MetaOp::TemporalPurge*` variant the scheduler dispatches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitemporalEngineKind {
    /// Graph edge store (versioned edge rows in redb).
    EdgeStore,
    /// Strict-document engine (versioned `documents_versioned` /
    /// `indexes_versioned` tables).
    DocumentStrict,
    /// Columnar engine, either plain (row-level purge via delete bitmaps)
    /// or timeseries (partition-level purge via `max_system_ts`). The
    /// dispatcher picks the right sub-path at runtime based on whether
    /// the collection is in the timeseries registry.
    Columnar,
    /// CRDT (Loro-backed) engine. Purge drops archived row versions
    /// from the per-collection bitemporal history sibling map while
    /// preserving the live row.
    Crdt,
    /// Array engine — global by `array_id`. Registered with
    /// `tenant_id = TenantId::new(0)` because the array catalog is
    /// currently global; per-tenant array isolation is a separate
    /// initiative.
    Array,
}

impl BitemporalEngineKind {
    /// Wire tag for the `RecordType::TemporalPurge` audit record payload.
    pub fn wire_tag(self) -> TemporalPurgeEngine {
        match self {
            BitemporalEngineKind::EdgeStore => TemporalPurgeEngine::EdgeStore,
            BitemporalEngineKind::DocumentStrict => TemporalPurgeEngine::DocumentStrict,
            BitemporalEngineKind::Columnar => TemporalPurgeEngine::Columnar,
            BitemporalEngineKind::Crdt => TemporalPurgeEngine::Crdt,
            BitemporalEngineKind::Array => TemporalPurgeEngine::Array,
        }
    }
}

/// Errors returned by [`BitemporalRetentionRegistry::register`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("bitemporal retention validation: {0}")]
    Invalid(#[from] nodedb_types::config::RetentionValidationError),
}

#[derive(Debug, Clone)]
pub struct Entry {
    /// Owning database. Scopes the enforcement-loop's temporal-purge
    /// dispatch so a collection's audit-retention purge runs against the
    /// right database's storage.
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: String,
    pub engine: BitemporalEngineKind,
    pub retention: BitemporalRetention,
}

pub struct BitemporalRetentionRegistry {
    inner: RwLock<HashMap<(DatabaseId, TenantId, String), Entry>>,
}

impl BitemporalRetentionRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Register (or replace) a collection's bitemporal retention policy.
    ///
    /// Validates `audit_retain_ms >= minimum_audit_retain_ms` before
    /// accepting; a DDL that violates the compliance floor is rejected
    /// here with a typed error rather than silently accepted.
    pub fn register(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: impl Into<String>,
        engine: BitemporalEngineKind,
        retention: BitemporalRetention,
    ) -> Result<(), RegisterError> {
        retention.validate()?;
        let collection = collection.into();
        let key = (database_id, tenant_id, collection.clone());
        let entry = Entry {
            database_id,
            tenant_id,
            collection,
            engine,
            retention,
        };
        let mut w = self.inner.write().unwrap_or_else(|p| p.into_inner());
        w.insert(key, entry);
        Ok(())
    }

    /// Remove a collection's policy. Idempotent.
    pub fn unregister(&self, database_id: DatabaseId, tenant_id: TenantId, collection: &str) {
        let mut w = self.inner.write().unwrap_or_else(|p| p.into_inner());
        w.remove(&(database_id, tenant_id, collection.to_string()));
    }

    /// Snapshot all registered entries. Used by the enforcement loop to
    /// iterate without holding the lock across dispatches.
    pub fn snapshot(&self) -> Vec<Entry> {
        let r = self.inner.read().unwrap_or_else(|p| p.into_inner());
        r.values().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        let r = self.inner.read().unwrap_or_else(|p| p.into_inner());
        r.is_empty()
    }

    pub fn len(&self) -> usize {
        let r = self.inner.read().unwrap_or_else(|p| p.into_inner());
        r.len()
    }
}

impl Default for BitemporalRetentionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ret(audit_ms: u64, floor_ms: u64) -> BitemporalRetention {
        BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: audit_ms,
            minimum_audit_retain_ms: floor_ms,
        }
    }

    #[test]
    fn register_accepts_above_floor() {
        let r = BitemporalRetentionRegistry::new();
        r.register(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "users",
            BitemporalEngineKind::DocumentStrict,
            ret(120_000, 60_000),
        )
        .unwrap();
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn register_rejects_below_floor() {
        let r = BitemporalRetentionRegistry::new();
        let err = r
            .register(
                DatabaseId::DEFAULT,
                TenantId::new(1),
                "users",
                BitemporalEngineKind::DocumentStrict,
                ret(60_000, 120_000),
            )
            .expect_err("must reject");
        matches!(err, RegisterError::Invalid(_));
        assert!(r.is_empty());
    }

    #[test]
    fn register_replaces_existing() {
        let r = BitemporalRetentionRegistry::new();
        r.register(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "c",
            BitemporalEngineKind::EdgeStore,
            ret(60_000, 0),
        )
        .unwrap();
        r.register(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "c",
            BitemporalEngineKind::EdgeStore,
            ret(120_000, 0),
        )
        .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.snapshot()[0].retention.audit_retain_ms, 120_000);
    }

    #[test]
    fn unregister_is_idempotent() {
        let r = BitemporalRetentionRegistry::new();
        r.register(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "c",
            BitemporalEngineKind::Columnar,
            ret(60_000, 0),
        )
        .unwrap();
        r.unregister(DatabaseId::DEFAULT, TenantId::new(1), "c");
        r.unregister(DatabaseId::DEFAULT, TenantId::new(1), "c");
        assert!(r.is_empty());
    }

    #[test]
    fn engine_wire_tag_matches() {
        assert_eq!(
            BitemporalEngineKind::EdgeStore.wire_tag(),
            TemporalPurgeEngine::EdgeStore
        );
        assert_eq!(
            BitemporalEngineKind::DocumentStrict.wire_tag(),
            TemporalPurgeEngine::DocumentStrict
        );
        assert_eq!(
            BitemporalEngineKind::Columnar.wire_tag(),
            TemporalPurgeEngine::Columnar
        );
        assert_eq!(
            BitemporalEngineKind::Crdt.wire_tag(),
            TemporalPurgeEngine::Crdt
        );
        assert_eq!(
            BitemporalEngineKind::Array.wire_tag(),
            TemporalPurgeEngine::Array
        );
    }

    #[test]
    fn register_accepts_array_kind() {
        let r = BitemporalRetentionRegistry::new();
        r.register(
            DatabaseId::DEFAULT,
            TenantId::new(0),
            "my_array",
            BitemporalEngineKind::Array,
            ret(86_400_000, 0),
        )
        .unwrap();
        assert_eq!(r.len(), 1);
        let snap = r.snapshot();
        assert_eq!(snap[0].engine, BitemporalEngineKind::Array);
        assert_eq!(snap[0].collection, "my_array");
    }
}
