// SPDX-License-Identifier: BUSL-1.1

//! Three-level RAII connection permit.
//!
//! A [`ConnectionPermit`] holds:
//! 1. A global permit (from the cluster-wide `max_connections` semaphore).
//! 2. An optional per-database permit (from a per-`DatabaseId` semaphore).
//! 3. An optional per-tenant permit (from a per-`(DatabaseId, TenantId)` semaphore).
//!
//! All three levels are released atomically when the permit is dropped.
//! The permit is `Send` — it is held inside a Tokio task for the connection
//! lifetime and dropped when the task exits.
//!
//! A protocol that acquires its global slot and its scoped slots at different
//! points holds a [`ScopedConnectionPermit`] instead. Both types acquire the
//! scoped slots through [`ScopedConnectionPermit::acquire`], so every protocol
//! takes the database and tenant slots in the same order and reports the same
//! refusal.

use tokio::sync::OwnedSemaphorePermit;

use nodedb_types::{DatabaseId, TenantId};

use super::registry::{AdmissionError, AdmissionRegistry};

/// The database- and tenant-scoped half of a connection's admission.
///
/// pgwire takes its global slot at TCP accept, where the database and tenant
/// are still unknown, and takes these two slots after the startup handshake
/// binds a database. Dropping this releases both scoped slots.
#[must_use = "ScopedConnectionPermit must be kept alive for the connection's lifetime"]
pub struct ScopedConnectionPermit {
    /// Per-database connection slot. `None` if the database has no
    /// `max_connections` quota configured.
    pub database: Option<OwnedSemaphorePermit>,
    /// Per-tenant connection slot. `None` if the tenant has no
    /// `max_connections` quota configured.
    pub tenant: Option<OwnedSemaphorePermit>,
    /// The database this permit is scoped to (for metrics / tracing).
    pub db_id: DatabaseId,
    /// The tenant this permit is scoped to (for metrics / tracing).
    pub tenant_id: TenantId,
}

impl ScopedConnectionPermit {
    /// Take one connection's database and tenant slots.
    ///
    /// The database slot is taken first. A tenant refusal drops it before
    /// returning, so a connection that is turned away holds no slot at all.
    pub fn acquire(
        registry: &AdmissionRegistry,
        db_id: DatabaseId,
        tenant_id: TenantId,
    ) -> Result<Self, AdmissionError> {
        let database = registry.try_acquire_database(db_id)?;
        let tenant = match registry.try_acquire_tenant(db_id, tenant_id) {
            Ok(tenant) => tenant,
            Err(error) => {
                // Releases the database slot this call already took.
                drop(database);
                return Err(error);
            }
        };
        Ok(Self {
            database,
            tenant,
            db_id,
            tenant_id,
        })
    }
}

impl std::fmt::Debug for ScopedConnectionPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedConnectionPermit")
            .field("db_id", &self.db_id)
            .field("tenant_id", &self.tenant_id)
            .field("has_database_permit", &self.database.is_some())
            .field("has_tenant_permit", &self.tenant.is_some())
            .finish()
    }
}

/// A three-level RAII connection permit.
///
/// Holds the connection's slot at the global, database, and (optionally)
/// tenant level. Dropping this struct releases all three slots simultaneously.
#[must_use = "ConnectionPermit must be kept alive for the connection's lifetime"]
pub struct ConnectionPermit {
    /// Global connection slot, always held. Nothing reads it: its `Drop`
    /// releases the cluster-wide `max_connections` slot at connection close.
    /// Dropping the field instead releases that slot at end-of-auth.
    pub(crate) _global: OwnedSemaphorePermit,
    /// Per-database connection slot. `None` if the database has no
    /// `max_connections` quota configured.
    pub(crate) database: Option<OwnedSemaphorePermit>,
    /// Per-tenant connection slot. `None` if the tenant has no
    /// `max_connections` quota configured.
    pub(crate) tenant: Option<OwnedSemaphorePermit>,
    /// The database this permit is scoped to (for metrics / tracing).
    pub db_id: DatabaseId,
    /// The tenant this permit is scoped to (for metrics / tracing).
    pub tenant_id: TenantId,
}

impl ConnectionPermit {
    /// Combine an accept-time global slot with the connection's scoped slots.
    pub fn assemble(global: OwnedSemaphorePermit, scoped: ScopedConnectionPermit) -> Self {
        Self {
            _global: global,
            database: scoped.database,
            tenant: scoped.tenant,
            db_id: scoped.db_id,
            tenant_id: scoped.tenant_id,
        }
    }
}

impl std::fmt::Debug for ConnectionPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPermit")
            .field("db_id", &self.db_id)
            .field("tenant_id", &self.tenant_id)
            .field("global_permit_held", &true)
            .field("has_database_permit", &self.database.is_some())
            .field("has_tenant_permit", &self.tenant.is_some())
            .finish()
    }
}
