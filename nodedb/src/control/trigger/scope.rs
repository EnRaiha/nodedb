// SPDX-License-Identifier: BUSL-1.1

//! Database and tenant scope used when firing triggers.

use crate::types::{DatabaseId, TenantId};

/// The database and tenant that scope a trigger lookup and execution.
#[derive(Clone, Copy, Debug)]
pub struct TriggerScope {
    /// Database that owns the target collection.
    pub database_id: DatabaseId,
    /// Tenant that owns the target collection.
    pub tenant_id: TenantId,
}

/// What every synchronous fire path shares: BEFORE, INSTEAD OF and SYNC
/// AFTER. Each body joins `txn`, the triggering statement's transaction, so
/// the statement and its bodies commit together or not at all.
#[derive(Clone, Copy)]
pub struct SyncFire<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a crate::control::state::SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a crate::control::security::identity::AuthenticatedIdentity,
    /// Database and tenant scope for trigger lookup and execution.
    pub scope: TriggerScope,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// The triggering statement's transaction.
    pub txn: &'a crate::control::server::shared::session::DmlTxnCtx<'a>,
}
