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
