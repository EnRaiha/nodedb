// SPDX-License-Identifier: BUSL-1.1

//! RESP per-connection session state.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::TenantId;

/// Per-connection state for a RESP session.
///
/// Tracks the selected KV collection and authenticated tenant.
/// Each TCP connection gets its own session.
pub struct RespSession {
    /// Currently selected KV collection (via SELECT command).
    /// Defaults to "default" — the implicit KV collection.
    pub collection: String,

    /// Tenant ID for this connection.
    /// Defaults to tenant 1 (single-tenant mode).
    /// In multi-tenant mode, set after AUTH.
    pub tenant_id: TenantId,

    /// Identity established by RESP AUTH. Data operations fail closed until set.
    pub identity: Option<AuthenticatedIdentity>,
}

impl Default for RespSession {
    fn default() -> Self {
        Self {
            collection: "default".into(),
            tenant_id: TenantId::new(1),
            identity: None,
        }
    }
}
