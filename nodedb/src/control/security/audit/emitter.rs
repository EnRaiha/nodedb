// SPDX-License-Identifier: BUSL-1.1

//! `AuditEmitter` — thin trait for emitting audit events at enforcement points.
//!
//! Enforcement points (`permission::check`, `rls::eval`, `credential::lockout`)
//! accept `&dyn AuditEmitter` rather than a concrete `Arc<Mutex<AuditLog>>`.
//! This keeps the security logic decoupled from the audit infrastructure and
//! allows tests to inject a capturing mock without a real `AuditLog`.

use std::sync::{Arc, Mutex};

use nodedb_types::DatabaseId;

use crate::types::TenantId;

use super::auth::AuditAuth;
use super::event::AuditEvent;
use super::log::AuditLog;

/// Emit a single audit event at a security enforcement point.
///
/// Implementations must be `Send + Sync` — enforcement points live on the
/// Control Plane.  Emission is best-effort; implementations must not panic
/// or propagate errors (lock poisoning, capacity, etc.) to the caller.
pub trait AuditEmitter: Send + Sync {
    fn emit(&self, event: AuditEvent, source: &str, detail: &str, auth: AuditEmitContext<'_>);
}

/// Caller-supplied context for a single emission.
#[derive(Clone, Copy)]
pub struct AuditEmitContext<'a> {
    pub tenant_id: Option<TenantId>,
    /// Database context for database-scoped security enforcement events.
    pub database_id: Option<DatabaseId>,
    pub auth_user_id: &'a str,
    pub auth_user_name: &'a str,
}

impl<'a> AuditEmitContext<'a> {
    pub fn new(
        tenant_id: Option<TenantId>,
        auth_user_id: &'a str,
        auth_user_name: &'a str,
    ) -> Self {
        Self {
            tenant_id,
            database_id: None,
            auth_user_id,
            auth_user_name,
        }
    }
}

/// Production implementation: writes into an `Arc<Mutex<AuditLog>>`.
pub struct ArcAuditEmitter(pub Arc<Mutex<AuditLog>>);

impl AuditEmitter for ArcAuditEmitter {
    fn emit(&self, event: AuditEvent, source: &str, detail: &str, ctx: AuditEmitContext<'_>) {
        let auth = AuditAuth {
            user_id: ctx.auth_user_id.to_string(),
            user_name: ctx.auth_user_name.to_string(),
            session_id: String::new(),
        };
        // The sync wait runs with the audit lock released, so concurrent emitters share one fsync.
        super::log_commit::record_concurrent(
            &self.0,
            super::log::AuditRecordInput {
                event,
                tenant_id: ctx.tenant_id,
                database_id: ctx.database_id,
                source,
                detail,
                auth: &auth,
            },
        );
    }
}

/// No-op implementation: discards every emission silently.
///
/// Used by callers that are not the terminal denial point (e.g. intermediate
/// checks in a multi-layer fallback chain) so that only the final denial
/// produces an audit row.
pub struct NoopAuditEmitter;

impl AuditEmitter for NoopAuditEmitter {
    fn emit(&self, _event: AuditEvent, _source: &str, _detail: &str, _ctx: AuditEmitContext<'_>) {}
}

#[cfg(test)]
pub mod test_helpers {
    use std::sync::Mutex;

    use super::*;

    /// Capturing emitter for unit tests — stores every emission in a `Vec`.
    pub struct CapturingEmitter {
        pub events: Mutex<Vec<(AuditEvent, String, String)>>,
    }

    impl Default for CapturingEmitter {
        fn default() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
            }
        }
    }

    impl CapturingEmitter {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn recorded(&self) -> Vec<(AuditEvent, String, String)> {
            self.events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
        }
    }

    impl AuditEmitter for CapturingEmitter {
        fn emit(&self, event: AuditEvent, source: &str, detail: &str, _ctx: AuditEmitContext<'_>) {
            let mut events = self.events.lock().unwrap_or_else(|p| p.into_inner());
            events.push((event, source.to_string(), detail.to_string()));
        }
    }
}
