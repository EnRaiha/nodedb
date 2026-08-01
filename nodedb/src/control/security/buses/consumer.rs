// SPDX-License-Identifier: BUSL-1.1

//! Audit-log consumer for the session-invalidation and user-change buses.
//!
//! `spawn_bus_consumer` launches a single Tokio task that:
//!
//! 1. Selects across the two broadcast receivers.
//! 2. On `SessionInvalidated` with a hard-revoke reason: writes the
//!    `AuditEvent::SessionRevoked` row first (durable before close),
//!    then calls `session_registry.kill_sessions_for_user` to signal
//!    open sessions.
//! 3. On `SessionInvalidated` with a soft-revoke reason: writes the
//!    `AuditEvent::SessionRevoked` audit row.  Identity rehydrate is
//!    driven by the version check at request-entry time — no explicit
//!    session signal needed here.
//! 4. On `UserChanged`: writes an `AuditEvent::PrivilegeChange` row.
//! 5. On receiver lag (`RecvError::Lagged`): writes an
//!    `AuditEvent::AuditBusLagged` row so operators can detect dropped events.
//! 6. On `RecvError::Closed` (both senders dropped): exits cleanly.
//!
//! The returned `JoinHandle` is stored as `SharedState::bus_consumer_handle`.
//! Graceful shutdown drops the `SessionInvalidationBus` / `UserChangeBus`
//! senders, which eventually closes both receivers and causes the task to exit.

use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::control::security::audit::event::AuditEvent;
use crate::control::security::audit::log::AuditLog;
use crate::control::security::sessions::SessionRegistry;

use super::session_invalidation::SessionInvalidated;
use super::user_change::UserChanged;

/// Spawn the bus consumer task.
///
/// - `si_rx`: pre-subscribed receiver for the session-invalidation bus.
/// - `uc_rx`: pre-subscribed receiver for the user-change bus.
/// - `audit`: shared audit log.
/// - `session_registry`: active session registry; used for hard-revoke.
///
/// Returns a `JoinHandle` suitable for storing in `SharedState::bus_consumer_handle`,
/// or `None` when there is no Tokio runtime to spawn onto — a `SharedState` built
/// from a synchronous test or tooling context has no reactor, and constructing one
/// must not panic. Production startup always runs inside the Control Plane runtime.
pub fn spawn_bus_consumer(
    si_rx: broadcast::Receiver<SessionInvalidated>,
    uc_rx: broadcast::Receiver<UserChanged>,
    audit: Arc<Mutex<AuditLog>>,
    session_registry: Arc<SessionRegistry>,
) -> Option<tokio::task::JoinHandle<()>> {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!(
            "bus_consumer: no tokio runtime; skipping spawn (test or non-async context)"
        );
        return None;
    };
    Some(handle.spawn(run_bus_consumer(si_rx, uc_rx, audit, session_registry)))
}

async fn run_bus_consumer(
    mut si_rx: broadcast::Receiver<SessionInvalidated>,
    mut uc_rx: broadcast::Receiver<UserChanged>,
    audit: Arc<Mutex<AuditLog>>,
    session_registry: Arc<SessionRegistry>,
) {
    loop {
        tokio::select! {
            biased;

            result = si_rx.recv() => {
                match result {
                    Ok(event) => {
                        handle_session_invalidated(&audit, &session_registry, &event);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        record_lagged(
                            &audit,
                            &format!("session_invalidation_bus lagged; dropped={n}"),
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        drain_user_change_to_closed(&mut uc_rx, &audit).await;
                        return;
                    }
                }
            }

            result = uc_rx.recv() => {
                match result {
                    Ok(event) => {
                        handle_user_changed(&audit, &event);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        record_lagged(
                            &audit,
                            &format!("user_change_bus lagged; dropped={n}"),
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        drain_session_invalidation_to_closed(
                            &mut si_rx,
                            &audit,
                            &session_registry,
                        )
                        .await;
                        return;
                    }
                }
            }
        }
    }
}

async fn drain_session_invalidation_to_closed(
    si_rx: &mut broadcast::Receiver<SessionInvalidated>,
    audit: &Arc<Mutex<AuditLog>>,
    session_registry: &Arc<SessionRegistry>,
) {
    loop {
        match si_rx.recv().await {
            Ok(event) => handle_session_invalidated(audit, session_registry, &event),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                record_lagged(
                    audit,
                    &format!("session_invalidation_bus lagged; dropped={n}"),
                );
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

async fn drain_user_change_to_closed(
    uc_rx: &mut broadcast::Receiver<UserChanged>,
    audit: &Arc<Mutex<AuditLog>>,
) {
    loop {
        match uc_rx.recv().await {
            Ok(event) => handle_user_changed(audit, &event),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                record_lagged(audit, &format!("user_change_bus lagged; dropped={n}"));
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

fn handle_session_invalidated(
    audit: &Arc<Mutex<AuditLog>>,
    session_registry: &Arc<SessionRegistry>,
    event: &SessionInvalidated,
) {
    // Audit row written BEFORE closing any connection — durable even if
    // the close subsequently fails.
    let detail = format!("user_id={} reason={}", event.user_id, event.reason.as_str());
    if let Ok(mut log) = audit.lock() {
        log.record(
            AuditEvent::SessionRevoked,
            None,
            "session_invalidation_bus",
            &detail,
        );
    }

    // Hard revoke: signal every open session for this user to close.
    if event.reason.is_hard_revoke() {
        session_registry.kill_sessions_for_user(
            event.user_id,
            crate::control::security::sessions::KillReason::UserDropped,
        );
    }
    // Soft revoke: identity rehydrate is driven by the per-user version
    // counter at the next request-entry boundary — no explicit signal needed.
}

fn handle_user_changed(audit: &Arc<Mutex<AuditLog>>, event: &UserChanged) {
    let detail = format!("user_id={} credential_mutation", event.user_id);
    if let Ok(mut log) = audit.lock() {
        log.record(
            AuditEvent::PrivilegeChange,
            None,
            "user_change_bus",
            &detail,
        );
    }
}

fn record_lagged(audit: &Arc<Mutex<AuditLog>>, detail: &str) {
    if let Ok(mut log) = audit.lock() {
        log.record(AuditEvent::AuditBusLagged, None, "bus_consumer", detail);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::buses::session_invalidation::{
        SessionInvalidationBus, SessionInvalidationReason,
    };
    use crate::control::security::buses::user_change::UserChangeBus;
    use crate::control::security::sessions::registry::{SessionParams, SessionRegistry};

    fn make_audit() -> Arc<Mutex<AuditLog>> {
        Arc::new(Mutex::new(AuditLog::new(1_000)))
    }

    fn make_registry() -> Arc<SessionRegistry> {
        Arc::new(SessionRegistry::new())
    }

    #[tokio::test]
    async fn session_invalidated_produces_audit_row() {
        let audit = make_audit();
        let registry = make_registry();
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit), Arc::clone(&registry));

        si_bus.publish(SessionInvalidated {
            user_id: 99,
            reason: SessionInvalidationReason::UserDropped,
        });

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        let log = audit.lock().unwrap();
        let rows = log.query_by_event(&AuditEvent::SessionRevoked);
        assert_eq!(rows.len(), 1, "expected exactly one SessionRevoked row");
        assert!(rows[0].detail.contains("user_id=99"));
        assert!(rows[0].detail.contains("UserDropped"));
    }

    #[tokio::test]
    async fn session_invalidation_consumer_hard_revoke() {
        let audit = make_audit();
        let registry = Arc::new(SessionRegistry::new());
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        // Register a session for user 42.
        let params = SessionParams {
            user_id: 42,
            username: "alice".into(),
            db_user: "alice".into(),
            peer_addr: "127.0.0.1:5000".into(),
            protocol: "native".into(),
            auth_method: "password".into(),
            tenant_id: 1,
            credential_version: 0,
            current_database: None,
            token_expiry_ms: None,
        };
        let mut kill_rx = registry.register("session-42", &params).unwrap();

        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit), Arc::clone(&registry));

        si_bus.publish(SessionInvalidated {
            user_id: 42,
            reason: SessionInvalidationReason::UserDropped,
        });

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        // Audit row must be present.
        {
            let log = audit.lock().unwrap();
            let rows = log.query_by_event(&AuditEvent::SessionRevoked);
            assert_eq!(rows.len(), 1, "audit row must exist before session close");
        }

        // Kill signal must have fired.
        assert!(
            kill_rx.has_changed().unwrap_or(false),
            "kill_rx must be signalled for hard revoke"
        );
        assert_ne!(
            *kill_rx.borrow_and_update(),
            crate::control::security::sessions::KillReason::Alive,
            "kill value must not be Alive"
        );
    }

    #[tokio::test]
    async fn user_changed_produces_audit_row() {
        let audit = make_audit();
        let registry = make_registry();
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit), Arc::clone(&registry));

        uc_bus.publish(UserChanged { user_id: 55 });

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        let log = audit.lock().unwrap();
        let rows = log.query_by_event(&AuditEvent::PrivilegeChange);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].detail.contains("user_id=55"));
    }

    #[tokio::test]
    async fn both_buses_produce_rows() {
        let audit = make_audit();
        let registry = make_registry();
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit), Arc::clone(&registry));

        si_bus.publish(SessionInvalidated {
            user_id: 1,
            reason: SessionInvalidationReason::UserDeactivated,
        });
        uc_bus.publish(UserChanged { user_id: 2 });

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        let log = audit.lock().unwrap();
        assert_eq!(log.query_by_event(&AuditEvent::SessionRevoked).len(), 1);
        assert_eq!(log.query_by_event(&AuditEvent::PrivilegeChange).len(), 1);
    }
}
