// SPDX-License-Identifier: BUSL-1.1

//! [`record_auth_violation`] — the one place an authentication or
//! authorization violation is recorded.
//!
//! Every rejection that used to write a bare
//! `audit_record(AuditEvent::AuthFailure, ...)` calls this instead, so the
//! audit entry and the escalation counter can never drift apart: one site
//! adds a violation, every site gets both effects.
//!
//! The verdict an escalation produces is an account-status change, and it is
//! persisted the same way any other auth-user status change is — onto the
//! `_system.auth_users` record, which [`check_blacklist`](crate::control::server::session_auth::guards::check_blacklist)
//! consults on every subsequent request — and replicated as a
//! [`CatalogEntry::PutAuthUser`]. A suspension that lived only in a
//! process-local map would evaporate on restart, which is a security control
//! that silently stops working.

use tracing::warn;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::auth_context::AuthStatus;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jit::auth_user::EscalationVerdict;
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Who a violation is attributed to.
pub enum ViolationSubject<'a> {
    /// A resolved identity. Internal-service identities are audited but never
    /// escalated — server-owned work (triggers, Raft apply, CRDT sync,
    /// scheduler, replay) must not be able to suspend itself, matching the
    /// exemption the blacklist, risk, and rate-limit guards already apply.
    Identity(&'a AuthenticatedIdentity),
    /// A pre-identity failure that names a principal — SCRAM, cleartext
    /// password, trust-mode, and certificate rejections all reach this before
    /// an `AuthenticatedIdentity` exists. The name is resolved against the
    /// credential store; a name that matches no user is audited only, so an
    /// attacker cannot mint tracker entries (or escalate anybody) with names
    /// nobody owns.
    Username(&'a str),
    /// Record the audit entry and nothing else. Used where no principal is
    /// attributable, and where the rejection *is* the previous verdict being
    /// enforced (an already-suspended account, a blacklisted org) — counting
    /// those would let a client in a retry loop drive the ladder from its own
    /// rejections.
    AuditOnly,
}

/// One authentication or authorization violation.
pub struct AuthViolation<'a> {
    /// Who the violation is attributed to.
    pub subject: ViolationSubject<'a>,
    /// Tenant, when the rejection happened after tenant resolution.
    pub tenant_id: Option<TenantId>,
    /// Audit `source` — the peer address for every transport that has one.
    pub source: &'a str,
    /// Audit `detail`.
    pub detail: &'a str,
}

/// Record the audit entry for `violation` and, when it is attributable to a
/// real account and the operator enabled `[auth.escalation]`, count it toward
/// auto-suspension / auto-ban.
pub fn record_auth_violation(state: &SharedState, violation: AuthViolation<'_>) {
    state.audit_record(
        AuditEvent::AuthFailure,
        violation.tenant_id,
        violation.source,
        violation.detail,
    );

    if !state.escalation.is_enabled() {
        return;
    }

    let Some(subject) = resolve(state, &violation.subject) else {
        return;
    };

    // An account already sitting on a verdict does not climb the ladder from
    // requests that verdict is refusing. The next rung is reached only after
    // an operator restores the account and it re-offends — which is what
    // "N suspensions, then a ban" means.
    if matches!(
        state.auth_users.get_status(&subject.user_id),
        Some(AuthStatus::Suspended | AuthStatus::Banned)
    ) {
        return;
    }

    let Some(escalation) = state.escalation.record_violation(&subject.user_id) else {
        return;
    };

    let verdict = EscalationVerdict {
        user_id: subject.user_id,
        username: subject.username,
        tenant_id: subject.tenant_id,
        status: escalation.status,
        suspensions: escalation.suspensions,
    };

    // Install locally first. This is deliberately the reverse of the DDL
    // ordering used by `PutApiKey` and friends, which propose first and write
    // directly only when there is no cluster: a security verdict must take
    // effect on the node that reached it even if the propose is buffered by
    // an open transactional-DDL block, refused because this node is not the
    // metadata leader, or times out.
    let stored = match state.auth_users.apply_escalation(verdict) {
        Ok(stored) => stored,
        Err(e) => {
            warn!(
                user_id = %subject.log_id,
                error = %e,
                "escalation verdict could not be persisted to the auth-user record"
            );
            return;
        }
    };

    let entry = CatalogEntry::PutAuthUser(Box::new(stored));
    if let Err(e) = crate::control::metadata_proposer::propose_catalog_entry(state, &entry) {
        warn!(
            user_id = %subject.log_id,
            error = %e,
            "escalation verdict persisted locally but could not be replicated"
        );
    }
}

/// The account an escalation applies to.
struct EscalationTarget {
    user_id: String,
    username: String,
    tenant_id: TenantId,
    /// Copy of `user_id` retained for logging after the fields are moved into
    /// the verdict.
    log_id: String,
}

/// Resolve a subject to an account, or `None` when the violation is not
/// attributable and must be audited only.
fn resolve(state: &SharedState, subject: &ViolationSubject<'_>) -> Option<EscalationTarget> {
    match subject {
        ViolationSubject::Identity(identity) => {
            if identity.is_internal_service() {
                return None;
            }
            let user_id = identity.user_id.to_string();
            Some(EscalationTarget {
                log_id: user_id.clone(),
                user_id,
                username: identity.username.clone(),
                tenant_id: identity.tenant_id,
            })
        }
        ViolationSubject::Username(username) => {
            let user = state.credentials.get_user(username)?;
            let user_id = user.user_id.to_string();
            Some(EscalationTarget {
                log_id: user_id.clone(),
                user_id,
                username: user.username,
                tenant_id: user.tenant_id,
            })
        }
        ViolationSubject::AuditOnly => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::escalation::{EscalationConfig, EscalationEngine};
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::wal::WalManager;

    /// Shared state whose escalation engine is built from `config`. Returns the
    /// backing `TempDir` guard too — it must outlive the state or the WAL file
    /// is removed out from under it.
    fn escalating_state(config: EscalationConfig) -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct shared state");
        let s = Arc::get_mut(&mut state).expect("state is not shared yet");
        s.escalation = EscalationEngine::new(config);
        (state, dir)
    }

    fn enabled(suspend: u32, ban: u32) -> EscalationConfig {
        EscalationConfig {
            enabled: true,
            suspend_after_violations: suspend,
            ban_after_suspensions: ban,
            violation_window_secs: 0,
            ..Default::default()
        }
    }

    fn regular(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "violator",
            crate::types::TenantId::new(1),
            AuthMethod::CleartextPassword,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT]),
        )
    }

    fn internal() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            99,
            "internal-service",
            crate::types::TenantId::new(1),
            vec![Role::ReadWrite],
            true,
            None,
            DatabaseSet::All,
        )
    }

    fn violate(state: &SharedState, identity: &AuthenticatedIdentity) {
        record_auth_violation(
            state,
            AuthViolation {
                subject: ViolationSubject::Identity(identity),
                tenant_id: Some(identity.tenant_id),
                source: "127.0.0.1:5432",
                detail: "test violation",
            },
        );
    }

    #[test]
    fn repeated_violations_suspend_the_account() {
        let (state, _dir) = escalating_state(enabled(3, 3));
        let identity = regular(7);

        violate(&state, &identity);
        violate(&state, &identity);
        assert_eq!(state.auth_users.get_status("7"), None);

        violate(&state, &identity);
        assert_eq!(
            state.auth_users.get_status("7"),
            Some(AuthStatus::Suspended),
            "the verdict must land on the auth-user record the admission gate reads"
        );
        assert!(!state.auth_users.is_active("7"));
    }

    #[test]
    fn repeated_suspensions_ban_the_account() {
        let (state, _dir) = escalating_state(enabled(2, 2));
        let identity = regular(8);

        violate(&state, &identity);
        violate(&state, &identity);
        assert_eq!(
            state.auth_users.get_status("8"),
            Some(AuthStatus::Suspended)
        );

        // An operator restores the account; it re-offends and hits the second
        // suspension, which is the ban threshold.
        state
            .auth_users
            .set_status("8", AuthStatus::Active)
            .expect("restore the account");
        violate(&state, &identity);
        violate(&state, &identity);
        assert_eq!(state.auth_users.get_status("8"), Some(AuthStatus::Banned));
    }

    #[test]
    fn a_standing_verdict_does_not_climb_the_ladder() {
        let (state, _dir) = escalating_state(enabled(1, 2));
        let identity = regular(9);

        violate(&state, &identity);
        assert_eq!(
            state.auth_users.get_status("9"),
            Some(AuthStatus::Suspended)
        );

        // Retries against the suspension must not reach the ban rung on their
        // own — only an operator restore followed by a fresh offence does.
        for _ in 0..50 {
            violate(&state, &identity);
        }
        assert_eq!(
            state.auth_users.get_status("9"),
            Some(AuthStatus::Suspended)
        );
    }

    #[test]
    fn internal_service_identities_are_never_escalated() {
        let (state, _dir) = escalating_state(enabled(1, 1));
        let identity = internal();

        for _ in 0..25 {
            violate(&state, &identity);
        }

        assert_eq!(state.auth_users.get_status("99"), None);
        assert_eq!(state.escalation.tracked_users(), 0);
    }

    #[test]
    fn unknown_usernames_are_audited_but_never_tracked() {
        let (state, _dir) = escalating_state(enabled(1, 1));

        for _ in 0..25 {
            record_auth_violation(
                &state,
                AuthViolation {
                    subject: ViolationSubject::Username("nobody"),
                    tenant_id: None,
                    source: "127.0.0.1:5432",
                    detail: "unknown user",
                },
            );
        }

        assert_eq!(
            state.escalation.tracked_users(),
            0,
            "a name matching no account must not mint tracker entries"
        );
    }

    #[test]
    fn disabled_engine_records_the_audit_entry_only() {
        let (state, _dir) = escalating_state(EscalationConfig::default());
        let identity = regular(11);

        for _ in 0..50 {
            violate(&state, &identity);
        }

        assert_eq!(state.auth_users.get_status("11"), None);
        assert_eq!(state.escalation.tracked_users(), 0);
    }
}
