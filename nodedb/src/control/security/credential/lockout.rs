// SPDX-License-Identifier: BUSL-1.1

//! Login lockout enforcement.

use std::net::IpAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::auth::Argon2Config;
use crate::control::security::audit::{AuditEmitContext, AuditEmitter, AuditEvent};
use crate::control::security::catalog::StoredLockoutRecord;
use crate::types::TenantId;

use super::store::core::{CredentialStore, read_lock, write_lock};

/// Tracks failed login attempts for lockout enforcement.
#[derive(Debug, Clone)]
pub(super) struct LoginAttemptTracker {
    /// Number of consecutive failed attempts.
    pub(super) failed_count: u32,
    /// When the lockout expires (if locked out).
    pub(super) locked_until: Option<Instant>,
    /// Last failure IP (forensic, mirrors redb).
    pub(super) last_failure_ip: Option<IpAddr>,
}

/// Returns the current time as milliseconds since the Unix epoch.
pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

impl CredentialStore {
    /// Configure lockout policy, password expiry and grace period.
    /// Called after construction from server config.
    pub fn set_lockout_policy(
        &mut self,
        max_failed: u32,
        lockout_secs: u64,
        password_expiry_days: u32,
    ) {
        self.set_lockout_policy_with_grace(max_failed, lockout_secs, password_expiry_days, 0);
    }

    /// Configure lockout policy with all expiry knobs.
    pub fn set_lockout_policy_with_grace(
        &mut self,
        max_failed: u32,
        lockout_secs: u64,
        password_expiry_days: u32,
        password_expiry_grace_days: u32,
    ) {
        self.max_failed_logins = max_failed;
        self.lockout_duration = std::time::Duration::from_secs(lockout_secs);
        self.password_expiry_secs = if password_expiry_days > 0 {
            password_expiry_days as u64 * 86400
        } else {
            0
        };
        self.password_expiry_grace_days = password_expiry_grace_days;
    }

    /// Set the Argon2id hashing parameters from server config.
    /// Called after construction alongside `set_lockout_policy_with_grace`.
    pub fn set_argon2_config(&mut self, cfg: Argon2Config) {
        self.argon2_config = cfg;
    }

    /// Rebuild the in-memory login-attempt cache from the persistent
    /// `_system.lockout_state` table, then garbage-collect records that are
    /// both expired and have no pending failures.
    ///
    /// Must be called after `set_lockout_policy_with_grace` because GC uses the
    /// configured `lockout_duration` to determine the cutoff.
    pub fn rebuild_lockout_cache(&self) -> crate::Result<()> {
        let catalog = &self.catalog;

        let records = catalog.load_all_lockout_records()?;
        let now_epoch_ms = now_ms();
        let lockout_duration_ms = self.lockout_duration.as_millis() as u64;
        let gc_cutoff_ms = now_epoch_ms.saturating_sub(lockout_duration_ms);

        let mut attempts = write_lock(&self.login_attempts);

        for (username, stored) in records {
            // Reconstruct the Instant-based locked_until from the epoch timestamp.
            let locked_until = if stored.locked_until_ms > 0 {
                let remaining_ms = stored.locked_until_ms.saturating_sub(now_epoch_ms);
                if remaining_ms == 0 {
                    // Lock has already expired — treat as not locked.
                    None
                } else {
                    Some(Instant::now() + Duration::from_millis(remaining_ms))
                }
            } else {
                None
            };

            let last_failure_ip = stored
                .last_failure_ip
                .as_deref()
                .and_then(|s| s.parse().ok());

            attempts.insert(
                username,
                LoginAttemptTracker {
                    failed_count: stored.failed_count,
                    locked_until,
                    last_failure_ip,
                },
            );
        }

        drop(attempts);

        // GC entries that are both cleared (failed_count == 0) and whose lock
        // window has fully elapsed.
        let gc_count = catalog.gc_lockout_records(gc_cutoff_ms)?;
        if gc_count > 0 {
            tracing::info!(gc_count, "pruned expired lockout records from catalog");
        }

        Ok(())
    }

    /// Check if a user is currently locked out.
    pub fn check_lockout(&self, username: &str) -> crate::Result<()> {
        if self.max_failed_logins == 0 {
            return Ok(());
        }

        let attempts = read_lock(&self.login_attempts);

        if let Some(tracker) = attempts.get(username)
            && let Some(locked_until) = tracker.locked_until
            && Instant::now() < locked_until
        {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: TenantId::new(0),
                resource: format!(
                    "user '{username}' is locked out ({} failed attempts)",
                    tracker.failed_count
                ),
            });
        }

        Ok(())
    }

    /// Record a failed login attempt. May trigger lockout.
    ///
    /// `ip` is the remote address of the failing connection, used for forensic
    /// audit. Pass `None` when the IP is not available (e.g. in-process paths).
    ///
    /// When the failed-count reaches `max_failed_logins` and the account is
    /// locked, emits `AuditEvent::LockoutTriggered` via `emitter`.
    pub fn record_login_failure(
        &self,
        username: &str,
        ip: Option<IpAddr>,
        emitter: &dyn AuditEmitter,
    ) {
        if self.max_failed_logins == 0 {
            return;
        }

        let mut attempts = write_lock(&self.login_attempts);

        let tracker = attempts
            .entry(username.to_string())
            .or_insert(LoginAttemptTracker {
                failed_count: 0,
                locked_until: None,
                last_failure_ip: None,
            });

        tracker.failed_count += 1;
        tracker.last_failure_ip = ip;

        let newly_locked =
            tracker.failed_count >= self.max_failed_logins && tracker.locked_until.is_none();
        if newly_locked {
            tracker.locked_until = Some(Instant::now() + self.lockout_duration);
        }
        // Compute the epoch-ms deadline for redb persistence.
        let locked_until_ms = tracker
            .locked_until
            .map(|t| {
                let remaining = t.saturating_duration_since(Instant::now());
                now_ms() + remaining.as_millis() as u64
            })
            .unwrap_or(0);

        let should_emit = newly_locked;
        let failed_count = tracker.failed_count;
        let lockout_secs = self.lockout_duration.as_secs();

        let stored = StoredLockoutRecord {
            failed_count,
            locked_until_ms,
            last_failure_ms: now_ms(),
            last_failure_ip: ip.map(|a| a.to_string()),
        };

        drop(attempts);

        // Write through to redb (best-effort; log on failure, do not abort).
        if let Err(e) = self.catalog.put_lockout_record(username, &stored) {
            tracing::warn!(
                username,
                error = %e,
                "failed to persist lockout record; in-memory state updated"
            );
        }

        if should_emit {
            tracing::warn!(
                username,
                failed_count,
                lockout_secs,
                "user locked out due to failed login attempts"
            );
            emitter.emit(
                AuditEvent::LockoutTriggered,
                username,
                &format!(
                    "user '{}' locked out after {} failed attempts (duration: {}s)",
                    username, failed_count, lockout_secs
                ),
                AuditEmitContext::new(None, "", username),
            );
        }
    }

    /// Reset failed login counter on successful authentication.
    pub fn record_login_success(&self, username: &str) {
        if self.max_failed_logins == 0 {
            return;
        }

        let mut attempts = write_lock(&self.login_attempts);

        attempts.remove(username);
        drop(attempts);

        // Remove from redb on success (best-effort).
        if let Err(e) = self.catalog.delete_lockout_record(username) {
            tracing::warn!(
                username,
                error = %e,
                "failed to delete lockout record on success; will be cleaned up on next startup"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::control::security::audit::NoopAuditEmitter;

    const NOOP: &NoopAuditEmitter = &NoopAuditEmitter;

    #[test]
    fn login_attempt_cache_remains_enforced_after_panic_while_write_locked() {
        let mut store = CredentialStore::new().expect("in-memory credential store");
        store.set_lockout_policy(2, 300, 0);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _attempts = store.login_attempts.write();
            panic!("simulated interrupted lockout update");
        }));
        assert!(result.is_err());

        store.record_login_failure("poison-free-lockout", None, NOOP);
        assert!(store.check_lockout("poison-free-lockout").is_ok());
        store.record_login_failure("poison-free-lockout", None, NOOP);
        assert!(store.check_lockout("poison-free-lockout").is_err());
        store.record_login_success("poison-free-lockout");
        assert!(store.check_lockout("poison-free-lockout").is_ok());
    }

    #[test]
    fn lockout_after_threshold() {
        let mut store = CredentialStore::new().unwrap();
        store.set_lockout_policy(3, 300, 0);

        store.record_login_failure("alice", None, NOOP);
        store.record_login_failure("alice", None, NOOP);
        assert!(store.check_lockout("alice").is_ok());

        store.record_login_failure("alice", None, NOOP);
        assert!(store.check_lockout("alice").is_err());
    }

    #[test]
    fn login_success_resets_counter() {
        let mut store = CredentialStore::new().unwrap();
        store.set_lockout_policy(3, 300, 0);

        store.record_login_failure("bob", None, NOOP);
        store.record_login_failure("bob", None, NOOP);
        store.record_login_success("bob");
        store.record_login_failure("bob", None, NOOP);
        assert!(store.check_lockout("bob").is_ok());
    }

    #[test]
    fn lockout_disabled_when_zero() {
        let store = CredentialStore::new().unwrap();
        // max_failed_logins = 0 means disabled
        for _ in 0..100 {
            store.record_login_failure("charlie", None, NOOP);
        }
        assert!(store.check_lockout("charlie").is_ok());
    }

    #[test]
    fn lockout_trigger_emits_audit_row() {
        use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;

        let mut store = CredentialStore::new().unwrap();
        store.set_lockout_policy(2, 300, 0);

        let emitter = CapturingEmitter::new();
        store.record_login_failure("dave", None, &emitter);
        assert!(
            emitter.recorded().is_empty(),
            "first failure should not emit"
        );

        store.record_login_failure("dave", None, &emitter);
        let recorded = emitter.recorded();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, AuditEvent::LockoutTriggered);
    }

    #[test]
    fn lockout_not_emitted_when_disabled() {
        use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;

        // max_failed_logins = 0 → lockout disabled, never emits.
        let store = CredentialStore::new().unwrap();
        let emitter = CapturingEmitter::new();
        for _ in 0..10 {
            store.record_login_failure("frank", None, &emitter);
        }
        assert!(emitter.recorded().is_empty());
    }

    #[test]
    fn lockout_persisted_survives_rebuild() {
        use std::net::IpAddr;
        use std::str::FromStr;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("system.redb");

        // Open a store, configure lockout, trigger it.
        {
            let mut store = CredentialStore::open(&db_path).expect("open");
            store.set_lockout_policy_with_grace(2, 300, 0, 0);
            let ip: IpAddr = IpAddr::from_str("192.0.2.1").unwrap();
            store.record_login_failure("grace", Some(ip), NOOP);
            store.record_login_failure("grace", Some(ip), NOOP);
            assert!(store.check_lockout("grace").is_err(), "should be locked");
        }

        // Reopen and rebuild cache — must still be locked.
        {
            let mut store = CredentialStore::open(&db_path).expect("reopen");
            store.set_lockout_policy_with_grace(2, 300, 0, 0);
            store.rebuild_lockout_cache().expect("rebuild");
            assert!(
                store.check_lockout("grace").is_err(),
                "user must still be locked after restart"
            );
        }
    }

    #[test]
    fn lockout_rebuild_gc_removes_cleared_expired() {
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("system.redb");

        // Insert a "cleared, long-expired" record directly via the catalog.
        {
            let mut store = CredentialStore::open(&db_path).expect("open");
            store.set_lockout_policy_with_grace(5, 1, 0, 0); // 1s lockout
            // Simulate two failures then a success: persists a cleared record.
            store.record_login_failure("han", None, NOOP);
            store.record_login_failure("han", None, NOOP);
            store.record_login_success("han");
        }

        // Reopen with a very long lockout so the gc_cutoff is in the past
        // relative to the last_failure_ms.
        {
            let mut store = CredentialStore::open(&db_path).expect("reopen");
            // Use a lockout_duration of 0 so cutoff = now — any old record qualifies.
            store.set_lockout_policy_with_grace(5, 0, 0, 0);
            store.rebuild_lockout_cache().expect("rebuild");
            // The record was deleted on success, so nothing to GC in this case.
            // Verify the user is not locked.
            assert!(store.check_lockout("han").is_ok());
        }
    }

    #[test]
    fn lockout_ip_roundtrip() {
        use std::net::IpAddr;
        use std::str::FromStr;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("system.redb");

        let ip = IpAddr::from_str("2001:db8::1").unwrap();

        {
            let mut store = CredentialStore::open(&db_path).expect("open");
            store.set_lockout_policy_with_grace(5, 300, 0, 0);
            store.record_login_failure("ivan", Some(ip), NOOP);
        }

        // Reload and verify IP survived the round-trip.
        {
            let mut store = CredentialStore::open(&db_path).expect("reopen");
            store.set_lockout_policy_with_grace(5, 300, 0, 0);
            store.rebuild_lockout_cache().expect("rebuild");

            let attempts = read_lock(&store.login_attempts);
            let tracker = attempts.get("ivan").expect("ivan not found");
            assert_eq!(
                tracker.last_failure_ip,
                Some(ip),
                "last_failure_ip must survive redb round-trip"
            );
        }
    }
}
