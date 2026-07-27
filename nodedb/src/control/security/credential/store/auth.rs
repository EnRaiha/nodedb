// SPDX-License-Identifier: BUSL-1.1

//! Authentication lookups: password verification, SCRAM credential
//! exports, identity-building.

use super::super::super::identity::{AuthMethod, AuthenticatedIdentity};
use super::super::super::time::now_secs;
use super::super::hash::{VerifyOutcome, hash_password_argon2, verify_argon2_with_rehash};
use super::super::record::UserRecord;
use super::core::{CredentialStore, read_lock, write_lock};

/// Result of a `get_scram_credentials` call, carrying an optional warning
/// string when the login is allowed but the password has entered the grace period.
pub struct ScramCredentials {
    pub salt: Vec<u8>,
    pub salted_password: Vec<u8>,
    /// Non-empty when the account is in expiry grace period or `must_change_password`
    /// is set (login allowed, but the client should be told to change their password).
    pub warning: Option<String>,
}

/// Why an authentication attempt failed to yield a usable credential.
///
/// Distinguishes a genuine credential failure — which is a brute-force
/// signal and must count toward account lockout — from a non-credential
/// rejection, which must not. Collapsing these (the historical behaviour,
/// where every failure path returned a bare `false`/`None`) lets routine
/// policy rejections lock out an account whose password is correct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthRejection {
    /// Wrong password, or an unknown user. A genuine credential failure;
    /// counts toward the lockout counter.
    BadCredential,
    /// The credential is not at fault — a policy denies the login
    /// (password expired, change required, account inactive, service
    /// account). Must NOT count toward the lockout counter.
    PolicyDenied,
    /// Verification could not be performed because stored credential data is
    /// invalid. Must NOT count toward the lockout counter.
    Internal,
}

/// Outcome of a cleartext-password verification.
pub enum PasswordVerification {
    /// Password verified. Carries an optional warning (grace period or
    /// `must_change_password` with a grace window remaining).
    Verified(Option<String>),
    /// Login denied; the reason classifies lockout treatment.
    Rejected(AuthRejection),
}

/// Outcome of a SCRAM credential lookup.
pub enum ScramLookup {
    /// Credentials available for the SCRAM handshake.
    Found(ScramCredentials),
    /// Lookup denied; the reason classifies lockout treatment.
    Rejected(AuthRejection),
}

impl CredentialStore {
    /// Look up a user by username. Returns None if not found or
    /// inactive.
    pub fn get_user(&self, username: &str) -> Option<UserRecord> {
        let users = read_lock(&self.users);
        users.get(username).filter(|u| u.is_active).cloned()
    }

    /// Get the SCRAM salt and salted password for pgwire SCRAM auth.
    ///
    /// Returns [`ScramLookup::Found`] (with a non-empty warning when in the
    /// grace period or `must_change_password` is set). Returns
    /// [`ScramLookup::Rejected`] otherwise — the [`AuthRejection`] reason
    /// classifies whether the rejection counts toward account lockout:
    /// only an unknown user (`BadCredential`) does; service accounts,
    /// inactive accounts and expired/must-change passwords (`PolicyDenied`)
    /// do not.
    pub fn get_scram_credentials(&self, username: &str) -> ScramLookup {
        let users = read_lock(&self.users);
        let u = match users.get(username) {
            Some(u) => u,
            // Unknown user — a genuine credential failure.
            None => return ScramLookup::Rejected(AuthRejection::BadCredential),
        };
        // An inactive account or a service account cannot use password
        // auth at all; neither is a credential failure.
        if !u.is_active || u.is_service_account {
            return ScramLookup::Rejected(AuthRejection::PolicyDenied);
        }

        let now = now_secs();
        let grace_secs = self.password_expiry_grace_days as u64 * 86400;

        // Expired with no grace: policy rejection, not a credential failure.
        if u.password_expires_at > 0
            && now >= u.password_expires_at
            && (grace_secs == 0 || now >= u.password_expires_at + grace_secs)
        {
            tracing::warn!(username = u.username, "password expired, login denied");
            return ScramLookup::Rejected(AuthRejection::PolicyDenied);
        }

        // must_change_password with no grace: policy rejection.
        if u.must_change_password && grace_secs == 0 {
            tracing::warn!(
                username = u.username,
                "must_change_password set with no grace period, login denied"
            );
            return ScramLookup::Rejected(AuthRejection::PolicyDenied);
        }

        // Compute warning if in grace period or must_change_password is set.
        let warning = if u.must_change_password {
            Some("password change required: please change your password".to_string())
        } else if u.password_expires_at > 0
            && now >= u.password_expires_at
            && grace_secs > 0
            && now < u.password_expires_at + grace_secs
        {
            let days_left = (u.password_expires_at + grace_secs).saturating_sub(now) / 86400 + 1;
            Some(format!(
                "password expired: grace period ends in {days_left} day(s), please change your password"
            ))
        } else {
            None
        };

        ScramLookup::Found(ScramCredentials {
            salt: u.scram_salt.clone(),
            salted_password: u.scram_salted_password.clone(),
            warning,
        })
    }

    /// Verify a cleartext password against the stored Argon2 hash.
    ///
    /// Also enforces `password_expires_at` and `must_change_password`
    /// (same policy as `get_scram_credentials`) so that all auth paths
    /// honour the expiry policy.
    ///
    /// On a successful match, transparently rehashes the stored password if
    /// the stored Argon2 parameters are strictly weaker than the configured
    /// ones.  Write-back failure is non-fatal (logged as a warning).  If the
    /// stored PHC string is unparseable the login is denied.
    ///
    /// Returns [`PasswordVerification::Verified`] (with an optional warning
    /// when in the grace period or `must_change_password` is set) or
    /// [`PasswordVerification::Rejected`]. The rejection carries an
    /// [`AuthRejection`] reason: a wrong password or unknown user is a
    /// `BadCredential` and counts toward lockout; an expired / must-change
    /// password or inactive account is a `PolicyDenied` and must not.
    ///
    /// The wrong-password check is evaluated *before* the expiry / change
    /// policy so that a wrong password on an otherwise policy-blocked
    /// account is still classified as a credential failure.
    pub fn verify_password_with_status(
        &self,
        username: &str,
        password: &str,
    ) -> PasswordVerification {
        let users = read_lock(&self.users);
        let record = match users.get(username) {
            Some(r) => r,
            None => {
                // Timing oracle mitigation: run a dummy hash for unknown users.
                let _ = hash_password_argon2(password, &self.argon2_config);
                // An unknown user is a genuine credential failure.
                return PasswordVerification::Rejected(AuthRejection::BadCredential);
            }
        };
        if !record.is_active {
            // Disabled account: the supplied credential is not the issue.
            let _ = hash_password_argon2(password, &self.argon2_config);
            return PasswordVerification::Rejected(AuthRejection::PolicyDenied);
        }

        // Constant-time verify + rehash decision; runs before the policy
        // checks so the timing profile is the same for expired and valid
        // accounts.
        let stored_hash = record.password_hash.clone();
        let outcome = verify_argon2_with_rehash(&stored_hash, password, &self.argon2_config);

        // Classify the verification outcome first. A wrong password is a
        // credential failure regardless of any policy state on the account.
        let rehash_hash = match outcome {
            VerifyOutcome::Ok { rehash } => rehash,
            VerifyOutcome::WrongPassword => {
                return PasswordVerification::Rejected(AuthRejection::BadCredential);
            }
            // Unparseable stored PHC is a data integrity error, not a
            // credential failure — deny login without counting it.
            VerifyOutcome::BadStoredHash => {
                tracing::error!(
                    username,
                    "stored password hash is not a valid PHC string; login denied"
                );
                return PasswordVerification::Rejected(AuthRejection::Internal);
            }
        };

        // The password is correct. The policy checks below are
        // non-credential rejections — they must not count toward lockout.
        let now = now_secs();
        let grace_secs = self.password_expiry_grace_days as u64 * 86400;

        // Expired past grace: deny despite the correct password.
        if record.password_expires_at > 0
            && now >= record.password_expires_at
            && (grace_secs == 0 || now >= record.password_expires_at + grace_secs)
        {
            tracing::warn!(username, "password expired, login denied");
            return PasswordVerification::Rejected(AuthRejection::PolicyDenied);
        }

        // must_change_password with no grace: deny despite the correct password.
        if record.must_change_password && grace_secs == 0 {
            tracing::warn!(username, "must_change_password set, login denied");
            return PasswordVerification::Rejected(AuthRejection::PolicyDenied);
        }

        // Drop read lock before acquiring write lock for rehash write-back.
        drop(users);

        // Perform write-back if a rehash was computed.
        if let Some(new_hash) = rehash_hash {
            self.apply_rehash(username, new_hash);
        }

        // Re-acquire read lock to compute the warning (record reference was dropped).
        let warning = self.compute_login_warning(username, now, grace_secs);

        PasswordVerification::Verified(warning)
    }

    /// Write the new password hash back to the in-memory store and catalog.
    ///
    /// Failure is non-fatal: a warning is logged and login continues.
    fn apply_rehash(&self, username: &str, new_hash: String) {
        let mut users = write_lock(&self.users);
        let record = match users.get_mut(username) {
            Some(r) => r,
            None => {
                tracing::warn!(
                    username,
                    "rehash write-back: user vanished between read and write; skipping"
                );
                return;
            }
        };
        record.password_hash = new_hash;
        if let Err(e) = self.persist_user(record) {
            tracing::warn!(
                username,
                error = %e,
                "rehash write-back: catalog persist failed; in-memory hash updated, \
                 catalog will be reconciled on next password change"
            );
        } else {
            tracing::debug!(username, "password hash upgraded to current Argon2 params");
        }
    }

    /// Compute the login warning string (grace period / must_change_password).
    /// Re-reads the record under the poison-free credential cache lock.
    fn compute_login_warning(&self, username: &str, now: u64, grace_secs: u64) -> Option<String> {
        let users = read_lock(&self.users);
        let record = users.get(username)?;

        if record.must_change_password {
            Some("password change required: please change your password".to_string())
        } else if record.password_expires_at > 0
            && now >= record.password_expires_at
            && grace_secs > 0
            && now < record.password_expires_at + grace_secs
        {
            let days_left =
                (record.password_expires_at + grace_secs).saturating_sub(now) / 86400 + 1;
            Some(format!(
                "password expired: grace period ends in {days_left} day(s), please change your password"
            ))
        } else {
            None
        }
    }

    /// Verify a cleartext password. Convenience wrapper that collapses the
    /// verdict to a boolean; ignores the warning and the rejection reason.
    /// Auth paths that drive the lockout counter must call
    /// `verify_password_with_status` and branch on the [`AuthRejection`].
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        matches!(
            self.verify_password_with_status(username, password),
            PasswordVerification::Verified(_)
        )
    }

    /// Build an `AuthenticatedIdentity` for a verified user.
    pub fn to_identity(&self, username: &str, method: AuthMethod) -> Option<AuthenticatedIdentity> {
        self.get_user(username).map(|record| {
            let is_su = record.is_superuser;
            AuthenticatedIdentity::from_catalog_principal(
                crate::control::security::identity::CatalogPrincipal {
                    user_id: record.user_id,
                    username: record.username,
                    tenant_id: record.tenant_id,
                    auth_method: method,
                    roles: record.roles,
                    is_superuser: is_su,
                    default_database: None,
                    accessible_databases: AuthenticatedIdentity::default_database_set(is_su),
                },
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::Role;

    use crate::types::TenantId;

    #[test]
    fn in_memory_create_and_verify() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .bootstrap_superuser("nodedb", "secret")
            .expect("bootstrap superuser");
        assert!(store.verify_password("nodedb", "secret"));
        assert!(!store.verify_password("nodedb", "wrong"));
    }

    #[test]
    fn scram_blocks_expired_account_no_grace() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user("frank", "pass", TenantId::new(1), vec![Role::ReadWrite])
            .expect("create user");
        store
            .users
            .write()
            .get_mut("frank")
            .expect("frank")
            .password_expires_at = 1;

        assert!(matches!(
            store.get_scram_credentials("frank"),
            ScramLookup::Rejected(AuthRejection::PolicyDenied)
        ));
    }

    #[test]
    fn scram_allows_expired_account_within_grace_with_warning() {
        let mut store = CredentialStore::new().expect("in-memory credential store");
        store.password_expiry_grace_days = 30;
        store
            .create_user(
                "grace_user",
                "pass",
                TenantId::new(1),
                vec![Role::ReadWrite],
            )
            .expect("create user");
        store
            .users
            .write()
            .get_mut("grace_user")
            .expect("grace_user")
            .password_expires_at = crate::control::security::time::now_secs() - 1;

        let credentials = match store.get_scram_credentials("grace_user") {
            ScramLookup::Found(credentials) => credentials,
            ScramLookup::Rejected(reason) => panic!("unexpected rejection: {reason:?}"),
        };
        let warning = credentials
            .warning
            .expect("grace-period login should carry a warning");
        assert!(
            warning.contains("grace") || warning.contains("expired"),
            "warning should mention expiry or grace: {warning}"
        );
    }

    #[test]
    fn scram_blocks_must_change_password_no_grace() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user("hank", "pass", TenantId::new(1), vec![Role::ReadWrite])
            .expect("create user");
        store
            .set_must_change_password("hank", true)
            .expect("set password policy");

        assert!(matches!(
            store.get_scram_credentials("hank"),
            ScramLookup::Rejected(AuthRejection::PolicyDenied)
        ));
    }

    #[test]
    fn verify_password_blocks_expired_account() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user(
                "ivan",
                "correct_pass",
                TenantId::new(1),
                vec![Role::ReadWrite],
            )
            .expect("create user");
        store
            .users
            .write()
            .get_mut("ivan")
            .expect("ivan")
            .password_expires_at = 1;

        assert!(matches!(
            store.verify_password_with_status("ivan", "correct_pass"),
            PasswordVerification::Rejected(AuthRejection::PolicyDenied)
        ));
    }

    #[test]
    fn wrong_password_remains_credential_failure_when_policy_blocked() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user(
                "karl",
                "correct_pass",
                TenantId::new(1),
                vec![Role::ReadWrite],
            )
            .expect("create user");
        store
            .users
            .write()
            .get_mut("karl")
            .expect("karl")
            .password_expires_at = 1;

        assert!(matches!(
            store.verify_password_with_status("karl", "wrong_pass"),
            PasswordVerification::Rejected(AuthRejection::BadCredential)
        ));
    }

    #[test]
    fn verify_password_grace_period_emits_warning() {
        let mut store = CredentialStore::new().expect("in-memory credential store");
        store.password_expiry_grace_days = 7;
        store
            .create_user("judy", "pass", TenantId::new(1), vec![Role::ReadWrite])
            .expect("create user");
        store
            .users
            .write()
            .get_mut("judy")
            .expect("judy")
            .password_expires_at = crate::control::security::time::now_secs() - 1;

        match store.verify_password_with_status("judy", "pass") {
            PasswordVerification::Verified(warning) => assert!(warning.is_some()),
            PasswordVerification::Rejected(reason) => panic!("unexpected rejection: {reason:?}"),
        }
    }
}
