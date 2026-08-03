// SPDX-License-Identifier: BUSL-1.1

//! Durable bootstrap of the configured superuser.

use rand::RngExt;

use crate::control::security::identity::Role;
use crate::control::security::time::now_secs;
use crate::types::TenantId;

use super::super::hash::{
    compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
};
use super::super::record::UserRecord;
use super::core::{
    CredentialStore, PasswordPrincipal, read_lock, validate_password_assignment, write_lock,
};

impl CredentialStore {
    /// Bootstrap the configured superuser with password credentials.
    ///
    /// Existing regular users retain their stable identity while their password
    /// material is replaced. Service accounts cannot be promoted into an
    /// interactive superuser.
    pub fn bootstrap_superuser(&self, username: &str, password: &str) -> crate::Result<()> {
        let (observed_user_id, principal) = {
            let users = read_lock(&self.users);
            match users.get(username) {
                Some(record) => (
                    Some(record.user_id),
                    PasswordPrincipal::Existing {
                        is_service_account: record.is_service_account,
                    },
                ),
                None => (None, PasswordPrincipal::New),
            }
        };
        validate_password_assignment(password, principal)?;

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password, &self.argon2_config)?;

        let mut users = write_lock(&self.users);

        if observed_user_id.is_some() && !users.contains_key(username) {
            return Err(changed_during_bootstrap(username));
        }
        if observed_user_id.is_none() && users.contains_key(username) {
            return Err(changed_during_bootstrap(username));
        }

        if let Some(existing) = users.get(username) {
            if Some(existing.user_id) != observed_user_id {
                return Err(changed_during_bootstrap(username));
            }
            validate_password_assignment(
                password,
                PasswordPrincipal::Existing {
                    is_service_account: existing.is_service_account,
                },
            )?;

            let mut candidate = existing.clone();
            candidate.password_hash = password_hash;
            candidate.scram_salt = salt;
            candidate.scram_salted_password = scram_salted_password;
            candidate.is_superuser = true;
            candidate.is_active = true;
            candidate.must_change_password = false;
            candidate.password_changed_at = now_secs();
            if !candidate.roles.contains(&Role::Superuser) {
                candidate.roles.push(Role::Superuser);
            }
            self.persist_user(&mut candidate)?;
            users.insert(username.to_string(), candidate);
        } else {
            let mut next_user_id = write_lock(&self.next_user_id);
            let user_id = *next_user_id;
            let following_user_id = user_id.checked_add(1).ok_or_else(|| crate::Error::Config {
                detail: "user ID space exhausted".into(),
            })?;
            let now = now_secs();
            let mut record = UserRecord {
                user_id,
                username: username.to_string(),
                tenant_id: TenantId::new(0),
                password_hash,
                scram_salt: salt,
                scram_salted_password,
                roles: vec![Role::Superuser],
                is_superuser: true,
                is_active: true,
                is_service_account: false,
                created_at: now,
                updated_at: now,
                password_expires_at: self.compute_expiry(),
                must_change_password: false,
                password_changed_at: now,
                default_database_id: 0,
                accessible_databases: vec![],
            };
            self.persist_new_user_with_next_id(&mut record, following_user_id)?;
            *next_user_id = following_user_id;
            users.insert(username.to_string(), record);
        }

        Ok(())
    }

    /// Materialize the configured trust-mode superuser as a durable identity.
    ///
    /// The generated secret is deliberately never returned or logged. It only
    /// supplies structurally valid password and SCRAM material so the principal
    /// remains catalog-valid if the node restarts. Password-mode bootstrap
    /// replaces that material before password listeners start.
    pub fn bootstrap_trust_superuser(&self, username: &str) -> crate::Result<()> {
        let mut configured_name = write_lock(&self.trust_superuser_name);
        if let Some(existing_name) = configured_name.as_deref()
            && existing_name != username
        {
            return Err(crate::Error::Config {
                detail: format!(
                    "trust superuser is already configured as '{existing_name}', not '{username}'"
                ),
            });
        }

        let observed_user_id = {
            let users = read_lock(&self.users);
            match users.get(username) {
                Some(record) if record.is_service_account => {
                    return Err(service_account_bootstrap_error(username));
                }
                Some(record) => Some(record.user_id),
                None => None,
            }
        };

        if let Some(observed_user_id) = observed_user_id {
            let mut users = write_lock(&self.users);
            let existing = users
                .get(username)
                .ok_or_else(|| changed_during_bootstrap(username))?;
            if existing.user_id != observed_user_id {
                return Err(changed_during_bootstrap(username));
            }
            if existing.is_service_account {
                return Err(service_account_bootstrap_error(username));
            }

            let role_missing = !existing.roles.contains(&Role::Superuser);
            if existing.is_active && existing.is_superuser && !role_missing {
                *configured_name = Some(username.to_string());
                return Ok(());
            }
            let mut candidate = existing.clone();
            candidate.is_active = true;
            candidate.is_superuser = true;
            if role_missing {
                candidate.roles.push(Role::Superuser);
            }
            self.persist_user(&mut candidate)?;
            users.insert(username.to_string(), candidate);
            *configured_name = Some(username.to_string());
            return Ok(());
        }

        let secret = generate_internal_trust_secret();
        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(&secret, &salt);
        let password_hash = hash_password_argon2(&secret, &self.argon2_config)?;

        let mut users = write_lock(&self.users);
        if users.contains_key(username) {
            return Err(changed_during_bootstrap(username));
        }

        let mut next_user_id = write_lock(&self.next_user_id);
        let user_id = *next_user_id;
        let following_user_id = user_id.checked_add(1).ok_or_else(|| crate::Error::Config {
            detail: "user ID space exhausted".into(),
        })?;
        let now = now_secs();
        let mut record = UserRecord {
            user_id,
            username: username.to_string(),
            tenant_id: TenantId::new(1),
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles: vec![Role::Superuser],
            is_superuser: true,
            is_active: true,
            is_service_account: false,
            created_at: now,
            updated_at: now,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: now,
            default_database_id: 0,
            accessible_databases: vec![],
        };
        self.persist_new_user_with_next_id(&mut record, following_user_id)?;
        *next_user_id = following_user_id;
        users.insert(username.to_string(), record);
        *configured_name = Some(username.to_string());
        Ok(())
    }

    /// Return the durable principal configured for trust-mode auto-authentication.
    pub fn configured_trust_superuser(&self) -> crate::Result<Option<String>> {
        Ok(read_lock(&self.trust_superuser_name).clone())
    }
}

fn generate_internal_trust_secret() -> String {
    const ALPHABET: &[u8] = b"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::rng();
    (0..48)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect()
}

fn changed_during_bootstrap(username: &str) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!("user '{username}' changed while credentials were being prepared"),
    }
}

fn service_account_bootstrap_error(username: &str) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!("cannot bootstrap service account '{username}' as superuser"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait GuardExpect<T> {
        fn expect(self, message: &str) -> Self;
    }

    impl<'a, T> GuardExpect<T> for parking_lot::RwLockReadGuard<'a, T> {
        fn expect(self, _message: &str) -> Self {
            self
        }
    }

    fn assert_bad_request(error: crate::Error) {
        assert!(matches!(error, crate::Error::BadRequest { .. }));
    }

    fn assert_user_unchanged(before: &UserRecord, after: &UserRecord) {
        assert_eq!(after.user_id, before.user_id);
        assert_eq!(after.username, before.username);
        assert_eq!(after.tenant_id, before.tenant_id);
        assert_eq!(after.password_hash, before.password_hash);
        assert_eq!(after.scram_salt, before.scram_salt);
        assert_eq!(after.scram_salted_password, before.scram_salted_password);
        assert_eq!(after.roles, before.roles);
        assert_eq!(after.is_superuser, before.is_superuser);
        assert_eq!(after.is_active, before.is_active);
        assert_eq!(after.is_service_account, before.is_service_account);
        assert_eq!(after.created_at, before.created_at);
        assert_eq!(after.updated_at, before.updated_at);
        assert_eq!(after.password_expires_at, before.password_expires_at);
        assert_eq!(after.must_change_password, before.must_change_password);
        assert_eq!(after.password_changed_at, before.password_changed_at);
        assert_eq!(after.default_database_id, before.default_database_id);
        assert_eq!(after.accessible_databases, before.accessible_databases);
    }

    #[test]
    fn trust_bootstrap_materializes_durable_superuser() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");

        let user = store.get_user("nodedb").expect("stored trust superuser");
        assert_eq!(user.tenant_id, TenantId::new(1));
        assert!(user.is_active);
        assert!(user.is_superuser);
        assert!(!user.is_service_account);
        assert!(user.roles.contains(&Role::Superuser));
        assert!(!user.password_hash.is_empty());
        assert!(!user.scram_salt.is_empty());
        assert!(!user.scram_salted_password.is_empty());
        assert_eq!(
            store
                .configured_trust_superuser()
                .expect("configured trust principal"),
            Some("nodedb".to_string())
        );
    }

    #[test]
    fn trust_bootstrap_persistence_failure_is_atomic() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");
        store.catalog().fail_next_user_counter_write_for_test();

        let error = store
            .bootstrap_trust_superuser("nodedb")
            .expect_err("injected catalog failure must reject bootstrap");

        assert!(matches!(error, crate::Error::Storage { .. }));
        assert!(store.get_user("nodedb").is_none());
        assert!(
            store
                .catalog()
                .get_user("nodedb")
                .expect("read catalog")
                .is_none()
        );
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id
        );
        assert_eq!(
            store
                .catalog()
                .load_next_user_id()
                .expect("load durable next user ID"),
            next_user_id
        );
        assert_eq!(
            store
                .configured_trust_superuser()
                .expect("configured trust principal"),
            None
        );
    }

    #[test]
    fn repeated_trust_bootstrap_preserves_identity_and_credentials() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .bootstrap_trust_superuser("nodedb")
            .expect("first trust bootstrap");
        let before = store.get_user("nodedb").expect("first record");

        store
            .bootstrap_trust_superuser("nodedb")
            .expect("second trust bootstrap");
        let after = store.get_user("nodedb").expect("second record");

        assert_user_unchanged(&before, &after);
    }

    #[test]
    fn trust_bootstrap_rejects_conflicting_configured_name_without_mutation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .bootstrap_trust_superuser("nodedb")
            .expect("first trust bootstrap");
        let before = store.get_user("nodedb").expect("configured record");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");

        let error = store
            .bootstrap_trust_superuser("different-root")
            .expect_err("conflicting trust principal must fail");

        assert!(matches!(error, crate::Error::Config { .. }));
        assert_user_unchanged(
            &before,
            &store.get_user("nodedb").expect("configured record"),
        );
        assert!(store.get_user("different-root").is_none());
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id
        );
        assert_eq!(
            store
                .configured_trust_superuser()
                .expect("configured trust principal"),
            Some("nodedb".to_string())
        );
    }

    #[test]
    fn trust_bootstrap_preserves_existing_regular_user_credentials() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user(
                "configured-root",
                "existing-password",
                TenantId::new(7),
                vec![Role::ReadOnly],
            )
            .expect("create regular user");
        let before = store.get_user("configured-root").expect("regular user");

        store
            .bootstrap_trust_superuser("configured-root")
            .expect("elevate configured user");
        let after = store.get_user("configured-root").expect("elevated user");

        assert_eq!(after.user_id, before.user_id);
        assert_eq!(after.tenant_id, before.tenant_id);
        assert_eq!(after.password_hash, before.password_hash);
        assert_eq!(after.scram_salt, before.scram_salt);
        assert_eq!(after.scram_salted_password, before.scram_salted_password);
        assert!(after.is_active);
        assert!(after.is_superuser);
        assert!(after.roles.contains(&Role::Superuser));
        assert!(store.verify_password("configured-root", "existing-password"));
    }

    #[test]
    fn trust_bootstrap_rejects_service_account_atomically() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_service_account(
                "configured-root",
                TenantId::new(7),
                vec![Role::ReadOnly],
                vec![],
            )
            .expect("create service account");
        let before = store.get_user("configured-root").expect("service account");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");
        let version = store.current_version(user_id);

        let error = store
            .bootstrap_trust_superuser("configured-root")
            .expect_err("service account bootstrap must fail");

        assert_bad_request(error);
        let after = store.get_user("configured-root").expect("service account");
        assert_user_unchanged(&before, &after);
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id
        );
        assert_eq!(store.current_version(user_id), version);
        assert_eq!(
            store
                .configured_trust_superuser()
                .expect("configured trust principal"),
            None
        );
    }

    #[test]
    fn password_bootstrap_replaces_internal_trust_credential_without_rekeying_identity() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .bootstrap_trust_superuser("nodedb")
            .expect("trust bootstrap");
        let before = store.get_user("nodedb").expect("trust record");

        store
            .bootstrap_superuser("nodedb", "operator-password")
            .expect("password bootstrap");
        let after = store.get_user("nodedb").expect("password record");

        assert_eq!(after.user_id, before.user_id);
        assert_eq!(after.tenant_id, before.tenant_id);
        assert_ne!(after.password_hash, before.password_hash);
        assert!(store.verify_password("nodedb", "operator-password"));
    }

    #[test]
    fn password_bootstrap_persistence_failure_is_atomic() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");
        store.catalog().fail_next_user_counter_write_for_test();

        let error = store
            .bootstrap_superuser("nodedb", "operator-password")
            .expect_err("injected catalog failure must reject bootstrap");

        assert!(matches!(error, crate::Error::Storage { .. }));
        assert!(store.get_user("nodedb").is_none());
        assert!(
            store
                .catalog()
                .get_user("nodedb")
                .expect("read catalog")
                .is_none()
        );
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id
        );
        assert_eq!(
            store
                .catalog()
                .load_next_user_id()
                .expect("load durable next user ID"),
            next_user_id
        );
    }

    #[test]
    fn password_bootstrap_rejects_empty_password_for_absent_user_without_allocation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");

        let error = store
            .bootstrap_superuser("empty-bootstrap", "")
            .expect_err("empty bootstrap password must be rejected");

        assert_bad_request(error);
        assert!(store.get_user("empty-bootstrap").is_none());
        assert!(
            store
                .catalog()
                .get_user("empty-bootstrap")
                .expect("read catalog")
                .is_none(),
            "rejected bootstrap must not write the catalog"
        );
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id
        );
    }

    #[test]
    fn password_bootstrap_rejects_empty_password_for_existing_user_without_mutation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_user(
                "regular-bootstrap",
                "old-password",
                TenantId::new(3),
                vec![Role::ReadOnly],
            )
            .expect("create regular user");
        let before = store.get_user("regular-bootstrap").expect("regular user");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");
        let version = store.current_version(user_id);

        let error = store
            .bootstrap_superuser("regular-bootstrap", "")
            .expect_err("empty bootstrap password must be rejected");

        assert_bad_request(error);
        let after = store.get_user("regular-bootstrap").expect("regular user");
        assert_user_unchanged(&before, &after);
        assert_eq!(store.current_version(user_id), version);
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id
        );
    }

    #[test]
    fn password_bootstrap_rejects_service_account_without_mutation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_service_account(
                "bootstrap-api",
                TenantId::new(3),
                vec![Role::ReadOnly],
                vec![],
            )
            .expect("create service account");
        let before = store.get_user("bootstrap-api").expect("service account");
        let version = store.current_version(user_id);

        let error = store
            .bootstrap_superuser("bootstrap-api", "non-empty-secret")
            .expect_err("service accounts must not be bootstrapped with passwords");

        assert_bad_request(error);
        let after = store.get_user("bootstrap-api").expect("service account");
        assert_user_unchanged(&before, &after);
        assert_eq!(store.current_version(user_id), version);
    }

    #[test]
    fn password_bootstrap_updates_existing_regular_user() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user(
                "bootstrap-regular",
                "old-password",
                TenantId::new(3),
                vec![Role::ReadOnly],
            )
            .expect("create regular user");

        store
            .bootstrap_superuser("bootstrap-regular", "new-password")
            .expect("password bootstrap");

        let user = store
            .get_user("bootstrap-regular")
            .expect("bootstrapped user");
        assert!(store.verify_password("bootstrap-regular", "new-password"));
        assert!(!store.verify_password("bootstrap-regular", "old-password"));
        assert!(user.is_active);
        assert!(user.is_superuser);
        assert!(user.roles.contains(&Role::Superuser));
        assert!(!user.is_service_account);
    }
}
