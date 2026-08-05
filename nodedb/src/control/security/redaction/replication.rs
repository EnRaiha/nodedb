// SPDX-License-Identifier: BUSL-1.1

//! Applier-side helpers for replicated redaction policies.
//!
//! `install_replicated_policy` / `install_replicated_drop_policy`
//! mutate the in-memory `RedactionStore` from the
//! `CatalogEntry::{PutRedactionPolicy, DeleteRedactionPolicy}` applier,
//! bypassing the normal `create_policy` path so the proposer and follower
//! paths apply identical state.

use super::store::RedactionStore;
use super::types::{RedactionPolicy, policy_key};

impl RedactionStore {
    /// Install (create-or-replace) a replicated policy into the in-memory
    /// registry. Called from the `CatalogEntry::PutRedactionPolicy`
    /// post-apply side effect on every node.
    pub fn install_replicated_policy(&self, policy: RedactionPolicy) {
        let key = policy_key(policy.tenant_id, &policy.collection, &policy.for_role);
        let mut policies = self.lock_write();
        policies.insert(key, policy);
    }

    /// Remove a replicated policy from the in-memory registry.
    /// Returns `true` if a policy was removed.
    pub fn install_replicated_drop_policy(
        &self,
        tenant_id: u64,
        collection: &str,
        for_role: &str,
    ) -> bool {
        let key = policy_key(tenant_id, collection, for_role);
        let mut policies = self.lock_write();
        policies.remove(&key).is_some()
    }

    /// Check whether a policy already exists for the given
    /// (tenant, collection, role). Used by the handler pre-check before
    /// proposing `PutRedactionPolicy`.
    pub fn policy_exists(&self, tenant_id: u64, collection: &str, for_role: &str) -> bool {
        let key = policy_key(tenant_id, collection, for_role);
        let policies = self.lock_read();
        policies.contains_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::redaction::types::{RedactionMode, RedactionRule};

    fn make_policy(
        name: &str,
        tenant_id: u64,
        collection: &str,
        for_role: &str,
    ) -> RedactionPolicy {
        RedactionPolicy {
            name: name.into(),
            tenant_id,
            collection: collection.into(),
            for_role: for_role.into(),
            rules: vec![],
        }
    }

    #[test]
    fn install_replicated_policy_replaces_existing() {
        let store = RedactionStore::new();
        store.install_replicated_policy(make_policy("v1", 1, "users", "support"));
        store.install_replicated_policy(RedactionPolicy {
            rules: vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Null,
            }],
            ..make_policy("v2", 1, "users", "support")
        });

        assert_eq!(store.policy_count(), 1);
        let policies = store.list_all_flat();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].name, "v2");
        assert_eq!(policies[0].rules.len(), 1);
    }

    #[test]
    fn policy_exists_and_drop_round_trip() {
        let store = RedactionStore::new();
        assert!(!store.policy_exists(1, "users", "support"));

        store.install_replicated_policy(make_policy("p1", 1, "users", "support"));
        assert!(store.policy_exists(1, "users", "support"));
        // Different tenant, same collection+role — must not exist.
        assert!(!store.policy_exists(2, "users", "support"));

        assert!(store.install_replicated_drop_policy(1, "users", "support"));
        assert!(!store.policy_exists(1, "users", "support"));
        // Dropping again returns false.
        assert!(!store.install_replicated_drop_policy(1, "users", "support"));
    }
}
