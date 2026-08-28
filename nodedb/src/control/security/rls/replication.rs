// SPDX-License-Identifier: BUSL-1.1

//! Applier-side helpers for replicated RLS policies.
//!
//! `install_replicated_policy` / `install_replicated_drop_policy`
//! mutate the in-memory `RlsPolicyStore` from the
//! `CatalogEntry::{PutRlsPolicy, DeleteRlsPolicy}` applier, bypassing
//! the normal `create_policy` path so the proposer and follower
//! paths apply identical state.

use super::store::RlsPolicyStore;
use super::types::{RlsPolicy, policy_key};

impl RlsPolicyStore {
    /// Install (create-or-replace) a replicated policy into the
    /// in-memory registry. Called from the `CatalogEntry::PutRlsPolicy`
    /// post-apply side effect on every node.
    pub fn install_replicated_policy(&self, policy: RlsPolicy) {
        let tenant_id = policy.tenant_id;
        let key = policy_key(tenant_id, &policy.collection);
        {
            let mut policies = self.lock_write();
            let list = policies.entry(key).or_default();
            if let Some(existing) = list.iter_mut().find(|p| p.name == policy.name) {
                *existing = policy;
            } else {
                list.push(policy);
            }
        }
        self.bump_tenant_version(tenant_id);
    }

    /// Remove a replicated policy from the in-memory registry.
    /// Returns `true` if a policy was removed.
    pub fn install_replicated_drop_policy(
        &self,
        tenant_id: u64,
        collection: &str,
        policy_name: &str,
    ) -> bool {
        let key = policy_key(tenant_id, collection);
        let removed = {
            let mut policies = self.lock_write();
            if let Some(list) = policies.get_mut(&key) {
                let before = list.len();
                list.retain(|p| p.name != policy_name);
                list.len() < before
            } else {
                false
            }
        };
        if removed {
            self.bump_tenant_version(tenant_id);
        }
        removed
    }

    /// Check whether a policy with the given name already exists
    /// on the given (tenant, collection). Used by the handler
    /// pre-check before proposing `PutRlsPolicy`.
    pub fn policy_exists(&self, tenant_id: u64, collection: &str, policy_name: &str) -> bool {
        let key = policy_key(tenant_id, collection);
        let policies = self.lock_read();
        policies
            .get(&key)
            .map(|list| list.iter().any(|p| p.name == policy_name))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::PolicyType;
    use super::*;

    fn policy(name: &str, tenant_id: u64) -> RlsPolicy {
        RlsPolicy {
            name: name.into(),
            collection: "users".into(),
            display_collection: "users".into(),
            tenant_id,
            policy_type: PolicyType::Read,
            compiled_predicate: None,
            mode: Default::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        }
    }

    /// The Raft-applier path bumps the tenant version exactly like the
    /// proposer-side `create_policy` / `drop_policy` do — this is the path
    /// production `CREATE`/`DROP RLS POLICY` actually runs through, on
    /// every node, so a stale plan cache must go stale here too.
    #[test]
    fn install_replicated_policy_bumps_tenant_version() {
        let store = RlsPolicyStore::new();
        assert_eq!(store.tenant_version(1), 0);

        store.install_replicated_policy(policy("p1", 1));
        assert_eq!(store.tenant_version(1), 1);

        // Replacing the same policy still bumps — its predicate may differ.
        store.install_replicated_policy(policy("p1", 1));
        assert_eq!(store.tenant_version(1), 2);

        assert!(store.install_replicated_drop_policy(1, "users", "p1"));
        assert_eq!(store.tenant_version(1), 3);

        // Dropping an absent policy changes nothing.
        assert!(!store.install_replicated_drop_policy(1, "users", "p1"));
        assert_eq!(store.tenant_version(1), 3);

        assert_eq!(store.tenant_version(2), 0);
    }
}
