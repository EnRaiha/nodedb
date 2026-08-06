// SPDX-License-Identifier: BUSL-1.1

//! `RedactionStore` — in-memory policy registry keyed by
//! `(tenant_id, collection, for_role)`. CRUD + apply logic live here.

use std::collections::HashMap;
use std::sync::RwLock;

use super::apply::redacted_value;
use super::types::{RedactionPolicy, RedactionRule, policy_key};

/// Redaction policy store.
pub struct RedactionStore {
    /// `"{tenant_id}:{collection}:{for_role}"` → redaction policy.
    policies: RwLock<HashMap<String, RedactionPolicy>>,
}

impl Default for RedactionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RedactionStore {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire a read lock, recovering from `RwLock` poisoning.
    pub(super) fn lock_read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<String, RedactionPolicy>> {
        self.policies.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Acquire a write lock, recovering from `RwLock` poisoning.
    pub(super) fn lock_write(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<String, RedactionPolicy>> {
        self.policies.write().unwrap_or_else(|p| p.into_inner())
    }

    /// Create or replace a redaction policy.
    pub fn create_policy(&self, policy: RedactionPolicy) {
        let key = policy_key(policy.tenant_id, &policy.collection, &policy.for_role);
        let mut policies = self.lock_write();
        tracing::info!(
            name = %policy.name,
            tenant_id = policy.tenant_id,
            collection = %policy.collection,
            role = %policy.for_role,
            rules = policy.rules.len(),
            "redaction policy created"
        );
        policies.insert(key, policy);
    }

    /// Drop a redaction policy.
    pub fn drop_policy(&self, tenant_id: u64, collection: &str, for_role: &str) -> bool {
        let key = policy_key(tenant_id, collection, for_role);
        let mut policies = self.lock_write();
        policies.remove(&key).is_some()
    }

    /// Get redaction rules for a tenant+collection+role combination.
    pub fn rules_for(&self, tenant_id: u64, collection: &str, role: &str) -> Vec<RedactionRule> {
        let key = policy_key(tenant_id, collection, role);
        let policies = self.lock_read();
        policies
            .get(&key)
            .map(|p| p.rules.clone())
            .unwrap_or_default()
    }

    /// True when any of `roles` has a rule on `collection`.`field`.
    ///
    /// Allocation-free counterpart to [`RedactionStore::rules_for`] for the
    /// planner's refusal pass, which asks this per aggregate argument per role
    /// on the query path and must not clone the rule list to answer.
    pub fn has_rule_for_field(
        &self,
        tenant_id: u64,
        collection: &str,
        roles: &[String],
        field: &str,
    ) -> bool {
        let policies = self.lock_read();
        roles.iter().any(|role| {
            policies
                .get(&policy_key(tenant_id, collection, role))
                .is_some_and(|policy| policy.rules.iter().any(|rule| rule.field == field))
        })
    }

    /// True when any of `roles` has at least one rule on `collection`.
    pub fn has_any_rule_for_collection(
        &self,
        tenant_id: u64,
        collection: &str,
        roles: &[String],
    ) -> bool {
        let policies = self.lock_read();
        roles.iter().any(|role| {
            policies
                .get(&policy_key(tenant_id, collection, role))
                .is_some_and(|policy| !policy.rules.is_empty())
        })
    }

    /// True when any of `roles` has at least one rule anywhere in `tenant_id`.
    ///
    /// The collection is not part of the question, so this scans the registry.
    /// Reserved for callers that cannot name the collection a plan reads — an
    /// unscoped graph pattern — and must therefore fail closed on the whole
    /// tenant rather than on one collection.
    pub fn has_any_rule_for_roles(&self, tenant_id: u64, roles: &[String]) -> bool {
        let policies = self.lock_read();
        policies.values().any(|policy| {
            policy.tenant_id == tenant_id
                && !policy.rules.is_empty()
                && roles.contains(&policy.for_role)
        })
    }

    /// True when SOME role — any role at all — has at least one rule on
    /// `collection` in `tenant_id`.
    ///
    /// The role-agnostic counterpart to
    /// [`RedactionStore::has_any_rule_for_collection`], for the callers whose
    /// question is about the collection rather than about one identity: a
    /// subscriber whose entitlement cannot be established, and a definition-time
    /// refusal that must hold for every future reader. The role is not part of
    /// the question, so this scans the registry.
    pub fn has_any_rule_for_collection_any_role(&self, tenant_id: u64, collection: &str) -> bool {
        let policies = self.lock_read();
        policies.values().any(|policy| {
            policy.tenant_id == tenant_id
                && policy.collection == collection
                && !policy.rules.is_empty()
        })
    }

    /// True when SOME role has a rule on `field`, in `collection` when one is
    /// named and in any collection of `tenant_id` otherwise.
    ///
    /// `None` is for a caller that cannot name the collection its data comes
    /// from — a wildcard change stream carries rows from every collection in the
    /// tenant — and must therefore answer the tenant-wide question rather than
    /// silently pass.
    pub fn has_rule_for_field_any_role(
        &self,
        tenant_id: u64,
        collection: Option<&str>,
        field: &str,
    ) -> bool {
        let policies = self.lock_read();
        policies.values().any(|policy| {
            policy.tenant_id == tenant_id
                && collection.is_none_or(|name| policy.collection == name)
                && policy.rules.iter().any(|rule| rule.field == field)
        })
    }

    /// Apply redaction rules to a JSON document.
    ///
    /// Modifies the document in-place, replacing redacted field values.
    pub fn apply(
        &self,
        tenant_id: u64,
        collection: &str,
        roles: &[String],
        doc: &mut serde_json::Value,
    ) {
        let policies = self.lock_read();
        for role in roles {
            let key = policy_key(tenant_id, collection, role);
            if let Some(policy) = policies.get(&key) {
                for rule in &policy.rules {
                    if let Some(obj) = doc.as_object_mut()
                        && obj.contains_key(&rule.field)
                    {
                        let redacted = redacted_value(&rule.mode, obj.get(&rule.field));
                        obj.insert(rule.field.clone(), redacted);
                    }
                }
            }
        }
    }

    /// Clear all in-memory policies and reload from the catalog.
    /// Used by the recovery verifier repair path.
    pub fn clear_and_reload(
        &self,
        catalog: &crate::control::security::catalog::SystemCatalog,
    ) -> crate::Result<()> {
        let stored = catalog.load_all_redaction_policies()?;
        // Hold the write lock across clear + reload. Releasing it between the
        // two would let a concurrent reader observe an empty registry and
        // deliver fields in the clear that a policy says to redact — a
        // fail-open window for the width of the repair.
        let mut policies = self.lock_write();
        policies.clear();
        for s in stored {
            match s.to_runtime() {
                Ok(policy) => {
                    let key = policy_key(policy.tenant_id, &policy.collection, &policy.for_role);
                    policies.insert(key, policy);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "redaction_store.clear_and_reload: skipping unparseable policy");
                }
            }
        }
        Ok(())
    }

    /// List all redaction policies.
    pub fn list(&self) -> Vec<RedactionPolicy> {
        self.list_all_flat()
    }

    /// Flat list of all policies (all tenants, all collections, all roles).
    /// Used by the recovery verifier.
    pub fn list_all_flat(&self) -> Vec<RedactionPolicy> {
        let policies = self.lock_read();
        policies.values().cloned().collect()
    }

    /// Every policy in `tenant_id`, sorted by `(collection, for_role)` so
    /// `SHOW REDACTION POLICIES` renders in a stable order.
    pub fn policies_for_tenant(&self, tenant_id: u64) -> Vec<RedactionPolicy> {
        self.collect_sorted(|policy| policy.tenant_id == tenant_id)
    }

    /// Every policy on one `(tenant_id, collection)`, in the same order.
    pub fn policies_for_collection(
        &self,
        tenant_id: u64,
        collection: &str,
    ) -> Vec<RedactionPolicy> {
        self.collect_sorted(|policy| {
            policy.tenant_id == tenant_id && policy.collection == collection
        })
    }

    /// Clone every policy matching `keep`, sorted by `(collection, for_role)`.
    fn collect_sorted(&self, keep: impl Fn(&RedactionPolicy) -> bool) -> Vec<RedactionPolicy> {
        let policies = self.lock_read();
        let mut out: Vec<RedactionPolicy> = policies
            .values()
            .filter(|policy| keep(policy))
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.collection
                .cmp(&b.collection)
                .then_with(|| a.for_role.cmp(&b.for_role))
        });
        out
    }

    /// Total policies across all tenants and collections.
    pub fn policy_count(&self) -> usize {
        // Recover from a poisoned lock rather than reporting zero: this count
        // feeds the recovery-check verifier, and a spurious 0 reads as "the
        // registry lost every policy" and provokes a repair that isn't needed.
        self.lock_read().len()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::types::RedactionMode;
    use super::*;

    fn make_policy(
        name: &str,
        tenant_id: u64,
        collection: &str,
        for_role: &str,
        rules: Vec<RedactionRule>,
    ) -> RedactionPolicy {
        RedactionPolicy {
            name: name.into(),
            tenant_id,
            collection: collection.into(),
            for_role: for_role.into(),
            rules,
        }
    }

    #[test]
    fn mask_redaction() {
        let store = RedactionStore::new();
        store.create_policy(make_policy(
            "mask_pii",
            1,
            "users",
            "support",
            vec![
                RedactionRule {
                    field: "email".into(),
                    mode: RedactionMode::Mask("***@***.com".into()),
                },
                RedactionRule {
                    field: "ssn".into(),
                    mode: RedactionMode::Mask("***-**-****".into()),
                },
            ],
        ));

        let mut doc = json!({"email": "alice@example.com", "ssn": "123-45-6789", "name": "Alice"});
        store.apply(1, "users", &["support".into()], &mut doc);

        assert_eq!(doc["email"], "***@***.com");
        assert_eq!(doc["ssn"], "***-**-****");
        assert_eq!(doc["name"], "Alice"); // Not redacted.
    }

    #[test]
    fn hash_pseudonymization() {
        let store = RedactionStore::new();
        store.create_policy(make_policy(
            "pseudo",
            1,
            "users",
            "analyst",
            vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Hash,
            }],
        ));

        let mut doc1 = json!({"email": "alice@example.com"});
        let mut doc2 = json!({"email": "alice@example.com"});
        store.apply(1, "users", &["analyst".into()], &mut doc1);
        store.apply(1, "users", &["analyst".into()], &mut doc2);

        // Same input → same hash (joinable).
        assert_eq!(doc1["email"], doc2["email"]);
        // But not the original value.
        assert_ne!(doc1["email"], "alice@example.com");
        assert!(doc1["email"].as_str().unwrap().starts_with("hash:"));
    }

    #[test]
    fn hash_ignores_json_quoting() {
        // Regression: hashing `value.to_string()` on a JSON string hashes
        // the value *with* surrounding quotes, producing a digest that
        // doesn't match `sha256("alice@example.com")`. The raw scalar
        // must be hashed instead.
        use sha2::{Digest, Sha256};

        let store = RedactionStore::new();
        store.create_policy(make_policy(
            "pseudo",
            1,
            "users",
            "analyst",
            vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Hash,
            }],
        ));

        let mut doc = json!({"email": "alice@example.com"});
        store.apply(1, "users", &["analyst".into()], &mut doc);

        let expected = format!("hash:{:x}", Sha256::digest("alice@example.com".as_bytes()));
        assert_eq!(doc["email"], expected);
    }

    #[test]
    fn no_policy_no_redaction() {
        let store = RedactionStore::new();
        let mut doc = json!({"email": "alice@example.com"});
        store.apply(1, "users", &["admin".into()], &mut doc);
        assert_eq!(doc["email"], "alice@example.com");
    }

    #[test]
    fn tenant_scoping_isolates_same_collection_and_role() {
        // Two tenants with the same collection name and role must get
        // independent policies — collection names are not unique across
        // tenants, so keying the store without tenant_id would let one
        // tenant's redaction policy leak onto another tenant's rows.
        let store = RedactionStore::new();
        store.create_policy(make_policy(
            "tenant_a_mask",
            1,
            "users",
            "support",
            vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Mask("***@tenant-a.com".into()),
            }],
        ));

        // Tenant 2 has no policy for users/support — must not see tenant 1's rule.
        let mut doc_tenant_2 = json!({"email": "bob@example.com"});
        store.apply(2, "users", &["support".into()], &mut doc_tenant_2);
        assert_eq!(doc_tenant_2["email"], "bob@example.com");

        let mut doc_tenant_1 = json!({"email": "alice@example.com"});
        store.apply(1, "users", &["support".into()], &mut doc_tenant_1);
        assert_eq!(doc_tenant_1["email"], "***@tenant-a.com");

        assert_eq!(store.policy_count(), 1);
    }

    #[test]
    fn policy_count_and_list_all_flat() {
        let store = RedactionStore::new();
        store.create_policy(make_policy("p1", 1, "users", "support", vec![]));
        store.create_policy(make_policy("p2", 2, "users", "support", vec![]));
        assert_eq!(store.policy_count(), 2);
        assert_eq!(store.list_all_flat().len(), 2);
    }
}
