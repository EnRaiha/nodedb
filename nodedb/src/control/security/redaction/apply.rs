// SPDX-License-Identifier: BUSL-1.1

//! Applying redaction rules to already-decoded values.
//!
//! [`redacted_value`] is the single definition of what each [`RedactionMode`]
//! turns a field into; both the whole-document `RedactionStore::apply` and the
//! per-row [`RedactionStore::apply_flat_row`] go through it, so the mask / hash
//! / null semantics exist exactly once.
//!
//! `apply_flat_row` is the SELECT-path entry point: it rewrites one already
//! flattened result row, whose columns may come from several source
//! collections at once (a join), rather than one document belonging to a
//! single collection.

use serde_json::{Map, Value as JsonValue};

use super::store::RedactionStore;
use super::types::{RedactionMode, RedactionRule, policy_key};

/// The value `mode` produces for a field whose present value is `current`.
pub(super) fn redacted_value(mode: &RedactionMode, current: Option<&JsonValue>) -> JsonValue {
    match mode {
        RedactionMode::Mask(mask) => JsonValue::String(mask.clone()),
        RedactionMode::Hash => JsonValue::String(hash_value(current.unwrap_or(&JsonValue::Null))),
        // Writes an explicit null instead of removing the key: a redacted
        // column must still appear in a `SELECT *` result — valued null —
        // rather than disappearing from the derived column union.
        RedactionMode::Null => JsonValue::Null,
    }
}

/// SHA-256 hash for pseudonymization.
///
/// Hashes the raw scalar value, not its JSON-serialized form — a string
/// field hashes its bytes directly (not `"quoted"`), matching what an
/// operator expects `hash(email)` to mean.
fn hash_value(value: &JsonValue) -> String {
    use sha2::{Digest, Sha256};

    let digest = match value {
        JsonValue::String(s) => Sha256::digest(s.as_bytes()),
        JsonValue::Null => Sha256::digest(b""),
        other => Sha256::digest(other.to_string().as_bytes()),
    };
    format!("hash:{digest:x}")
}

/// Rewrite `row[key]` per `mode`, if the row actually carries that key.
fn redact_key(row: &mut Map<String, JsonValue>, key: &str, mode: &RedactionMode) {
    if !row.contains_key(key) {
        return;
    }
    let value = redacted_value(mode, row.get(key));
    row.insert(key.to_string(), value);
}

/// How many of the plan's sources a row-map key can be attributed to.
///
/// A key belongs to a source when it is that source's qualifier followed by a
/// dot. Anything else — a bare name, or a prefix matching two sources — is
/// unattributable and handled by the fail-closed pass.
fn attribution_count(key: &str, sources: &[(&str, Vec<&RedactionRule>)]) -> usize {
    sources
        .iter()
        .filter(|(qualifier, _)| {
            !qualifier.is_empty()
                && key.len() > qualifier.len()
                && key.is_char_boundary(qualifier.len())
                && key.as_bytes()[qualifier.len()] == b'.'
                && key.starts_with(*qualifier)
        })
        .count()
}

impl RedactionStore {
    /// Redact one already-flattened SELECT result row in place.
    ///
    /// `collections` lists the plan's source collections as
    /// `(qualifier, collection)`, where `qualifier` is the prefix that appears
    /// on that collection's keys in the row map: empty for a single-collection
    /// plan, and the join alias (or the collection name when there is no
    /// alias) for each side of a join.
    ///
    /// Matching differs by shape, and the difference is load-bearing:
    ///
    /// - **One source.** Row keys are bare field names, so a rule's `field`
    ///   matches the bare key.
    /// - **More than one source.** A rule matches ONLY the qualified key
    ///   `"{qualifier}.{field}"`. Matching the bare name here would redact the
    ///   wrong side of a join whenever both sides carry an identically named
    ///   column (`SELECT w.id, b.id`).
    /// - **Keys that cannot be attributed to exactly one source.** These are
    ///   redacted if ANY of the plan's collections has a rule for that bare
    ///   field name. This deliberately over-redacts: when the row map cannot
    ///   say which collection a column came from, delivering it in the clear
    ///   would be a policy bypass, so the ambiguous case fails closed.
    pub fn apply_flat_row(
        &self,
        tenant_id: u64,
        roles: &[String],
        collections: &[(String, String)],
        row: &mut Map<String, JsonValue>,
    ) {
        if roles.is_empty() || collections.is_empty() || row.is_empty() {
            return;
        }

        let policies = self.lock_read();
        let mut sources: Vec<(&str, Vec<&RedactionRule>)> = Vec::with_capacity(collections.len());
        for (qualifier, collection) in collections {
            let mut rules: Vec<&RedactionRule> = Vec::new();
            for role in roles {
                if let Some(policy) = policies.get(&policy_key(tenant_id, collection, role)) {
                    rules.extend(policy.rules.iter());
                }
            }
            sources.push((qualifier.as_str(), rules));
        }
        if sources.iter().all(|(_, rules)| rules.is_empty()) {
            return;
        }

        if let [(_, rules)] = sources.as_slice() {
            for rule in rules {
                redact_key(row, &rule.field, &rule.mode);
            }
            return;
        }

        for (qualifier, rules) in &sources {
            for rule in rules {
                redact_key(row, &format!("{qualifier}.{}", rule.field), &rule.mode);
            }
        }

        // Fail-closed pass over the keys no single source owns.
        let unattributed: Vec<String> = row
            .keys()
            .filter(|key| attribution_count(key, &sources) != 1)
            .cloned()
            .collect();
        for key in unattributed {
            let bare = key.rfind('.').map_or(key.as_str(), |dot| &key[dot + 1..]);
            let mode = sources
                .iter()
                .flat_map(|(_, rules)| rules.iter())
                .find(|rule| rule.field == bare || rule.field == key)
                .map(|rule| rule.mode.clone());
            if let Some(mode) = mode {
                redact_key(row, &key, &mode);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::types::RedactionPolicy;
    use super::*;

    fn store_with(collection: &str, role: &str, rules: Vec<RedactionRule>) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}"),
            tenant_id: 1,
            collection: collection.into(),
            for_role: role.into(),
            rules,
        });
        store
    }

    fn mask(field: &str, with: &str) -> RedactionRule {
        RedactionRule {
            field: field.into(),
            mode: RedactionMode::Mask(with.into()),
        }
    }

    fn row(value: serde_json::Value) -> Map<String, JsonValue> {
        match value {
            JsonValue::Object(map) => map,
            other => panic!("test row must be an object, got {other}"),
        }
    }

    #[test]
    fn single_source_matches_bare_keys() {
        let store = store_with("users", "support", vec![mask("email", "***")]);
        let mut r = row(json!({"email": "a@b.c", "name": "Alice"}));
        store.apply_flat_row(
            1,
            &["support".into()],
            &[(String::new(), "users".into())],
            &mut r,
        );
        assert_eq!(r["email"], "***");
        assert_eq!(r["name"], "Alice");
    }

    #[test]
    fn role_without_the_policy_sees_the_clear_value() {
        let store = store_with("users", "support", vec![mask("email", "***")]);
        let mut r = row(json!({"email": "a@b.c"}));
        store.apply_flat_row(
            1,
            &["analyst".into()],
            &[(String::new(), "users".into())],
            &mut r,
        );
        assert_eq!(r["email"], "a@b.c");
    }

    /// The rule belongs to the left side only; the right side's identically
    /// named column must survive in the clear.
    #[test]
    fn join_matches_only_the_ruled_sides_qualified_key() {
        let store = store_with("workspaces", "support", vec![mask("id", "***")]);
        let mut r = row(json!({"w.id": "w1", "b.id": "b1"}));
        store.apply_flat_row(
            1,
            &["support".into()],
            &[
                ("w".into(), "workspaces".into()),
                ("b".into(), "boards".into()),
            ],
            &mut r,
        );
        assert_eq!(r["w.id"], "***");
        assert_eq!(r["b.id"], "b1");
    }

    /// A join whose row map carries a bare key cannot say which side it came
    /// from, so the rule applies rather than being skipped.
    #[test]
    fn join_fails_closed_on_unattributable_keys() {
        let store = store_with("workspaces", "support", vec![mask("id", "***")]);
        let mut r = row(json!({"id": "w1", "b.title": "t"}));
        store.apply_flat_row(
            1,
            &["support".into()],
            &[
                ("w".into(), "workspaces".into()),
                ("b".into(), "boards".into()),
            ],
            &mut r,
        );
        assert_eq!(r["id"], "***");
        assert_eq!(r["b.title"], "t");
    }

    #[test]
    fn null_mode_keeps_the_key_present() {
        let store = store_with(
            "users",
            "support",
            vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Null,
            }],
        );
        let mut r = row(json!({"email": "a@b.c"}));
        store.apply_flat_row(
            1,
            &["support".into()],
            &[(String::new(), "users".into())],
            &mut r,
        );
        assert!(r.contains_key("email"), "the column must not disappear");
        assert_eq!(r["email"], JsonValue::Null);
    }

    #[test]
    fn no_policy_leaves_the_row_untouched() {
        let store = RedactionStore::new();
        let original = row(json!({"email": "a@b.c", "name": "Alice"}));
        let mut r = original.clone();
        store.apply_flat_row(
            1,
            &["support".into()],
            &[(String::new(), "users".into())],
            &mut r,
        );
        assert_eq!(r, original);
    }
}
