// SPDX-License-Identifier: Apache-2.0

use std::time::{SystemTime, UNIX_EPOCH};

use loro::LoroValue;

use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::policy::{CollectionPolicy, ConflictPolicy, PolicyRegistry};
use nodedb_crdt::validator::ProposedChange;

pub fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn schema() -> ConstraintSet {
    let mut cs = ConstraintSet::new();
    cs.add_unique("users_email_unique", "users", "email");
    cs.add_not_null("users_name_nn", "users", "name");
    cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
    cs
}

pub fn user_row(name: &str, email: &str) -> Vec<(String, LoroValue)> {
    vec![
        ("name".into(), LoroValue::String(name.to_string().into())),
        ("email".into(), LoroValue::String(email.to_string().into())),
    ]
}

pub fn user_change(row_id: &str, name: &str, email: &str) -> ProposedChange {
    ProposedChange {
        collection: "users".into(),
        row_id: row_id.into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: user_row(name, email),
    }
}

pub fn post_change(row_id: &str, author_id: &str) -> ProposedChange {
    ProposedChange {
        collection: "posts".into(),
        row_id: row_id.into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![(
            "author_id".into(),
            LoroValue::String(author_id.to_string().into()),
        )],
    }
}

pub fn rename_suffix_policy() -> PolicyRegistry {
    let mut policies = PolicyRegistry::new();
    let mut policy = CollectionPolicy::ephemeral();
    policy.unique = ConflictPolicy::RenameSuffix;
    policies.set("users", policy);
    policies
}

pub fn cascade_defer_policy(ttl_secs: u64, max_retries: u32) -> PolicyRegistry {
    let mut policies = PolicyRegistry::new();
    let mut policy = CollectionPolicy::ephemeral();
    policy.foreign_key = ConflictPolicy::CascadeDefer {
        max_retries,
        ttl_secs,
    };
    policies.set("posts", policy);
    policies
}

// Re-export CrdtAuthContext so test modules don't need to import it separately.
pub use nodedb_crdt::CrdtAuthContext as AuthCtx;
