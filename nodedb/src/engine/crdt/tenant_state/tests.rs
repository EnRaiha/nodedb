// SPDX-License-Identifier: BUSL-1.1

use loro::LoroValue;

use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::policy::CollectionPolicy;
use nodedb_crdt::pre_validate::PreValidationResult;
use nodedb_crdt::validator::ProposedChange;

use crate::types::TenantId;

use super::core::TenantCrdtEngine;

fn test_constraints() -> ConstraintSet {
    let mut cs = ConstraintSet::new();
    cs.add_unique("users_email_unique", "users", "email");
    cs.add_not_null("users_name_nn", "users", "name");
    cs
}

#[test]
fn valid_write_applies() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            ("name".into(), LoroValue::String("Alice".into())),
            (
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            ),
        ],
    };

    engine
        .validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
        )
        .unwrap();

    assert!(engine.row_exists("users", "u1"));
    assert_eq!(engine.dlq_len(), 0);
}

#[test]
fn constraint_violation_routes_to_dlq() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();
    // Use strict policy so violations escalate to DLQ instead of auto-resolving.
    engine
        .validator
        .policies_mut()
        .set("users", CollectionPolicy::strict());

    // Missing "name" field violates NOT NULL.
    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
    };

    let err = engine
        .validate_and_apply(
            42,
            nodedb_crdt::CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
        )
        .unwrap_err();

    assert!(matches!(err, crate::Error::Crdt(_)));
    assert_eq!(engine.dlq_len(), 1);
}

#[test]
fn pre_validate_fast_rejects() {
    let engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
    };

    match engine.pre_validate(&change) {
        PreValidationResult::FastReject { constraint, .. } => {
            assert_eq!(constraint, "users_name_nn");
        }
        _ => panic!("expected fast reject"),
    }
}

#[test]
fn unique_violation_after_first_write() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();
    // Strict mode: UNIQUE violations escalate to DLQ.
    engine
        .validator
        .policies_mut()
        .set("users", CollectionPolicy::strict());

    let first = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            ("name".into(), LoroValue::String("Alice".into())),
            (
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            ),
        ],
    };
    engine
        .validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &first,
            b"d1".to_vec(),
        )
        .unwrap();

    // Second write with same email should fail.
    let second = ProposedChange {
        collection: "users".into(),
        row_id: "u2".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            ("name".into(), LoroValue::String("Bob".into())),
            (
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            ),
        ],
    };
    assert!(
        engine
            .validate_and_apply(
                2,
                nodedb_crdt::CrdtAuthContext::default(),
                &second,
                b"d2".to_vec()
            )
            .is_err()
    );
    assert_eq!(engine.dlq_len(), 1);
}

// ── per-collection isolation ──────────────────────────────────────────────────

#[test]
fn separate_collections_have_isolated_docs() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("name".into(), LoroValue::String("Alice".into()))],
    };
    engine
        .validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &change,
            b"d".to_vec(),
        )
        .unwrap();

    assert!(engine.row_exists("users", "u1"));
    assert!(!engine.row_exists("orders", "u1"));
    assert!(engine.read_row("users", "u1").is_some());
    assert!(engine.read_row("orders", "u1").is_none());
}

// ── cross-collection FK via tenant-wide validator ─────────────────────────────

fn fk_constraints() -> ConstraintSet {
    let mut cs = ConstraintSet::new();
    cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
    cs
}

fn apply_change(
    engine: &mut TenantCrdtEngine,
    collection: &str,
    row_id: &str,
    fields: Vec<(String, LoroValue)>,
) -> crate::Result<()> {
    let change = ProposedChange {
        collection: collection.into(),
        row_id: row_id.into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields,
    };
    engine.validate_and_apply(
        1,
        nodedb_crdt::CrdtAuthContext::default(),
        &change,
        b"d".to_vec(),
    )
}

#[test]
fn cross_collection_fk_rejects_missing_referent() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(2), 0, fk_constraints()).unwrap();
    engine.set_collection_policy_typed("posts", CollectionPolicy::strict());

    let result = apply_change(
        &mut engine,
        "posts",
        "p1",
        vec![
            ("title".into(), LoroValue::String("Hello".into())),
            ("author_id".into(), LoroValue::String("u1".into())),
        ],
    );

    assert!(result.is_err());
    assert_eq!(engine.dlq_len(), 1);
    assert!(!engine.row_exists("posts", "p1"));
}

#[test]
fn cross_collection_fk_accepts_after_referent_inserted() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(3), 0, fk_constraints()).unwrap();
    engine.set_collection_policy_typed("posts", CollectionPolicy::strict());

    apply_change(
        &mut engine,
        "users",
        "u1",
        vec![("name".into(), LoroValue::String("Alice".into()))],
    )
    .unwrap();

    apply_change(
        &mut engine,
        "posts",
        "p1",
        vec![
            ("title".into(), LoroValue::String("Hello".into())),
            ("author_id".into(), LoroValue::String("u1".into())),
        ],
    )
    .unwrap();

    assert!(engine.row_exists("users", "u1"));
    assert!(engine.row_exists("posts", "p1"));
    assert_eq!(engine.dlq_len(), 0);
}

// ── array-surrogate FK via tenant registry ────────────────────────────────────

#[test]
fn array_surrogate_satisfies_cross_engine_fk() {
    let mut cs = ConstraintSet::new();
    cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
    let mut engine = TenantCrdtEngine::new(TenantId::new(4), 0, cs).unwrap();
    engine.set_collection_policy_typed("posts", CollectionPolicy::strict());

    engine.register_array_surrogate("arr_42");

    apply_change(
        &mut engine,
        "posts",
        "p1",
        vec![
            ("title".into(), LoroValue::String("Hello".into())),
            ("author_id".into(), LoroValue::String("arr_42".into())),
        ],
    )
    .unwrap();

    assert!(engine.row_exists("posts", "p1"));
    assert_eq!(engine.dlq_len(), 0);
}

#[test]
fn installed_constraints_are_enforced_and_droppable() {
    // Engine starts with NO constraints; U2 installs them at runtime.
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
    // Strict policy so a UNIQUE violation escalates (returns Err) instead of
    // auto-resolving.
    engine
        .validator
        .policies_mut()
        .set("users", CollectionPolicy::strict());

    let unique_email = nodedb_crdt::Constraint {
        name: "users_email_unique".into(),
        collection: "users".into(),
        field: "email".into(),
        kind: nodedb_crdt::ConstraintKind::Unique,
    };
    assert!(engine.set_collection_constraints("users", 1, vec![unique_email.clone()]));

    let mk = |row: &str, coll: &str, email: &str| ProposedChange {
        collection: coll.into(),
        row_id: row.into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("email".into(), LoroValue::String(email.into()))],
    };

    // First insert with the email succeeds.
    engine
        .validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &mk("u1", "users", "a@b.com"),
            b"d".to_vec(),
        )
        .unwrap();

    // Duplicate email in the same collection is rejected.
    assert!(
        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &mk("u2", "users", "a@b.com"),
                b"d".to_vec()
            )
            .is_err(),
        "duplicate email must violate the installed UNIQUE constraint"
    );

    // A different collection carries no such constraint — same value is fine.
    engine
        .validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &mk("p1", "posts", "a@b.com"),
            b"d".to_vec(),
        )
        .unwrap();

    // After dropping the constraint, the duplicate is accepted.
    assert!(engine.drop_collection_constraints("users", 2));
    engine
        .validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &mk("u3", "users", "a@b.com"),
            b"d".to_vec(),
        )
        .unwrap();
    assert!(engine.row_exists("users", "u3"));
}

#[test]
fn set_collection_constraints_replaces_rather_than_accumulates() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
    let c = nodedb_crdt::Constraint {
        name: "users_email_unique".into(),
        collection: "users".into(),
        field: "email".into(),
        kind: nodedb_crdt::ConstraintKind::Unique,
    };
    engine.set_collection_constraints("users", 1, vec![c.clone()]);
    engine.set_collection_constraints("users", 1, vec![c.clone()]);
    // Setting twice (same version, allowed by the `>=` fence) leaves exactly
    // one rule scoped to "users".
    assert_eq!(engine.validator.constraints_for("users").len(), 1);

    // An empty set clears the collection's constraints.
    engine.set_collection_constraints("users", 2, Vec::<nodedb_crdt::Constraint>::new());
    assert_eq!(engine.validator.constraints_for("users").len(), 0);
}

/// Builds a UNIQUE constraint named `name` on `users.email`.
fn unique_named(name: &str) -> nodedb_crdt::Constraint {
    nodedb_crdt::Constraint {
        name: name.into(),
        collection: "users".into(),
        field: "email".into(),
        kind: nodedb_crdt::ConstraintKind::Unique,
    }
}

#[test]
fn set_constraint_version_fence_rejects_stale_and_accepts_newer() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();

    // Install version 5: the constraint is visible.
    let v5 = unique_named("rule_v5");
    assert!(engine.set_collection_constraints("users", 5, vec![v5.clone()]));
    let installed = engine.constraints_for_collection("users");
    assert_eq!(installed.len(), 1);
    assert_eq!(installed[0].name, "rule_v5");

    // An older version 3 with a different set is rejected as stale; the
    // version-5 constraints remain untouched.
    let v3 = unique_named("rule_v3");
    assert!(!engine.set_collection_constraints("users", 3, vec![v3.clone()]));
    let unchanged = engine.constraints_for_collection("users");
    assert_eq!(unchanged.len(), 1);
    assert_eq!(unchanged[0].name, "rule_v5");

    // A newer version 7 applies and replaces.
    let v7 = unique_named("rule_v7");
    assert!(engine.set_collection_constraints("users", 7, vec![v7.clone()]));
    let replaced = engine.constraints_for_collection("users");
    assert_eq!(replaced.len(), 1);
    assert_eq!(replaced[0].name, "rule_v7");
}

#[test]
fn drop_constraint_version_fence_rejects_stale_and_accepts_newer() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
    assert!(engine.set_collection_constraints("users", 5, vec![unique_named("rule_v5")]));

    // A drop at version 4 (older than the installed 5) is rejected; the
    // constraints survive.
    assert!(!engine.drop_collection_constraints("users", 4));
    assert_eq!(engine.constraints_for_collection("users").len(), 1);

    // A drop at version 6 applies and clears.
    assert!(engine.drop_collection_constraints("users", 6));
    assert_eq!(
        engine.constraints_for_collection("users"),
        Vec::<nodedb_crdt::Constraint>::new()
    );
}

#[test]
fn set_constraint_same_version_is_idempotent() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
    let rule = unique_named("rule_v5");

    // The same version delivered twice both apply (the `>=` fence) and leave a
    // single rule per name — re-delivery is harmless.
    assert!(engine.set_collection_constraints("users", 5, vec![rule.clone()]));
    assert!(engine.set_collection_constraints("users", 5, vec![rule.clone()]));
    let installed = engine.constraints_for_collection("users");
    assert_eq!(installed.len(), 1);
    assert_eq!(installed[0].name, "rule_v5");
}

#[test]
fn purge_clears_constraints_and_resets_version_fence() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();

    // Install a constraint at version 5, then drop the whole collection.
    assert!(engine.set_collection_constraints("users", 5, vec![unique_named("old_rule")]));
    engine.purge_collection("users").unwrap();

    // Purge clears the constraints outright.
    assert_eq!(
        engine.constraints_for_collection("users"),
        Vec::<nodedb_crdt::Constraint>::new()
    );

    // A re-created collection of the same name restarts its descriptor version
    // at 1. Because purge also reset the fence, that fresh low-version install
    // is accepted rather than rejected as stale against the dropped 5.
    assert!(engine.set_collection_constraints("users", 1, vec![unique_named("new_rule")]));
    let installed = engine.constraints_for_collection("users");
    assert_eq!(installed.len(), 1);
    assert_eq!(installed[0].name, "new_rule");
}
