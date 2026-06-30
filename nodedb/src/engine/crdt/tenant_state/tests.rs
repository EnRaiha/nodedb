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
