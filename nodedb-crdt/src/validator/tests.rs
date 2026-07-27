// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::CrdtAuthContext;
use crate::constraint::ConstraintSet;
use crate::dead_letter::CompensationHint;
use crate::error::CrdtError;
use crate::policy::{PolicyResolution, ResolvedAction};
use crate::state::CrdtState;
use loro::LoroValue;

fn setup() -> (CrdtState, ConstraintSet) {
    let state = CrdtState::new(1).unwrap();
    let mut cs = ConstraintSet::new();
    cs.add_unique("users_email_unique", "users", "email");
    cs.add_not_null("users_name_nn", "users", "name");
    cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
    (state, cs)
}

#[test]
fn valid_insert_accepted() {
    let (state, cs) = setup();
    let validator = Validator::new(cs, 100);

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

    assert!(matches!(
        validator.validate(&state, &change),
        ValidationOutcome::Accepted
    ));
}

#[test]
fn not_null_violation() {
    let (_state, cs) = setup();
    let validator = Validator::new(cs, 100);
    let state = CrdtState::new(1).unwrap();

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![(
            "email".into(),
            LoroValue::String("alice@example.com".into()),
        )],
    };

    match validator.validate(&state, &change) {
        ValidationOutcome::Rejected(v) => {
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].constraint_name, "users_name_nn");
            assert!(matches!(
                v[0].hint,
                CompensationHint::ProvideRequiredField { .. }
            ));
        }
        _ => panic!("expected rejection"),
    }
}

#[test]
fn foreign_key_violation() {
    let (state, cs) = setup();
    let validator = Validator::new(cs, 100);

    let change = ProposedChange {
        collection: "posts".into(),
        row_id: "p1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
    };

    match validator.validate(&state, &change) {
        ValidationOutcome::Rejected(v) => {
            assert_eq!(v[0].constraint_name, "posts_author_fk");
            assert!(matches!(
                v[0].hint,
                CompensationHint::CreateReferencedRow { .. }
            ));
        }
        _ => panic!("expected FK violation"),
    }
}

#[test]
fn foreign_key_passes_when_parent_exists() {
    let (state, cs) = setup();
    let validator = Validator::new(cs, 100);

    state
        .upsert(
            "users",
            "u1",
            &[
                ("name", LoroValue::String("Alice".into())),
                ("email", LoroValue::String("a@b.com".into())),
            ],
        )
        .unwrap();

    let change = ProposedChange {
        collection: "posts".into(),
        row_id: "p1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
    };

    assert!(matches!(
        validator.validate(&state, &change),
        ValidationOutcome::Accepted
    ));
}

#[test]
fn validate_or_reject_enqueues_to_dlq() {
    let (state, cs) = setup();
    let policies = crate::policy::PolicyRegistry::new();
    let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

    let strict_policy = crate::policy::CollectionPolicy::strict();
    validator.policies_mut().set("users", strict_policy);

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
    };

    let err = validator
        .validate_or_reject(
            &state,
            42,
            CrdtAuthContext::default(),
            &change,
            b"delta-bytes".to_vec(),
        )
        .unwrap_err();

    assert!(matches!(err, CrdtError::ConstraintViolation { .. }));
    assert_eq!(validator.dlq().len(), 1);

    let dl = validator.dlq().peek().unwrap();
    assert_eq!(dl.peer_id, 42);
    assert_eq!(dl.violated_constraint, "users_name_nn");
}

#[test]
fn validate_with_policy_last_writer_wins() {
    let (state, cs) = setup();
    let mut policies = crate::policy::PolicyRegistry::new();
    policies.set("users", crate::policy::CollectionPolicy::ephemeral());

    let mut policy = crate::policy::CollectionPolicy::ephemeral();
    policy.not_null = crate::policy::ConflictPolicy::LastWriterWins;
    policies.set("users", policy);

    let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let resolution = validator
        .validate_with_policy(
            &state,
            42,
            CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
            now,
        )
        .unwrap();

    assert!(matches!(
        resolution,
        PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
    ));
}

#[test]
fn validate_with_policy_rename_suffix() {
    let (state, cs) = setup();
    let mut policies = crate::policy::PolicyRegistry::new();
    let mut policy = crate::policy::CollectionPolicy::ephemeral();
    policy.unique = crate::policy::ConflictPolicy::RenameSuffix;
    policies.set("users", policy);

    let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

    state
        .upsert(
            "users",
            "u1",
            &[
                ("name", LoroValue::String("Alice".into())),
                ("email", LoroValue::String("alice@example.com".into())),
            ],
        )
        .unwrap();

    let change = ProposedChange {
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

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let resolution = validator
        .validate_with_policy(
            &state,
            42,
            CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
            now,
        )
        .unwrap();

    match resolution {
        PolicyResolution::AutoResolved(ResolvedAction::RenamedField { field, new_value }) => {
            assert_eq!(field, "email");
            assert!(new_value.contains("_1"));
        }
        _ => panic!("expected RenamedField resolution"),
    }
}

#[test]
fn validate_with_policy_cascade_defer() {
    let (state, cs) = setup();
    let mut policies = crate::policy::PolicyRegistry::new();
    let mut policy = crate::policy::CollectionPolicy::ephemeral();
    policy.foreign_key = crate::policy::ConflictPolicy::CascadeDefer {
        max_retries: 3,
        ttl_secs: 60,
    };
    policies.set("posts", policy);

    let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

    let change = ProposedChange {
        collection: "posts".into(),
        row_id: "p1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let resolution = validator
        .validate_with_policy(
            &state,
            42,
            CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
            now,
        )
        .unwrap();

    match resolution {
        PolicyResolution::Deferred {
            retry_after_ms,
            attempt,
            ..
        } => {
            assert_eq!(attempt, 0);
            assert_eq!(retry_after_ms, 500);
        }
        _ => panic!("expected Deferred resolution"),
    }

    assert_eq!(validator.deferred().len(), 1);
}

#[test]
fn validate_with_policy_escalate_to_dlq() {
    let (state, cs) = setup();
    let mut policies = crate::policy::PolicyRegistry::new();
    let mut policy = crate::policy::CollectionPolicy::ephemeral();
    policy.not_null = crate::policy::ConflictPolicy::EscalateToDlq;
    policies.set("users", policy);

    let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let resolution = validator
        .validate_with_policy(
            &state,
            42,
            CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
            now,
        )
        .unwrap();

    assert!(matches!(resolution, PolicyResolution::Escalate { .. }));
    assert_eq!(validator.dlq().len(), 1);
}

#[test]
fn check_constraint_passes_when_predicate_true() {
    let state = CrdtState::new(1).unwrap();
    let mut cs = ConstraintSet::new();
    cs.add_check("people_age_check", "people", "age", "age > 0", "age > 0");
    let validator = Validator::new(cs, 100);

    let change = ProposedChange {
        collection: "people".into(),
        row_id: "p1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("age".into(), LoroValue::I64(5))],
    };

    assert!(matches!(
        validator.validate(&state, &change),
        ValidationOutcome::Accepted
    ));
}

#[test]
fn check_constraint_rejects_when_predicate_false() {
    let state = CrdtState::new(1).unwrap();
    let mut cs = ConstraintSet::new();
    cs.add_check("people_age_check", "people", "age", "age > 0", "age > 0");
    let validator = Validator::new(cs, 100);

    let change = ProposedChange {
        collection: "people".into(),
        row_id: "p1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("age".into(), LoroValue::I64(-1))],
    };

    match validator.validate(&state, &change) {
        ValidationOutcome::Rejected(v) => {
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].constraint_name, "people_age_check");
            assert!(matches!(
                v[0].hint,
                CompensationHint::ManualIntervention { .. }
            ));
        }
        _ => panic!("expected rejection"),
    }
}

#[test]
fn check_constraint_rejects_malformed_expr() {
    let state = CrdtState::new(1).unwrap();
    let mut cs = ConstraintSet::new();
    cs.add_check("people_age_check", "people", "age", "age >", "age >");
    let validator = Validator::new(cs, 100);

    let change = ProposedChange {
        collection: "people".into(),
        row_id: "p1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("age".into(), LoroValue::I64(5))],
    };

    match validator.validate(&state, &change) {
        ValidationOutcome::Rejected(v) => {
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].constraint_name, "people_age_check");
        }
        _ => panic!("expected rejection for malformed CHECK expression"),
    }
}

/// A CHECK predicate that divides by zero is an *evaluation* error, not an
/// ordinary violation: `validate` returns `ValidationOutcome::EvalError` so the
/// downstream conflict-policy machinery never "resolves" an unevaluable
/// predicate. Distinct from both `Rejected` (a genuine violation) and
/// `Accepted`.
#[test]
fn check_constraint_division_by_zero_surfaces_eval_error() {
    let state = CrdtState::new(1).unwrap();
    let mut cs = ConstraintSet::new();
    // `100 / age` is unevaluable when age = 0.
    cs.add_check(
        "people_ratio_check",
        "people",
        "age",
        "100 / age > 0",
        "100 / age > 0",
    );
    let validator = Validator::new(cs, 100);

    let change = ProposedChange {
        collection: "people".into(),
        row_id: "p1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("age".into(), LoroValue::I64(0))],
    };

    match validator.validate(&state, &change) {
        ValidationOutcome::EvalError {
            constraint_name,
            error,
        } => {
            assert_eq!(constraint_name, "people_ratio_check");
            assert_eq!(error, nodedb_query::EvalError::DivisionByZero);
        }
        other => panic!("expected ValidationOutcome::EvalError, got {other:?}"),
    }
}
