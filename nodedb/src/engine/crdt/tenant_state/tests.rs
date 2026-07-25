// SPDX-License-Identifier: BUSL-1.1

use loro::LoroValue;

use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::policy::CollectionPolicy;
use nodedb_crdt::pre_validate::PreValidationResult;
use nodedb_crdt::validator::ProposedChange;

use crate::types::TenantId;

use super::core::TenantCrdtEngine;

#[test]
fn list_operations_stay_behind_engine_methods() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
    engine
        .doc_upsert(
            "pages",
            "one",
            &[("title", LoroValue::String("draft".into()))],
        )
        .unwrap();
    engine
        .list_insert_fields(
            "pages",
            "one",
            "blocks",
            0,
            &[("id".into(), LoroValue::String("block-0".into()))],
        )
        .unwrap();
    engine
        .list_insert_fields(
            "pages",
            "one",
            "blocks",
            1,
            &[("id".into(), LoroValue::String("block-1".into()))],
        )
        .unwrap();
    engine.list_move("pages", "one", "blocks", 1, 0).unwrap();

    assert_eq!(engine.list_length("pages", "one", "blocks").unwrap(), 2);
    let Some(LoroValue::Map(first)) = engine.list_get("pages", "one", "blocks", 0).unwrap() else {
        panic!("first list value must be a block map");
    };
    assert_eq!(first.get("id"), Some(&LoroValue::String("block-1".into())));
    engine.list_delete("pages", "one", "blocks", 0).unwrap();
    assert_eq!(engine.list_length("pages", "one", "blocks").unwrap(), 1);
}

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

// ── an apply that did not apply must not report success ──────────────────────

/// Build a peer document spanning two collections and export one incremental
/// delta per write, mirroring how an embedded client that keeps a single Loro
/// document for the whole database produces its deltas.
///
/// Returns `(first_delta_for_target, later_delta_for_target)` where the later
/// delta causally depends on an intervening write to the *other* collection.
fn interleaved_collection_deltas(peer: u64, target: &str, other: &str) -> (Vec<u8>, Vec<u8>) {
    let state = nodedb_crdt::state::CrdtState::new(peer).unwrap();

    let v0 = state.oplog_version_vector();
    state
        .upsert(target, "first", &[("v", LoroValue::I64(1))])
        .unwrap();
    let first = state.export_updates_since(&v0).unwrap();

    state
        .upsert(other, "aside", &[("v", LoroValue::I64(2))])
        .unwrap();

    let v2 = state.oplog_version_vector();
    state
        .upsert(target, "later", &[("v", LoroValue::I64(3))])
        .unwrap();
    let later = state.export_updates_since(&v2).unwrap();

    (first, later)
}

/// `apply_committed_delta` runs AFTER Raft consensus: the entry is already in
/// the log on every replica. If the import leaves its operations causally
/// pending, this replica's state silently diverges from a committed log entry
/// while returning `Ok` — the divergence is undetectable and permanent.
#[test]
fn raft_committed_apply_does_not_report_success_without_applying() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
    let (first, later) = interleaved_collection_deltas(31, "users", "signals");

    engine.apply_committed_delta("users", &first).unwrap();
    assert!(engine.row_exists("users", "first"));

    let result = engine.apply_committed_delta("users", &later);

    assert!(
        result.is_err() || engine.row_exists("users", "later"),
        "a Raft-committed apply reported success while its operations stayed \
         causally pending — this replica has silently diverged from the log"
    );
}

/// Snapshot restore must not report a completed restore when the blob's
/// operations could not be applied: the collection would come back partially
/// populated and be indistinguishable from a correct restore.
#[test]
fn snapshot_import_does_not_report_success_without_applying() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
    let (first, later) = interleaved_collection_deltas(32, "users", "signals");

    engine.import_snapshot_bytes("users", &first).unwrap();
    assert!(engine.row_exists("users", "first"));

    let result = engine.import_snapshot_bytes("users", &later);

    assert!(
        result.is_err() || engine.row_exists("users", "later"),
        "snapshot import reported a completed restore while its operations \
         stayed causally pending"
    );
}

/// Transaction rollback replaces a collection with an exact pre-image. If the
/// pre-image import leaves operations pending, rollback installs an incomplete
/// state and tells the transaction driver it succeeded.
#[test]
fn rollback_pre_image_does_not_report_success_without_applying() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
    let (first, later) = interleaved_collection_deltas(33, "users", "signals");

    engine
        .restore_collection_snapshot("users", Some(&first))
        .unwrap();
    assert!(engine.row_exists("users", "first"));

    let result = engine.restore_collection_snapshot("users", Some(&later));

    assert!(
        result.is_err() || engine.row_exists("users", "later"),
        "rollback reported success while the pre-image's operations stayed \
         causally pending — the transaction driver believes state was restored"
    );
}

/// Every collection's document is constructed with the same peer id, so Loro
/// operation ids are unique only *within* a collection. Two collections'
/// snapshots therefore carry colliding `(peer, counter)` identities, and a
/// consumer that merges them into one document — which is exactly how an
/// embedded client stores them — silently loses one side of the collision.
#[test]
fn collection_snapshots_carry_distinct_operation_identities() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();

    engine
        .doc_upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("Alice".into()))],
        )
        .unwrap();
    engine
        .doc_upsert(
            "orders",
            "o1",
            &[("item", LoroValue::String("book".into()))],
        )
        .unwrap();

    let users = engine
        .export_snapshot_bytes("users")
        .unwrap()
        .expect("users snapshot");
    let orders = engine
        .export_snapshot_bytes("orders")
        .unwrap()
        .expect("orders snapshot");

    // A single document holding both collections — the embedded-client shape.
    let merged = nodedb_crdt::state::CrdtState::new(99).unwrap();
    merged.import(&users).expect("import users snapshot");
    merged.import(&orders).expect("import orders snapshot");

    assert!(
        merged.row_exists("users", "u1"),
        "the users row was lost when both collections merged into one document"
    );
    assert!(
        merged.row_exists("orders", "o1"),
        "the orders row was lost when both collections merged into one document"
    );
}
