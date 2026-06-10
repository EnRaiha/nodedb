// SPDX-License-Identifier: BUSL-1.1

//! Executor-level OLLP surrogate verification tests.
//!
//! These tests validate the optimistic lock-based predicate (OLLP) verification
//! path in `execute_bulk_update` and `execute_bulk_delete`. The executor compares
//! the `ollp_predicted_surrogates` embedded in the plan against the set of
//! document surrogates that actually match the predicate at admission time.
//!
//! Scenarios covered:
//!
//! 1. **No OLLP** (`ollp_predicted_surrogates: None`): the existing behaviour is
//!    preserved — bulk update and delete proceed without any surrogate check.
//!
//! 2. **Correct prediction**: `ollp_predicted_surrogates` matches the actual
//!    matching set — the write proceeds and returns `Ok`.
//!
//! 3. **Stale prediction (race)**: a document was inserted between the pre-exec
//!    scan and admission, so the predicted set is smaller than the actual set.
//!    The executor returns `ErrorCode::OllpRetryRequired` WITHOUT writing.
//!
//! 4. **Retry with corrected prediction**: after receiving `OllpRetryRequired`,
//!    the caller re-scans and re-submits with the corrected surrogate set.
//!    The executor accepts and writes.
//!
//! The "race" is simulated synchronously: insert a document after recording the
//! predicted surrogates but before the bulk operation. The executor sees the
//! mismatch because it scans live storage at admission time.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb::bridge::scan_filter::ScanFilter;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, UpdateValue};

use crate::helpers::*;

// ── helpers ────────────────────────────────────────────────────────────────

const COLLECTION: &str = "ollp_items";

fn filter_active() -> Vec<u8> {
    let f = ScanFilter {
        field: "active".into(),
        op: "eq".into(),
        value: nodedb_types::Value::Bool(true),
        clauses: Vec::new(),
        expr: None,
    };
    zerompk::to_msgpack_vec(&vec![f]).unwrap()
}

/// Deterministic surrogate for a string ID — same formula as other test files.
fn surrogate_for(id: &str) -> nodedb_types::Surrogate {
    let mut h: u32 = 2_166_136_261;
    for &b in id.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16_777_619);
    }
    nodedb_types::Surrogate::new(h.max(1))
}

/// The u32 value of a surrogate — what the OLLP comparison operates on.
fn surrogate_u32(id: &str) -> u32 {
    surrogate_for(id).as_u32()
}

/// Insert a document with `active: true`.
fn insert_active(ctx: &mut TestCtx, id: &str) {
    let value = format!(r#"{{"active":true,"name":"{id}"}}"#);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: COLLECTION.into(),
            document_id: id.into(),
            value: value.into_bytes(),
            surrogate: surrogate_for(id),
            pk_bytes: id.as_bytes().to_vec(),
        }),
    );
}

/// Build a BulkUpdate plan that sets `name = "updated"` for all `active = true` docs.
fn bulk_update_plan(predicted: Option<Vec<u32>>) -> PhysicalPlan {
    let updates = vec![(
        "name".to_string(),
        UpdateValue::Literal(nodedb_types::json_to_msgpack(&serde_json::json!("updated")).unwrap()),
    )];
    PhysicalPlan::Document(DocumentOp::BulkUpdate {
        collection: COLLECTION.into(),
        filters: filter_active(),
        updates,
        returning: None,
        ollp_predicted_surrogates: predicted,
    })
}

/// Build a BulkDelete plan that deletes all `active = true` docs.
fn bulk_delete_plan(predicted: Option<Vec<u32>>) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::BulkDelete {
        collection: COLLECTION.into(),
        filters: filter_active(),
        returning: None,
        ollp_predicted_surrogates: predicted,
    })
}

// ── tests ──────────────────────────────────────────────────────────────────

/// BulkUpdate without OLLP (`predicted = None`) continues to work normally.
#[test]
fn bulk_update_no_ollp_proceeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "a");
    insert_active(&mut ctx, "b");

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(None),
    );
    assert_eq!(resp.status, Status::Ok, "no-OLLP BulkUpdate should succeed");
}

/// BulkDelete without OLLP continues to work normally.
#[test]
fn bulk_delete_no_ollp_proceeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "a");
    insert_active(&mut ctx, "b");

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan(None),
    );
    assert_eq!(resp.status, Status::Ok, "no-OLLP BulkDelete should succeed");
}

/// BulkUpdate with a correct prediction succeeds.
#[test]
fn bulk_update_correct_prediction_succeeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "x1");
    insert_active(&mut ctx, "x2");

    // Pre-exec scan would have returned exactly these two surrogates.
    let predicted = vec![surrogate_u32("x1"), surrogate_u32("x2")];
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(predicted)),
    );
    assert_eq!(
        resp.status,
        Status::Ok,
        "BulkUpdate with correct prediction should succeed"
    );
}

/// BulkDelete with a correct prediction succeeds.
#[test]
fn bulk_delete_correct_prediction_succeeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "y1");
    insert_active(&mut ctx, "y2");

    let predicted = vec![surrogate_u32("y1"), surrogate_u32("y2")];
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan(Some(predicted)),
    );
    assert_eq!(
        resp.status,
        Status::Ok,
        "BulkDelete with correct prediction should succeed"
    );
}

/// BulkUpdate with a stale prediction (concurrent insert raced) returns
/// OllpRetryRequired WITHOUT writing.
///
/// Scenario:
/// 1. Pre-exec scan sees {z1, z2} → predicted = [z1, z2].
/// 2. A concurrent insert adds z3 (active=true).
/// 3. BulkUpdate with predicted=[z1, z2] is admitted.
/// 4. Executor scans and finds {z1, z2, z3} — mismatch → OllpRetryRequired.
/// 5. The z1/z2 values are NOT updated.
#[test]
fn bulk_update_stale_prediction_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "z1");
    insert_active(&mut ctx, "z2");

    // Simulate: pre-exec captured [z1, z2] as predicted surrogates.
    let predicted = vec![surrogate_u32("z1"), surrogate_u32("z2")];

    // Concurrent insert: z3 lands after the pre-exec scan but before admission.
    insert_active(&mut ctx, "z3");

    // Submit BulkUpdate with the stale predicted set.
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(predicted)),
    );

    assert_eq!(
        resp.status,
        Status::Error,
        "stale prediction should produce Status::Error"
    );
    assert_eq!(
        resp.error_code,
        Some(ErrorCode::OllpRetryRequired),
        "error code must be OllpRetryRequired, got {:?}",
        resp.error_code
    );

    // Verify no write occurred: z1 should still have name="z1", not "updated".
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: COLLECTION.into(),
            document_id: "z1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: surrogate_for("z1"),
            pk_bytes: "z1".as_bytes().to_vec(),
        }),
    );
    let val = payload_value(&payload);
    let name = val.get("name").and_then(|n| n.as_str()).unwrap_or_default();
    assert_eq!(
        name, "z1",
        "OllpRetryRequired must not have modified the document"
    );
}

/// BulkDelete with a stale prediction returns OllpRetryRequired WITHOUT deleting.
#[test]
fn bulk_delete_stale_prediction_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "d1");
    insert_active(&mut ctx, "d2");

    let predicted = vec![surrogate_u32("d1"), surrogate_u32("d2")];

    // Concurrent insert adds d3.
    insert_active(&mut ctx, "d3");

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan(Some(predicted)),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(resp.error_code, Some(ErrorCode::OllpRetryRequired));
}

/// After OllpRetryRequired, the caller re-scans and retries with the corrected
/// predicted set. The second attempt succeeds and all three docs are updated.
#[test]
fn bulk_update_retry_with_corrected_prediction_succeeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "r1");
    insert_active(&mut ctx, "r2");

    // First attempt: stale prediction [r1, r2] — r3 was concurrently inserted.
    let stale_predicted = vec![surrogate_u32("r1"), surrogate_u32("r2")];
    insert_active(&mut ctx, "r3");

    let first_resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(stale_predicted)),
    );
    assert_eq!(first_resp.error_code, Some(ErrorCode::OllpRetryRequired));

    // Retry: re-scan sees {r1, r2, r3} → corrected prediction.
    let corrected_predicted = vec![
        surrogate_u32("r1"),
        surrogate_u32("r2"),
        surrogate_u32("r3"),
    ];
    let retry_resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(corrected_predicted)),
    );

    assert_eq!(
        retry_resp.status,
        Status::Ok,
        "retry with corrected prediction must succeed"
    );

    // Verify the write went through for all three docs.
    for id in ["r1", "r2", "r3"] {
        let payload = send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection: COLLECTION.into(),
                document_id: id.into(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                surrogate: surrogate_for(id),
                pk_bytes: id.as_bytes().to_vec(),
            }),
        );
        let val = payload_value(&payload);
        let name = val.get("name").and_then(|n| n.as_str()).unwrap_or_default();
        assert_eq!(
            name, "updated",
            "doc {id} should have been updated on retry"
        );
    }
}

/// Prediction that is a superset of the actual set (a document was deleted
/// concurrently) also triggers OllpRetryRequired.
#[test]
fn bulk_update_superset_prediction_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "s1");
    insert_active(&mut ctx, "s2");

    // Pre-exec captured [s1, s2] but s2 was deleted before admission.
    // Delete s2 to simulate the concurrent delete.
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: COLLECTION.into(),
            document_id: "s2".into(),
            surrogate: surrogate_for("s2"),
            pk_bytes: "s2".as_bytes().to_vec(),
            returning: None,
        }),
    );

    // Submit with the now-stale superset prediction.
    let stale_predicted = vec![surrogate_u32("s1"), surrogate_u32("s2")];
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(stale_predicted)),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(resp.error_code, Some(ErrorCode::OllpRetryRequired));
}
