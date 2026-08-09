// SPDX-License-Identifier: BUSL-1.1

//! Cross-engine transaction rollback matrix: index side-effects.
//!
//! A point put drives FTS and spatial index maintenance as a side-effect of
//! the document write. Rolling the write back must roll those back too —
//! a posting or an R-tree entry that survives a failed batch is a row that
//! searches can still find but reads cannot.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{DocumentOp, MetaOp, PhysicalPlan, TextOp};

use crate::helpers::*;
use crate::test_transaction_matrix_helpers::*;

// ---------------------------------------------------------------------------
// FTS side-effect rollback: doc with text fields, batch fails, FTS is clean
//
// When tx_point_put runs, it calls `inverted.index_document` as a side-effect.
// When the batch fails and the PutDocument undo entry is applied, `apply_undo_document`
// calls `inverted.remove_document` to revert the posting. This test proves
// that the FTS posting does NOT surface in a subsequent search after rollback.
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_fts_side_effect_rolled_back() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed the vector index so we have a deterministic failure trigger.
    send_ok(&mut core, &mut tx, &mut rx, vector_set_params("vec"));
    send_ok(&mut core, &mut tx, &mut rx, vector_seed("vec"));

    // TransactionBatch:
    //   plan 0: PointPut a document with a text "title" field (triggers FTS index)
    //   plan 1: vector insert with dim-mismatch (always fails)
    let doc_value = r#"{"title":"unique_rollback_sentinel quantum database"}"#;
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            txn_id: None,
            plans: vec![
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "articles".into(),
                    document_id: "fts_rollback_doc".into(),
                    value: doc_value.as_bytes().to_vec(),
                    surrogate: nodedb_types::Surrogate::new(7001),
                    pk_bytes: b"fts_rollback_doc".to_vec(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
                vector_fail("vec"),
            ],
        }),
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "batch must fail on dim-mismatch"
    );
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RollbackFailed { .. })
        ),
        "rollback itself must succeed; got {:?}",
        resp.error_code
    );

    // FTS search for the sentinel term must return zero results — the posting
    // was removed by apply_undo_document → inverted.remove_document.
    let search_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Text(TextOp::Search {
            collection: "articles".into(),
            query: "unique_rollback_sentinel".into(),
            top_k: 10,
            fuzzy: false,
            rls_filters: Vec::new(),
            prefilter: None,
        }),
    );
    assert_eq!(search_resp.status, Status::Ok);
    let json = crate::helpers::payload_json(&search_resp.payload);
    let val: serde_json::Value =
        serde_json::from_str(&json).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let arr = val.as_array().unwrap_or(&empty);
    assert!(
        arr.is_empty(),
        "FTS posting for rolled-back doc must not appear in search results; got {json}"
    );
}

// ---------------------------------------------------------------------------
// Spatial side-effect NOT written in tx path — confirmed by test
//
// The transactional PointPut path (tx_point_put) writes to sparse + inverted
// only. It does NOT call apply_point_put, so the spatial R-tree is never
// touched during a transaction. This test proves that after a failed batch
// containing a PointPut with a geometry field, a spatial scan returns zero
// results — confirming no stale R-tree entry was left.
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_spatial_not_written_in_tx_path() {
    use nodedb_physical::physical_plan::{SpatialOp, SpatialPredicate};
    use nodedb_types::geometry::Geometry;

    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed vector index for the failing second op.
    send_ok(&mut core, &mut tx, &mut rx, vector_set_params("vec"));
    send_ok(&mut core, &mut tx, &mut rx, vector_seed("vec"));

    // TransactionBatch:
    //   plan 0: PointPut a doc with a GeoJSON geometry field
    //   plan 1: vector insert with dim-mismatch (always fails)
    let geo_doc = r#"{"name":"poi","location":{"type":"Point","coordinates":[10.0,20.0]}}"#;
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            txn_id: None,
            plans: vec![
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "places".into(),
                    document_id: "geo_rollback_doc".into(),
                    value: geo_doc.as_bytes().to_vec(),
                    surrogate: nodedb_types::Surrogate::new(8001),
                    pk_bytes: b"geo_rollback_doc".to_vec(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
                vector_fail("vec"),
            ],
        }),
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "batch must fail on dim-mismatch"
    );
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RollbackFailed { .. })
        ),
        "rollback itself must succeed; got {:?}",
        resp.error_code
    );

    // Spatial scan for a wide bounding box must return zero results.
    // (The R-tree was never written since tx_point_put bypasses apply_point_put.)
    let query_geometry = Geometry::Point {
        coordinates: [10.0, 20.0],
    };
    let scan_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Spatial(SpatialOp::Scan {
            collection: "places".into(),
            field: "location".into(),
            predicate: SpatialPredicate::DWithin,
            query_geometry,
            distance_meters: 1_000_000.0,
            attribute_filters: Vec::new(),
            limit: 10,
            projection: Vec::new(),
            rls_filters: Vec::new(),
            prefilter: None,
        }),
    );
    assert_eq!(scan_resp.status, Status::Ok);
    let json = crate::helpers::payload_json(&scan_resp.payload);
    let val: serde_json::Value =
        serde_json::from_str(&json).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let arr = val.as_array().unwrap_or(&empty);
    assert!(
        arr.is_empty(),
        "spatial scan after rollback must return zero results (tx path never writes R-tree); \
         got {json}"
    );
}

// ---------------------------------------------------------------------------

#[test]
fn rollback_failed_error_code_is_typed() {
    // Construct the error code and verify it's distinguishable.
    let code = ErrorCode::RollbackFailed {
        entry_index: 2,
        detail: "sparse store error: disk full".into(),
    };
    assert!(
        matches!(code, ErrorCode::RollbackFailed { entry_index: 2, .. }),
        "RollbackFailed must carry structured fields"
    );
}
