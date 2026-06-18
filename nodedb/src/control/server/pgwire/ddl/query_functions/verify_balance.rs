// SPDX-License-Identifier: BUSL-1.1

//! `SELECT VERIFY_BALANCE('collection', 'column')`
//!
//! Full integrity check: recomputes each materialized balance from source rows,
//! compares to the stored balance, and reports discrepancies.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};

use super::super::super::types::{sqlstate_error, text_field};
use super::helpers::clean_arg;

/// `SELECT VERIFY_BALANCE('collection', 'column')`
///
/// For each row in the target collection, recomputes the materialized sum
/// from all source rows and compares to the stored balance.
pub async fn verify_balance(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;
    let args = super::helpers::extract_function_args(sql, "VERIFY_BALANCE")?;
    if args.len() < 2 {
        return Err(sqlstate_error(
            "42601",
            "VERIFY_BALANCE requires (collection, column)",
        ));
    }

    let collection = clean_arg(args[0]);
    let column = clean_arg(args[1]);

    // Find the materialized sum definition.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };
    let coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &collection)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{collection}' not found")))?;

    let Some(mat_def) = coll
        .materialized_sums
        .iter()
        .find(|m| m.target_column == column)
    else {
        return Err(sqlstate_error(
            "42704",
            &format!("no MATERIALIZED_SUM defined for column '{column}' on '{collection}'"),
        ));
    };

    // Scan all target rows.
    let target_vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);
    let target_scan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
        collection: collection.clone(),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });
    let target_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        target_vshard,
        target_scan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("target scan failed: {e}")))?;
    let target_json =
        crate::data::executor::response_codec::decode_payload_to_json(&target_resp.payload);
    let target_docs: Vec<serde_json::Value> = sonic_rs::from_str(&target_json)
        .map_err(|e| sqlstate_error("22P02", &format!("invalid JSON in target scan: {e}")))?;

    // Scan all source rows.
    let source_vshard =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, &mat_def.source_collection);
    let source_scan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
        collection: mat_def.source_collection.clone(),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });
    let source_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        source_vshard,
        source_scan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("source scan failed: {e}")))?;
    let source_json =
        crate::data::executor::response_codec::decode_payload_to_json(&source_resp.payload);
    let source_docs: Vec<serde_json::Value> = sonic_rs::from_str(&source_json)
        .map_err(|e| sqlstate_error("22P02", &format!("invalid JSON in source scan: {e}")))?;

    // For each target row, recompute balance from source rows.
    let mut discrepancies = 0u64;
    let mut checked = 0u64;

    for target_doc in &target_docs {
        let doc_id = target_doc
            .get("id")
            .or_else(|| target_doc.get("_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if doc_id.is_empty() {
            continue;
        }

        let stored_balance = target_doc
            .get(&column)
            .and_then(super::helpers::json_to_decimal)
            .unwrap_or(rust_decimal::Decimal::ZERO);

        // Sum value_expr for all source rows where join_column == doc_id.
        let mut computed = rust_decimal::Decimal::ZERO;
        for src_doc in &source_docs {
            let join_val = src_doc.get(&mat_def.join_column).and_then(|v| v.as_str());
            if join_val != Some(doc_id) {
                continue;
            }
            let src_val = nodedb_types::Value::from(src_doc.clone());
            let delta = serde_json::Value::from(mat_def.value_expr.eval(&src_val));
            if let Some(d) = super::helpers::json_to_decimal(&delta) {
                computed += d;
            }
        }

        if stored_balance != computed {
            discrepancies += 1;
        }
        checked += 1;
    }

    let result = serde_json::json!({
        "collection": collection,
        "column": column,
        "rows_checked": checked,
        "discrepancies": discrepancies,
        "valid": discrepancies == 0,
    });

    let schema = Arc::new(vec![text_field("result")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&result.to_string())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(encoder.take_row())]),
    ))])
}
