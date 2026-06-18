// SPDX-License-Identifier: BUSL-1.1

//! CRDT delta application endpoint.
//!
//! POST /v1/collections/{name}/crdt/apply
//! Request: `{ "doc_id": "...", "delta": "hex_encoded_bytes" }`
//! Response: `{ "status": "ok", "collection": "...", "doc_id": "..." }`

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::http::auth::{ApiError, AppState, resolve_identity};
use crate::control::server::http::types::{HttpCrdtApplyRequest, HttpCrdtApplyResponse};
use crate::control::server::pgwire::types::hex_decode;
use nodedb_physical::physical_plan::CrdtOp;

use super::document::{dispatch_plan, extract_request_id};

/// POST /v1/collections/{name}/crdt/apply
///
/// Apply a CRDT delta to a document in the collection.
///
/// Request body:
/// ```json
/// {
///   "doc_id": "doc-1",
///   "delta": "hex_encoded_delta_bytes"
/// }
/// ```
pub async fn crdt_apply(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(collection): Path<String>,
    axum::Json(body): axum::Json<HttpCrdtApplyRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;

    // Decode delta from hex.
    let delta = hex_decode(&body.delta)
        .ok_or_else(|| ApiError::BadRequest("invalid hex in 'delta' field".into()))?;

    let _trace_id = extract_request_id(&headers);

    let surrogate = state
        .shared
        .surrogate_assigner
        .assign(
            crate::types::DatabaseId::DEFAULT,
            identity.tenant_id,
            &collection,
            body.doc_id.as_bytes(),
        )
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.clone(),
        document_id: body.doc_id.clone(),
        delta,
        peer_id: identity.user_id,
        mutation_id: 0,
        surrogate,
        provenance: None,
    });

    state.shared.tenant_request_start(identity.tenant_id);
    let result = dispatch_plan(&state, identity.tenant_id, &collection, plan).await;
    state.shared.tenant_request_end(identity.tenant_id);

    result?;

    Ok(axum::Json(HttpCrdtApplyResponse::ok(
        collection,
        body.doc_id,
    )))
}
