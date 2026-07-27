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
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::Permission;
use crate::control::server::http::auth::{ApiError, AppState, resolve_identity};
use crate::control::server::http::types::{HttpCrdtApplyRequest, HttpCrdtApplyResponse};
use crate::control::server::shared::authorization::{authorize_collection, authorize_task_set};
use crate::control::server::shared::ddl::sql_parse::hex_decode;
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::document::extract_request_id;

/// Maximum encoded JSON request body for one CRDT apply request.
pub const CRDT_HTTP_BODY_MAX_BYTES: usize = 2 * nodedb_crdt::DEFAULT_MAX_DELTA_BYTES + 4096;

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

    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.shared.audit));
    authorize_collection(
        &identity,
        crate::types::DatabaseId::DEFAULT,
        &collection,
        Permission::Write,
        &state.shared.permissions,
        &state.shared.roles,
        &audit,
    )
    .map_err(crate::Error::from)
    .map_err(ApiError::from)?;

    // Decode and bound the external delta before allocating identity state.
    let delta = decode_bounded_delta(&body.delta)?;

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
        // Local HTTP write, not a replicated peer sync — no constraint fence.
        constraint_version_required: 0,
        expected_frontier_digest: None,
    });

    let task = PhysicalTask {
        tenant_id: identity.tenant_id,
        vshard_id: crate::types::VShardId::from_collection_in_database(
            crate::types::DatabaseId::DEFAULT,
            &collection,
        ),
        database_id: crate::types::DatabaseId::DEFAULT,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let authorized = authorize_task_set(
        &identity,
        std::slice::from_ref(&task),
        &state.shared.permissions,
        &state.shared.roles,
        &audit,
    )
    .map_err(crate::Error::from)
    .map_err(ApiError::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| ApiError::Internal("authorization returned no capability".into()))?;

    // Route through the Raft proposer gate so the delta is quorum-durable under
    // replication. A local-only dispatch would land it on the receiving node only
    // — lost to followers and entirely on leader failover. This handler is scoped
    // to the default database (matching its surrogate assignment above).
    let _request = state.shared.tenant_request_guard(identity.tenant_id);
    let policy = crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy::from_identity(
        identity.tenant_id,
        crate::types::DatabaseId::DEFAULT,
        &collection,
        &identity,
        "http".into(),
        &state.shared.rls,
        &audit,
    );
    let result = crate::control::crdt_admission::dispatch_authorized_crdt_apply_admitted(
        &state.shared,
        crate::control::crdt_admission::AuthorizedCrdtApplyAdmissionRequest {
            authorized,
            collection: &collection,
            timeout: std::time::Duration::from_secs(
                state.shared.tuning.network.default_deadline_secs,
            ),
            event_source: crate::event::EventSource::User,
            policy: &policy,
        },
    )
    .await;

    result.map_err(ApiError::from)?;

    Ok(axum::Json(HttpCrdtApplyResponse::ok(
        collection,
        body.doc_id,
    )))
}

fn decode_bounded_delta(encoded: &str) -> Result<Vec<u8>, ApiError> {
    let delta = hex_decode(encoded)
        .ok_or_else(|| ApiError::BadRequest("invalid hex in 'delta' field".into()))?;
    if delta.len() > nodedb_crdt::DEFAULT_MAX_DELTA_BYTES {
        return Err(ApiError::HttpStatus(
            413,
            "CRDT delta exceeds maximum size".into(),
        ));
    }
    Ok(delta)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoded_delta_limit_is_exact_and_invalid_hex_is_bad_request() {
        let exact = "00".repeat(nodedb_crdt::DEFAULT_MAX_DELTA_BYTES);
        assert_eq!(
            decode_bounded_delta(&exact).expect("exact limit").len(),
            nodedb_crdt::DEFAULT_MAX_DELTA_BYTES
        );

        let oversized = "00".repeat(nodedb_crdt::DEFAULT_MAX_DELTA_BYTES + 1);
        assert!(matches!(
            decode_bounded_delta(&oversized),
            Err(ApiError::HttpStatus(413, _))
        ));
        assert!(matches!(
            decode_bounded_delta("xyz"),
            Err(ApiError::BadRequest(_))
        ));
    }
}
