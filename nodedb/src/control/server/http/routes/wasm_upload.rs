// SPDX-License-Identifier: BUSL-1.1

//! HTTP endpoint: `PUT /v1/functions/{name}/wasm`
//!
//! Accepts a raw WASM binary as the request body, stores it content-addressed,
//! and updates the function's `wasm_hash` in the catalog.
//!
//! Auth: tenant_id is resolved via `resolve_identity` before any catalog
//! access, ensuring the caller's identity — not a hardcoded literal — governs
//! which tenant's function catalog is written to.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use super::super::auth::{AppState, resolve_identity};
use crate::control::planner::wasm;

/// `PUT /v1/functions/:name/wasm` — upload a WASM binary for a function.
///
/// The function must already exist with `language = WASM` in the catalog.
/// The binary replaces the previous one (if any) atomically.
pub async fn upload_wasm(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Resolve identity before any catalog access. This is the source of
    // tenant_id — never a hardcoded literal or a request parameter.
    let identity = match resolve_identity(&headers, &state, "http") {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    let name = name.to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state.shared.credentials.catalog();

    // Verify the function exists and is a WASM function.
    let mut func = match catalog.get_function(tenant_id, &name) {
        Ok(Some(f)) => f,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                format!("function '{name}' does not exist"),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("catalog read error: {e}"),
            )
                .into_response();
        }
    };

    if func.language != crate::control::security::catalog::function_types::FunctionLanguage::Wasm {
        return (
            StatusCode::BAD_REQUEST,
            format!("function '{name}' is not a WASM function"),
        )
            .into_response();
    }

    // Store the binary.
    let config = wasm::WasmConfig::default();
    let hash = match wasm::store::store_wasm_binary(catalog, &body, config.max_binary_size) {
        Ok(h) => h,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid WASM binary: {e}")).into_response();
        }
    };

    // Update function metadata.
    func.wasm_hash = Some(hash.clone());
    if let Err(e) = catalog.put_function(&func) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("catalog write error: {e}"),
        )
            .into_response();
    }

    state.shared.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        None,
        "_http_upload",
        &format!("WASM binary uploaded for function '{name}' (hash: {hash})"),
    );

    (
        StatusCode::OK,
        serde_json::json!({"hash": hash}).to_string(),
    )
        .into_response()
}
