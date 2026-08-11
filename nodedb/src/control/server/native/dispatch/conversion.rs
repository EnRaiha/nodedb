// SPDX-License-Identifier: BUSL-1.1

//! Shared conversion helpers for native protocol dispatch.

use nodedb_types::Value;
use nodedb_types::conversion::json_to_value_ref;
use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::Response;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::ddl::{DdlError, DdlResult};

/// Convert a Control-Plane error into a native error frame.
///
/// The stable numeric NodeDB code travels alongside the SQLSTATE, taken from
/// the one internal-to-public mapping table the crate owns. Without it the
/// frame reaches the client with `ndb_code == 0`, which the client can only
/// rebuild as a generic internal failure — so a planner's "collection does not
/// exist", an authorization refusal and a rate-limit rejection would all
/// answer `false` to `is_not_found()` / `is_auth_denied()` / `is_rate_exceeded()`.
/// The SQLSTATE is chosen here because it is a protocol-level rendering, while
/// the numeric code is the classification itself.
pub(crate) fn error_to_native(seq: u64, e: &crate::Error) -> NativeResponse {
    let (code, message) = match e {
        crate::Error::BadRequest { detail } => ("42601", detail.clone()),
        crate::Error::RejectedAuthz { resource, .. } => ("42501", resource.clone()),
        crate::Error::RateExceeded { .. } => (
            nodedb_types::error::sqlstate::TOO_MANY_CONNECTIONS,
            format!("{e}"),
        ),
        crate::Error::DeadlineExceeded { .. } => ("57014", "query cancelled due to timeout".into()),
        crate::Error::CollectionNotFound { collection, .. } => {
            ("42P01", format!("collection '{collection}' not found"))
        }
        // Same SQLSTATE as the "not authenticated" responses in
        // `session::request`: the client's stored bearer token expired
        // mid-connection and it must re-authenticate with a fresh Auth frame.
        crate::Error::SessionTokenExpired => (
            "28000",
            "OIDC bearer token expired; re-authenticate with a fresh Auth request".into(),
        ),
        // A cross-shard Calvin OCC abort is a serialization failure (40001) —
        // the client should retry the whole transaction.
        crate::Error::CalvinSerializationConflict => (
            nodedb_types::error::sqlstate::SERIALIZATION_FAILURE,
            format!("{e}"),
        ),
        other => ("XX000", format!("{other}")),
    };
    let ndb_code = crate::error_classify::classify(e).code().0;
    NativeResponse::error_with_code(seq, code, message, ndb_code)
}

/// Convert a `NodeDbError` produced while shaping a response into a
/// NativeResponse error frame.
///
/// The numeric code travels alongside the SQLSTATE: the error is already
/// classified here, and rendering only `XX000` would make the client rebuild
/// it as a generic internal failure.
pub(crate) fn shape_error_to_native(seq: u64, e: &nodedb_types::NodeDbError) -> NativeResponse {
    NativeResponse::error_with_code(seq, "XX000", e.message().to_string(), e.code().0)
}

/// Render an error [`Response`] from the Data Plane as a native error frame.
///
/// The Data Plane already classified the failure into a deterministic
/// `ErrorCode`; both the SQLSTATE and the message come from the same
/// protocol-neutral mapping pgwire uses (`error_code_to_sqlstate`), and the
/// stable numeric code comes from the one `ErrorCode` → `NodeDbError`
/// conversion the crate owns. Formatting the code with `{:?}` and stamping
/// `XX000` — which is what this path used to do — discarded that
/// classification, leaving a native client unable to tell a duplicate key
/// from a crashed database.
pub(crate) fn error_response_to_native(seq: u64, response: &Response) -> NativeResponse {
    let mut native = error_code_to_native(seq, response.error_code.as_deref());
    // A non-empty payload on an error response is a handler-rendered message
    // and is more specific than the mapping's generic rendering.
    if !response.payload.is_empty()
        && let Some(payload) = native.error.as_mut()
    {
        payload.message = String::from_utf8_lossy(&response.payload).into_owned();
    }
    native
}

/// Render a bare Data-Plane `ErrorCode` as a native error frame, for the
/// paths that carry the code without a full [`Response`] (a staging-gate
/// rejection). `None` means the Data Plane refused without classifying, which
/// is the only case that legitimately reaches the client as `XX000`.
pub(crate) fn error_code_to_native(
    seq: u64,
    code: Option<&crate::bridge::envelope::ErrorCode>,
) -> NativeResponse {
    let Some(code) = code else {
        return NativeResponse::error(seq, "XX000", "unknown data plane error");
    };
    let (_, sqlstate, message) = error_code_to_sqlstate(code);
    let public = nodedb_types::NodeDbError::from(crate::Error::DataPlane(code.clone()));
    NativeResponse::error_with_code(seq, sqlstate, message, public.code().0)
}

/// Encode a protocol-neutral DDL dispatch result into a single
/// `NativeResponse`.
///
/// Reduction mirrors the previous pgwire→native bridge: on error, an error
/// frame carrying the neutral SQLSTATE + message; otherwise the first
/// row-returning / status / empty result determines the response (a status tag
/// becomes a single-column status row, a row result becomes a columns+rows
/// frame, an empty result or an empty vec becomes a bare OK).
pub(crate) fn ddl_result_to_native(
    seq: u64,
    result: Result<Vec<DdlResult>, DdlError>,
) -> NativeResponse {
    match result {
        Err(DdlError { sqlstate, message }) => NativeResponse::error(seq, sqlstate, message),
        // Unknown pgwire response variants are dropped during translation, so
        // the first element is the first meaningful result — mirroring the
        // previous bridge, which returned on the first known variant.
        Ok(results) => match results.into_iter().next() {
            Some(DdlResult::Status { command, .. }) => NativeResponse::status_row(seq, command),
            Some(DdlResult::Rows(shaped)) => {
                let (columns, rows) = to_native_columns_rows(&shaped);
                NativeResponse {
                    seq,
                    status: nodedb_types::protocol::ResponseStatus::Ok,
                    columns: Some(columns),
                    rows: Some(rows),
                    rows_affected: None,
                    watermark_lsn: 0,
                    error: None,
                    auth: None,
                    warnings: Vec::new(),
                }
            }
            Some(DdlResult::Empty) | None => NativeResponse::ok(seq),
        },
    }
}

/// Build the native response for a completed Calvin transaction, surfacing
/// RETURNING rows when the write carried them.
///
/// `apply_result` is the applied Data-Plane response drained from the sidecar and
/// `plans` is the completed batch's plans, in dispatch order. A RETURNING plan
/// shapes the payload into native columns/rows; otherwise the batch's
/// count-bearing plan (if any) reports `rows_affected` READ FROM the applied
/// response.
///
/// There is deliberately no per-statement fallback count. The number of
/// dispatched tasks is not the number of affected rows — a single-row delete
/// dual-homed with its implicit edge cleanup dispatches two tasks and may affect
/// zero rows — so a batch whose count-bearing write reported nothing surfaces an
/// error rather than a plausible number.
pub(crate) fn calvin_native_response(
    seq: u64,
    apply_result: Option<crate::bridge::envelope::Response>,
    plans: &[crate::bridge::envelope::PhysicalPlan],
    state: &crate::control::state::SharedState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: nodedb_types::TenantId,
    auth: &crate::control::security::auth_context::AuthContext,
) -> NativeResponse {
    use crate::control::server::response_shape::compose::{
        ShapeOutcome, shape_response_materialized,
    };
    use crate::control::server::response_shape::redaction::QueryRedaction;
    use crate::control::server::response_shape::request::MaterializedShapeRequest;
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};

    let returning_plan = plans
        .iter()
        .find(|p| matches!(describe_plan(p), PlanKind::ReturningRows));
    let dml_plan = plans
        .iter()
        .find(|p| matches!(describe_plan(p), PlanKind::DmlResult(_)));

    let redaction = returning_plan.map(|plan| QueryRedaction::for_plan(tenant_id, auth, plan));
    if let (Some(resp), Some(plan)) = (apply_result.as_ref(), returning_plan)
        && matches!(describe_plan(plan), PlanKind::ReturningRows)
        && let Ok(ShapeOutcome::Rows(shaped)) =
            shape_response_materialized(MaterializedShapeRequest {
                payload: resp.payload.as_bytes(),
                plan,
                plan_kind: PlanKind::ReturningRows,
                projection: None,
                state,
                database_id,
                tenant_id,
                redaction: redaction.as_ref().map(|r| r.ctx(&state.redaction)),
            })
    {
        let (cols, rows) = to_native_columns_rows(&shaped);
        let mut r = NativeResponse::ok(seq);
        r.watermark_lsn = resp.watermark_lsn.as_u64();
        if !cols.is_empty() {
            r.columns = Some(cols);
        }
        r.rows = Some(rows);
        return r;
    }

    // Plain write: surface the affected count the mutation itself reported.
    let mut r = NativeResponse::ok(seq);
    if let Some(resp) = &apply_result {
        r.watermark_lsn = resp.watermark_lsn.as_u64();
    }
    // A batch with no count-bearing plan (pure graph / vector / DDL work) has no
    // row count to report, and says so by leaving `rows_affected` unset rather
    // than inventing one from the task count.
    if dml_plan.is_some() {
        let count = apply_result.as_ref().map_or_else(
            || {
                Err(crate::Error::Internal {
                    detail: "native Calvin write completed with no applied response to read its \
                             affected-row count from"
                        .to_owned(),
                })
            },
            |resp| {
                crate::control::server::shared::sql::staging_predicates::require_affected_count(
                    resp.payload.as_bytes(),
                )
            },
        );
        match count {
            Ok(n) => r.rows_affected = Some(n),
            Err(e) => return error_to_native(seq, &e),
        }
    }
    r
}

/// Convert protocol-neutral `ShapedRows` (produced by
/// `response_shape::compose::shape_response_materialized`) into native wire
/// columns/rows: each JSON cell becomes a typed `Value` via `json_to_value_ref`;
/// a column absent from a given row's map becomes `Value::Null`.
///
/// Structure is preserved all the way down, including nested objects and
/// arrays. The native protocol is MessagePack and `Value` has `Object` and
/// `Array` variants, so there is no format-level reason to render a nested
/// value as text — and doing so is lossy in a way the client cannot undo
/// reliably: a document field holding an object comes back as a `String` of
/// its JSON, so deserializing the row into the struct it was written from
/// fails with a type error, while a field that genuinely holds a JSON string
/// is indistinguishable from one that was flattened. Text rendering belongs
/// to pgwire, whose wire format is textual and which has its own
/// `json_value_to_text` for exactly that.
pub(crate) fn to_native_columns_rows(shaped: &ShapedRows) -> (Vec<String>, Vec<Vec<Value>>) {
    // Cells live in the row maps under per-column keys (display names may
    // repeat across columns, e.g. `SELECT w.id, b.id`), so read through the
    // shared accessor rather than by display name.
    let cell_keys = shaped.cell_keys();
    let rows = shaped
        .rows
        .iter()
        .map(|row| {
            cell_keys
                .iter()
                .map(|key| {
                    row.get(key.as_str())
                        .map(json_to_value_ref)
                        .unwrap_or(Value::Null)
                })
                .collect()
        })
        .collect();
    (shaped.columns.clone(), rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::ResponseStatus;

    /// A native session with a stored, now-expired OIDC token must surface
    /// an authentication-shaped error (SQLSTATE `28000`, the same class as
    /// "not authenticated") — not an internal error (`XX000`) — so the
    /// client can tell a stale token from a server fault and knows to
    /// re-authenticate rather than retry as-is.
    #[test]
    fn session_token_expired_maps_to_authentication_sqlstate() {
        let response = error_to_native(1, &crate::Error::SessionTokenExpired);

        assert_eq!(response.status, ResponseStatus::Error);
        let error = response
            .error
            .expect("error responses must carry a payload");
        assert_eq!(error.code, "28000");
    }

    /// A Control-Plane classification must ride the frame as the numeric code,
    /// not just as a SQLSTATE: the client rebuilds the typed error from the
    /// number, and a zero there collapses every planner / catalog refusal into
    /// a generic internal failure.
    #[test]
    fn control_plane_errors_carry_their_numeric_code() {
        let response = error_to_native(
            1,
            &crate::Error::CollectionNotFound {
                tenant_id: crate::types::TenantId::new(0),
                collection: "missing".to_owned(),
            },
        );

        let error = response
            .error
            .expect("error responses must carry a payload");
        assert_eq!(error.code, "42P01");
        assert_eq!(
            error.ndb_code,
            nodedb_types::error::ErrorCode::COLLECTION_NOT_FOUND.0
        );
    }

    /// The numeric code is populated for every variant, including the ones
    /// whose SQLSTATE falls through to `XX000` — otherwise the fix would be a
    /// per-variant special case rather than one classification.
    #[test]
    fn errors_without_a_dedicated_sqlstate_still_carry_a_code() {
        let response = error_to_native(
            1,
            &crate::Error::PlanError {
                detail: "no such column".to_owned(),
            },
        );

        let error = response
            .error
            .expect("error responses must carry a payload");
        assert_eq!(error.code, "XX000");
        assert_eq!(
            error.ndb_code,
            nodedb_types::error::ErrorCode::PLAN_ERROR.0,
            "an unmapped SQLSTATE must not also erase the numeric classification"
        );
    }
}
