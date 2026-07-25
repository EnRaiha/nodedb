// SPDX-License-Identifier: BUSL-1.1

//! Gateway-based SQL task dispatch for the native protocol.
//!
//! When `SharedState.gateway` is `Some`, tasks are routed through
//! `Gateway::execute` which handles cluster-aware routing, typed `NotLeader`
//! retry, and plan caching. The `None` fallback retains the original
//! `dispatch_to_data_plane` path for single-node boot before the gateway is
//! wired.

use crate::bridge::envelope::{Payload, Response, Status};
use std::sync::Arc;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::types::{Lsn, RequestId, TraceId};
use nodedb_physical::physical_task::PhysicalTask;

use super::DispatchCtx;

pub(super) fn authorize_native_task(
    ctx: &DispatchCtx<'_>,
    task: &PhysicalTask,
) -> crate::Result<crate::control::server::shared::authorization::AuthorizedTask> {
    let emitter = crate::control::security::audit::ArcAuditEmitter(Arc::clone(&ctx.state.audit));
    crate::control::server::shared::authorization::authorize_task_set(
        ctx.identity,
        std::slice::from_ref(task),
        &ctx.state.permissions,
        &ctx.state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "authorization returned an empty capability set".into(),
    })
}

/// Dispatch a single `PhysicalTask` through the gateway when available,
/// falling back to the local SPSC path.
///
/// Returns a synthetic `Response` shaped identically to the SPSC path so that
/// the calling code in `sql.rs` is unchanged.
pub(super) async fn dispatch_task_via_gateway(
    ctx: &DispatchCtx<'_>,
    task: PhysicalTask,
) -> crate::Result<Response> {
    let authorized = authorize_native_task(ctx, &task)?;
    let tenant_id = authorized.tenant_id();
    let database_id = authorized.database_id();
    let txn_id = authorized.txn_id();

    match ctx.state.gateway.get() {
        Some(gw) => {
            let gw_ctx = GatewayQueryContext {
                tenant_id,
                trace_id: TraceId::generate(),
                database_id,
                // Propagate the in-block transaction id so gateway local
                // dispatch resolves the per-txn staging overlay.
                txn_id,
            };
            gw.execute(&gw_ctx, authorized)
                .await
                .map_err(|e| {
                    let (code, msg) = GatewayErrorMap::to_native(&e);
                    crate::Error::Internal {
                        detail: format!("gateway error {code}: {msg}"),
                    }
                })
                .map(payloads_to_response)
        }
        None => {
            crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                ctx.state,
                authorized,
                TraceId::generate(),
            )
            .await
        }
    }
}

/// Convert gateway `Vec<Vec<u8>>` payloads into a synthetic `Response`.
///
/// Mirrors the same conversion used in the RESP gateway_dispatch module:
/// the first payload is used as the response body; an empty `Vec` yields an
/// empty payload with `Status::Ok`.
fn payloads_to_response(payloads: Vec<Vec<u8>>) -> Response {
    let payload = payloads
        .into_iter()
        .next()
        .map(Payload::from_vec)
        .unwrap_or_else(Payload::empty);
    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 0,
        partial: false,
        payload,
        watermark_lsn: Lsn::new(0),
        error_code: None,
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}
