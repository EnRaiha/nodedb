// SPDX-License-Identifier: BUSL-1.1

//! Native Graph MATCH dispatch and response-envelope handling.

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::{Response, Status};

use super::raw_dispatch::dispatch_authorized_single_task;
use super::response::data_plane_response_to_native;
use super::{DispatchCtx, error_to_native};

/// Dispatch a native `GraphMatch` op, unwrapping the DP `{rows, frontier}`
/// envelope into a bare rows array before native conversion.
///
/// MATCH responses are enveloped on the DP→CP hop (see
/// `data::executor::handlers::graph_match`). The native row decoder expects a
/// bare msgpack array, so this handler unwraps the envelope here. In B1
/// `cluster_mode` is always `false`, so the frontier is empty and the rows
/// payload is byte-identical to the prior bare-array native MATCH response.
/// (B2 will consume the frontier for cross-shard continuation.)
pub(crate) async fn handle_graph_match(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_lowercase();
    let vshard_key = fields.document_id.as_deref().unwrap_or(&collection);
    let vshard_id = ctx.vshard_for_key(vshard_key);
    let tenant_id = ctx.tenant_id();

    if let Err(error) = super::limits::check_op_limits(ctx.state, fields) {
        return NativeResponse::error(seq, "0A000", error.to_string());
    }
    if let Err(error) = ctx.state.check_tenant_quota(tenant_id) {
        return error_to_native(seq, &error);
    }

    let mut plan =
        match super::plan_builder::build_plan(ctx, OpCode::GraphMatch, fields, &collection) {
            Ok(plan) => plan,
            Err(error) => return NativeResponse::error(seq, "42601", error.to_string()),
        };
    if let Err(error) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        &mut plan,
        &ctx.state.rls,
        ctx.auth_context(),
    ) {
        return NativeResponse::error(seq, "42501", error.to_string());
    }

    // Stamp the active transaction id so MATCH reads resolve this connection's
    // staging overlay identically to every other direct-op read.
    let txn_id = ctx.sessions.tx_id(ctx.peer_addr);
    let plan_for_response = plan.clone();
    let _request = ctx.state.tenant_request_guard(tenant_id);
    let raw = dispatch_authorized_single_task(ctx, tenant_id, vshard_id, plan, txn_id).await;

    let response = match raw {
        Ok(response) => response,
        Err(error) => return error_to_native(seq, &error),
    };

    // A MATCH issued inside a native transaction records a collection-scoped
    // predicate read at the shard's watermark, identical to every other read
    // seam. Single-shard direct op means one watermark and one entry.
    if (response.status == Status::Ok
        || response.error_code.as_deref() == Some(&crate::bridge::envelope::ErrorCode::NotFound))
        && ctx.sessions.transaction_state(ctx.peer_addr)
            == crate::control::server::shared::session::TransactionState::InBlock
    {
        crate::control::server::shared::session::record_reads_for_response(
            ctx.state,
            ctx.sessions,
            ctx.peer_addr.into(),
            ctx.tenant_id(),
            crate::control::server::shared::session::ResponseReads {
                plan: &plan_for_response,
                watermarks: &[(vshard_id, response.watermark_lsn)],
                read_version_lsn: response.read_version_lsn,
                found: response.status == Status::Ok,
                distributed_reads: &[],
                read_lsn_vshard: vshard_id,
            },
        )
        .await;
    }

    if response.status == Status::Error {
        return data_plane_response_to_native(ctx, seq, &plan_for_response, &response);
    }

    // Unwrap the `{rows, frontier, resume}` envelope into a bare rows array.
    // This single-shard path does not consume frontier or resume metadata.
    let unwrapped =
        match crate::control::server::graph_dispatch::unwrap_match_envelope(&response.payload) {
            Ok(envelope) => Response {
                payload: envelope.rows_payload,
                ..response
            },
            Err(error) => return error_to_native(seq, &error),
        };
    data_plane_response_to_native(ctx, seq, &plan_for_response, &unwrapped)
}
