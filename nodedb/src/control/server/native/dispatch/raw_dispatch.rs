// SPDX-License-Identifier: BUSL-1.1

//! Raw native-operation dispatch shared by direct-op handlers.

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::types::{Lsn, RequestId, TenantId, TraceId, TxnId, VShardId};

use super::super::super::dispatch_utils;
use super::DispatchCtx;

pub(crate) async fn dispatch_single_task_raw(
    ctx: &DispatchCtx<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    match ctx.state.gateway.get() {
        Some(gateway) => {
            let query = GatewayQueryContext {
                tenant_id,
                trace_id: TraceId::generate(),
                database_id: ctx.database_id(),
                txn_id,
            };
            gateway
                .execute(&query, plan)
                .await
                .map(gateway_payloads_to_response)
                .map_err(|error| {
                    let (_, detail) = GatewayErrorMap::to_native(&error);
                    crate::Error::Dispatch { detail }
                })
        }
        None => dispatch_without_gateway(ctx, tenant_id, vshard_id, plan, txn_id).await,
    }
}

pub(super) async fn dispatch_without_gateway(
    ctx: &DispatchCtx<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    let database_id = ctx.database_id();
    if let PhysicalPlan::Crdt(nodedb_physical::physical_plan::CrdtOp::Apply {
        collection, ..
    }) = &plan
    {
        if txn_id.is_some() {
            return Err(crate::Error::CrdtApplyForbiddenInTransaction);
        }
        let collection = collection.clone();
        let outcome = crate::control::crdt_admission::dispatch_crdt_apply_admitted_outcome(
            ctx.state,
            crate::control::crdt_admission::CrdtApplyAdmissionRequest {
                tenant_id,
                database_id,
                collection: &collection,
                plan,
                timeout: std::time::Duration::from_secs(
                    ctx.state.tuning.network.default_deadline_secs,
                ),
                event_source: crate::event::EventSource::User,
                policy: &crate::control::crdt_admission::TrustedInternalCrdtPolicy,
            },
        )
        .await?;
        return Ok(Response {
            request_id: RequestId::new(0),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(outcome.payload),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: outcome.write_version,
            write_set: Vec::new(),
        });
    }
    let frontier_mutation = txn_id.is_none()
        && matches!(
            &plan,
            PhysicalPlan::Crdt(op)
                if crate::control::crdt_admission::changes_crdt_frontier(op)
        );
    let write = || async move {
        dispatch_utils::dispatch_autocommit_write(
            ctx.state,
            dispatch_utils::AutocommitWrite {
                tenant_id,
                database_id,
                vshard_id,
                plan,
                trace_id: TraceId::ZERO,
                event_source: crate::event::EventSource::User,
                txn_id,
            },
        )
        .await
    };
    if frontier_mutation {
        ctx.state
            .vshard_admission_sequencer
            .run(vshard_id, write)
            .await
    } else {
        write().await
    }
}

fn gateway_payloads_to_response(payloads: Vec<Vec<u8>>) -> Response {
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
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        read_set_valid: None,
        read_version_lsn: Lsn::ZERO,
        write_set: Vec::new(),
    }
}
