// SPDX-License-Identifier: BUSL-1.1

//! Submit a gate-rejected write through the deterministic Calvin scheduler.
//!
//! When [`admit`](super::admit) returns [`WriteAdmission::RouteToCalvin`](super::WriteAdmission),
//! the caller hands the single write here. Two shapes reach this path, each
//! routed through the existing Calvin entry point that builds a VALID write set
//! for it:
//!
//! - A **point write** whose key a pending commit holds (Document / KV / Vector /
//!   single-home edge): submitted as a single-vshard
//!   [`build_single_vshard_tx_class`] + [`submit_calvin_routed`]. Because it
//!   targets one vshard, it uses the single-vshard opt-in rather than the strict
//!   multi-vshard builder. The scheduler acquires its key on the SAME lock
//!   table the gate probed, queues FIFO behind the holder, and applies it once
//!   released.
//! - A **predicate write** (`BulkUpdate` / `BulkDelete` on a SINGLE
//!   collection): its write set is not statically known, so it goes through
//!   [`dispatch_dependent_edge_recon`] with the single-vshard opt-in, which
//!   runs the pre-exec reconnaissance scan to discover the affected
//!   surrogates and commits the dependent Calvin transaction. A single
//!   collection resolves to one vshard, so — exactly like the point-write
//!   case above — the strict multi-vshard dependent builder would reject it;
//!   the opt-in lets it sequence through the scheduler instead.
//!
//! Either way the applied [`Response`] (carrying any RETURNING rows) is returned
//! so the caller surfaces it in place of a fast dispatch.
//!
//! # Boxed future — breaks the async-recursion cycle
//!
//! The Calvin path this reaches (recon / routed submit) can, in turn, dispatch
//! writes back through the same autocommit funnel that called here — an async
//! recursion cycle whose future would otherwise be infinitely sized. Returning a
//! `Pin<Box<dyn Future>>` heap-boxes exactly one edge of that cycle, giving every
//! future in the strongly-connected group a finite size. The box is paid ONLY on
//! this cold routed path; the uncontended fast path never calls this function.

use std::future::Future;
use std::pin::Pin;

use crate::bridge::envelope::Response;
use crate::control::planner::calvin::{
    build_single_vshard_tx_class, dispatch_dependent_edge_recon, is_dependent_predicate,
    submit_calvin_routed,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, RequestId, TenantId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Synthesize the bare `Ok` command-tag response for a Calvin-routed write
/// that carried no RETURNING rows (i.e. [`route_write_to_calvin`] resolved to
/// `None`). Every caller of `route_write_to_calvin` needs this same fallback,
/// so it lives here once instead of being reconstructed at each call site.
pub fn bare_ok_response(request_id: RequestId) -> Response {
    Response {
        request_id,
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(Vec::new()),
        watermark_lsn: crate::types::Lsn::ZERO,
        error_code: None,
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}

/// Route one write to the deterministic scheduler and return the applied
/// `Response`. `None` for a plain write with no RETURNING rows — the caller then
/// synthesizes its normal command-tag response.
///
/// Returns a boxed future (rather than an `async fn`) to break the async
/// recursion cycle described in the module docs.
pub fn route_write_to_calvin<'a>(
    shared: &'a SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
) -> Pin<Box<dyn Future<Output = crate::Result<Option<Response>>> + Send + 'a>> {
    Box::pin(async move {
        let task = PhysicalTask {
            tenant_id,
            vshard_id,
            database_id,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };

        // Predicate writes have no statically-known write set: discover it via the
        // dependent reconnaissance path, which builds a valid dependent TxClass.
        //
        // This write reaches here ONLY because `admit` returned `RouteToCalvin`:
        // a pending commit already holds a key in the predicate's range. A
        // single-collection predicate targets a single vshard, so the recon
        // dispatch must opt in to the single-vshard-allowed dependent builder
        // (`allow_single_vshard: true`) — otherwise the strict multi-vshard
        // floor rejects the legitimately single-vshard write.
        if is_dependent_predicate(&task.plan) {
            let recon =
                dispatch_dependent_edge_recon(shared, vec![task], tenant_id, database_id, true)
                    .await?;
            return Ok(recon.apply_result);
        }

        // Point write: its key is known, so build a static TxClass and submit it.
        //
        // This write reaches here ONLY because `admit` returned `RouteToCalvin`:
        // a pending commit already holds its key. A point write targets a single
        // vshard, so it must be built with the single-vshard opt-in — it sequences
        // through the scheduler to serialize on the SAME shared per-vShard
        // `LockManager` the holder is on, rather than being rejected as a
        // (spuriously) single-vshard multi-shard dispatch.
        let tx_class = build_single_vshard_tx_class(std::slice::from_ref(&task), tenant_id, &[])?;
        submit_calvin_routed(shared, tx_class).await
    })
}
