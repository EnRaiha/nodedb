// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral in-transaction write-routing gate.
//!
//! Decides, for a single physical task submitted while a connection is
//! inside an explicit transaction block, whether the task is a plain read
//! (falls through to normal dispatch), a write that gets buffered for
//! COMMIT-time replay ("OK" now, durable apply later), or a stageable write
//! that must be applied to the per-transaction overlay immediately (real
//! command tag + statement-time constraint errors now, still buffered for
//! COMMIT's durable replay).
//!
//! This is the shared seam every protocol's dispatch loop routes through
//! (pgwire SQL today; native and the DSL/UPSERT path in later units), so the
//! staging decision lives in exactly one place. No pgwire types are
//! referenced here — callers translate the neutral [`InTxnRoute`] outcome
//! into their own protocol's response type.

use std::future::Future;
use std::net::SocketAddr;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::server::shared::sql::staging_predicates::{
    extract_affected_count, is_stageable_write, staged_tag_kind,
};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::state::TransactionState;
use super::store::SessionStore;
pub use crate::control::server::shared::sql::staging_predicates::StagedTagKind;

/// Outcome of routing a single task through the in-transaction staging gate.
pub enum InTxnRoute {
    /// Not a write (or not in a transaction block at all): the task is
    /// handed back, possibly with `txn_id` stamped for read-your-own-writes,
    /// for the caller's normal dispatch path.
    Read(Box<PhysicalTask>),
    /// A non-stageable write: buffered for COMMIT-time replay. The caller
    /// pushes an immediate "OK" tag.
    Buffered,
    /// A stageable write: applied to the per-transaction overlay now, with
    /// the real outcome available for a "command complete" tag. Also
    /// buffered (unchanged) for COMMIT's durable replay.
    Staged(StagedWriteOutcome),
}

/// The result of staging a write into the per-transaction overlay.
pub struct StagedWriteOutcome {
    pub kind: StagedTagKind,
    pub affected: usize,
    /// The stage handler's raw response payload, verbatim. Every staged
    /// write's response carries a payload here; only [`StagedTagKind::
    /// RawPayload`] outcomes (KV `Incr` / `IncrFloat` / `Cas` / `GetSet`,
    /// which return a computed value rather than an affected-row count) are
    /// expected to be forwarded to the client instead of being reduced to a
    /// tag + count.
    pub payload: Vec<u8>,
}

/// Session store + connection address, bundled so the protocol-neutral DDL
/// dispatch path (`dispatch` -> `try_dispatch` -> `upsert_document` /
/// `insert_document` -> `plan_and_dispatch`, plus the `COPY FROM` bulk-import
/// chain) can thread a single extra parameter down to [`route_in_tx_write`]
/// instead of two positional arguments at every layer.
pub struct DmlTxnCtx<'a> {
    pub sessions: &'a SessionStore,
    pub addr: &'a SocketAddr,
}

/// An owned, session-less scope for callers with no BEGIN/COMMIT transaction
/// concept over their transport (stateless HTTP, autocommit test helpers).
///
/// It owns a fresh [`SessionStore`] and a placeholder address; because a fresh
/// store reports [`TransactionState::Idle`] for every address,
/// [`route_in_tx_write`] always takes the `Read` (immediate autocommit
/// dispatch) branch through a [`DmlTxnCtx`] borrowed from here — byte-identical
/// to the pre-gate behavior. Keep the scope alive for the duration of the
/// dispatch call that borrows its [`ctx`](Self::ctx).
pub struct DetachedTxnScope {
    sessions: SessionStore,
    addr: SocketAddr,
}

impl Default for DetachedTxnScope {
    fn default() -> Self {
        Self::new()
    }
}

impl DetachedTxnScope {
    /// Create an owned session-less scope.
    pub fn new() -> Self {
        Self {
            sessions: SessionStore::new(),
            addr: SocketAddr::from(([0, 0, 0, 0], 0)),
        }
    }

    /// Borrow a [`DmlTxnCtx`] pointing at this scope's owned store + address.
    pub fn ctx(&self) -> DmlTxnCtx<'_> {
        DmlTxnCtx {
            sessions: &self.sessions,
            addr: &self.addr,
        }
    }
}

/// Error surfaced by [`route_in_tx_write`]. Kept distinct from
/// `crate::Error::DataPlane` (used elsewhere for data-plane errors that
/// arrive as a genuine `Err` from a dispatch call) because this variant
/// specifically represents a *successful* dispatch whose response carries a
/// logical failure (`Status::Error` + `error_code`) -- the same signal
/// `response_status_to_sqlstate` decodes today. Keeping the two separate
/// lets each protocol's caller reproduce its exact prior mapping: a real
/// dispatch `Err` maps through that protocol's generic error mapper (as
/// before), while a staged-write rejection maps through the precise
/// `ErrorCode` -> wire-format mapping the status check used to apply
/// inline.
pub enum StagingGateError {
    /// The dispatch closure itself returned an error.
    Dispatch(crate::Error),
    /// The dispatch succeeded, but the response reports a logical failure.
    /// `None` when the response carried no `error_code` (an "unknown data
    /// plane error" case).
    Rejected { code: Option<ErrorCode> },
}

/// Route a single physical task through the in-transaction staging gate.
///
/// `dispatch` is invoked ONLY for a stageable write, with a
/// `MetaOp::StageWrite` task wrapping the original plan; it must dispatch
/// that task and return the neutral `crate::Result<Response>` (i.e. the same
/// result a protocol's own single-task dispatch method produces, before any
/// protocol-specific error-to-wire mapping is applied).
pub async fn route_in_tx_write<F, Fut>(
    sessions: &SessionStore,
    addr: &SocketAddr,
    mut task: PhysicalTask,
    dispatch: F,
) -> Result<InTxnRoute, StagingGateError>
where
    F: FnOnce(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    if sessions.transaction_state(addr) != TransactionState::InBlock {
        return Ok(InTxnRoute::Read(Box::new(task)));
    }

    let is_write = crate::control::wal_replication::to_replicated_entry(
        task.tenant_id,
        task.vshard_id,
        &task.plan,
    )
    .is_some();

    if !is_write {
        // Not a write: an in-transaction read. Stamp the active transaction
        // id onto the task so the Data Plane can check this transaction's
        // staging overlay for read-your-own-writes on point lookups.
        task.txn_id = sessions.tx_id(addr);
        return Ok(InTxnRoute::Read(Box::new(task)));
    }

    // Point writes execute at STATEMENT time via the staging overlay (real
    // tag + statement-time constraint errors); the plan is still buffered so
    // COMMIT stays the sole durable apply. Other writes keep buffer + "OK".
    if !is_stageable_write(&task.plan) {
        sessions.buffer_write(addr, task);
        return Ok(InTxnRoute::Buffered);
    }

    stage_write(sessions, addr, task, dispatch).await
}

/// Stage a stageable write into the per-transaction overlay and classify its
/// outcome. Split out of [`route_in_tx_write`] to keep that function short.
async fn stage_write<F, Fut>(
    sessions: &SessionStore,
    addr: &SocketAddr,
    task: PhysicalTask,
    dispatch: F,
) -> Result<InTxnRoute, StagingGateError>
where
    F: FnOnce(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    let stage_task = PhysicalTask {
        tenant_id: task.tenant_id,
        vshard_id: task.vshard_id,
        database_id: task.database_id,
        plan: PhysicalPlan::Meta(MetaOp::StageWrite {
            plan: Box::new(task.plan.clone()),
        }),
        post_set_op: PostSetOp::None,
        txn_id: sessions.tx_id(addr),
    };

    let resp = dispatch(stage_task)
        .await
        .map_err(StagingGateError::Dispatch)?;

    if resp.status == Status::Error {
        return Err(StagingGateError::Rejected {
            code: resp.error_code.clone(),
        });
    }

    let affected = extract_affected_count(resp.payload.as_ref()).unwrap_or(1) as usize;
    let kind = staged_tag_kind(&task.plan, resp.payload.as_ref());
    let payload = resp.payload.as_ref().to_vec();

    // Durable path unchanged: still buffered, replayed at COMMIT.
    sessions.buffer_write(addr, task);

    Ok(InTxnRoute::Staged(StagedWriteOutcome {
        kind,
        affected,
        payload,
    }))
}
