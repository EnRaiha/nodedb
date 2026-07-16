// SPDX-License-Identifier: BUSL-1.1

//! `SubmitArgs` + `submit_to_data_plane`: the pgwire adapter onto the shared
//! Control-Plane write funnel, used by both `dispatch_local` and
//! `dispatch_task_no_wal` (see `dispatch.rs`).

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::types::{DatabaseId, TraceId};

use super::core::NodeDbPgHandler;

/// Inputs for [`NodeDbPgHandler::submit_to_data_plane`]: the request identity,
/// the plan, and the optional transaction id + the write's durability handling.
pub(super) struct SubmitArgs {
    pub(super) tenant_id: crate::types::TenantId,
    pub(super) vshard_id: crate::types::VShardId,
    pub(super) database_id: DatabaseId,
    pub(super) plan: crate::bridge::envelope::PhysicalPlan,
    pub(super) user_id: Option<Arc<str>>,
    pub(super) txn_id: Option<crate::types::TxnId>,
    /// Who owns this write's durable redo record — see [`WalDurability`].
    pub(super) durability: WalDurability,
}

impl NodeDbPgHandler {
    /// Submit a plan through the shared Control-Plane write funnel: admit, make
    /// durable, enqueue, collect, and publish. Shared by `dispatch_local` and
    /// `dispatch_task_no_wal`.
    pub(super) async fn submit_to_data_plane(&self, args: SubmitArgs) -> crate::Result<Response> {
        let SubmitArgs {
            tenant_id,
            vshard_id,
            database_id,
            plan,
            user_id,
            txn_id,
            durability,
        } = args;
        submit_write(
            &self.state,
            SubmitWrite {
                tenant_id,
                database_id,
                vshard_id,
                plan,
                trace_id: TraceId::generate(),
                event_source: crate::event::EventSource::User,
                txn_id,
                user_id,
                durability,
                ordering: WriteOrdering::Gate,
                // SQL DML raises no Control-Plane change event; see
                // [`ChangeFeedOwner::Unowned`] for the gap this preserves.
                change_feed: ChangeFeedOwner::Unowned,
            },
        )
        .await
        .map(|outcome| outcome.response)
    }
}
