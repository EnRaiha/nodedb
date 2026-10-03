// SPDX-License-Identifier: BUSL-1.1

//! What one dispatched task leaves on its sessions: the reads a transaction
//! validates at COMMIT, and the session's own committed write version.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::security::identity::{AuthenticatedIdentity, Permission, required_permission};
use crate::control::server::exchange::resolve::DistributedReadCapture;
use crate::control::server::shared::plan_util::extract_collection;
use crate::control::server::shared::session::{
    DmlTxnCtx, ResponseReads, SessionId, record_reads_for_response,
};
use crate::types::{DatabaseId, Lsn, VShardId};

use super::super::super::core::NodeDbPgHandler;

/// One task the loop dispatched, and the sessions it ran for.
pub(super) struct DispatchedTask<'a> {
    pub identity: &'a AuthenticatedIdentity,
    /// The client's session, which notes its own committed writes.
    pub client_session: SessionId,
    /// The statement's transaction, which validates the task's reads at
    /// COMMIT. `None` for an autocommit statement.
    pub txn: Option<&'a DmlTxnCtx<'a>>,
    pub plan: &'a PhysicalPlan,
    pub vshard: VShardId,
    pub database_id: DatabaseId,
}

impl NodeDbPgHandler {
    /// Record what one dispatched task observed and wrote.
    ///
    /// A read in a transaction joins the transaction's read set, for
    /// snapshot-isolation and cross-shard conflict checks. An absent-key
    /// point read (a `NotFound` from the Data Plane) joins it too: a "not
    /// found" is a validatable phantom observation. A genuine dispatch
    /// failure records nothing.
    ///
    /// A successful write records its committed per-collection version on
    /// the client's session, so a later transaction's read-set capture is
    /// floored at it (the read-your-writes floor for cross-shard OCC). An
    /// autocommit write floors a later transaction's read too, so this
    /// records outside a transaction as well.
    pub(super) async fn track_dispatched_task(
        &self,
        task: DispatchedTask<'_>,
        resp: &Response,
        shard_watermarks: Vec<(VShardId, Lsn)>,
        distributed_reads: &[DistributedReadCapture],
    ) {
        let DispatchedTask {
            identity,
            client_session,
            txn,
            plan,
            vshard,
            database_id,
        } = task;
        let records_read =
            resp.status == Status::Ok || resp.error_code.as_deref() == Some(&ErrorCode::NotFound);
        if records_read && let Some(txn) = txn {
            let watermarks = if shard_watermarks.is_empty() {
                vec![(vshard, resp.watermark_lsn)]
            } else {
                shard_watermarks
            };
            record_reads_for_response(
                &self.state,
                txn.sessions,
                txn.session_id,
                identity.tenant_id,
                ResponseReads {
                    plan,
                    watermarks: &watermarks,
                    read_version_lsn: resp.read_version_lsn,
                    found: resp.status == Status::Ok,
                    distributed_reads,
                    read_lsn_vshard: vshard,
                },
            )
            .await;
        }

        if resp.status == Status::Ok
            && resp.read_version_lsn > Lsn::ZERO
            && matches!(required_permission(plan), Permission::Write)
            && let Some(collection) = extract_collection(plan)
        {
            self.sessions.note_own_write(
                client_session,
                database_id,
                identity.tenant_id,
                collection,
                resp.read_version_lsn,
            );
        }
    }
}
