// SPDX-License-Identifier: BUSL-1.1

//! BEGIN and ROLLBACK handlers, plus the shared per-transaction staging
//! overlay release helper used by both ROLLBACK and (from `commit.rs`) COMMIT.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;

use super::super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Release the per-transaction staging overlay on the vShard that hosted
    /// the transaction's staged point writes.
    ///
    /// Best-effort cleanup dispatched AFTER the durable resolution (COMMIT
    /// batch flush / ROLLBACK). A failure here leaks in-memory overlay state
    /// on that core but does not affect the already-resolved transaction, so it
    /// is logged rather than surfaced to the client.
    pub(super) async fn dispatch_drop_txn_overlay(
        &self,
        tenant_id: crate::types::TenantId,
        vshard_id: crate::types::VShardId,
        txn_id: crate::types::TxnId,
    ) {
        let task = nodedb_physical::physical_task::PhysicalTask {
            tenant_id,
            vshard_id,
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: crate::bridge::envelope::PhysicalPlan::Meta(
                nodedb_physical::physical_plan::MetaOp::DropTxnOverlay { txn_id },
            ),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        if let Err(e) = self.dispatch_task_no_wal(task, None).await {
            tracing::warn!(error = %e, "failed to drop per-transaction staging overlay");
        }
    }

    /// Handle BEGIN / START TRANSACTION.
    pub(in crate::control::server::pgwire::handler) fn handle_begin(
        &self,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let snapshot_lsn = {
            let next = self.state.wal.next_lsn();
            crate::types::Lsn::new(next.as_u64().saturating_sub(1))
        };
        crate::control::server::shared::session::ddl_buffer::activate();
        self.sessions.begin(addr, snapshot_lsn).map_err(|msg| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                msg.to_owned(),
            )))
        })?;
        Ok(vec![Response::Execution(Tag::new("BEGIN"))])
    }

    /// Handle ROLLBACK / ABORT.
    pub(in crate::control::server::pgwire::handler) async fn handle_rollback(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        crate::control::server::shared::session::ddl_buffer::discard();
        // Snapshot the overlay identity BEFORE `rollback()` clears session
        // state, so the staging overlay can be released on its home vShard.
        let (overlay_txn_id, overlay_vshard) = self.sessions.txn_identity(addr);
        let reservations = self.sessions.rollback(addr).unwrap_or_default();
        for handle in &reservations {
            let key = &handle.sequence_key;
            let registry = &self.state.sequence_registry;
            registry.gap_free_manager().rollback(handle, || {
                let map = registry.sequences_read();
                if let Some(h) = map.get(key.as_str()) {
                    h.rollback_one();
                }
            });
            {
                let catalog = self.state.credentials.catalog();
                crate::control::sequence::log::log_reservation(
                    catalog,
                    &crate::control::sequence::log::rolled_back(
                        key,
                        handle.value,
                        &identity.username,
                        identity.tenant_id.as_u64(),
                    ),
                );
            }
        }
        self.sessions.close_non_hold_cursors(addr);
        // Discard NOTIFY messages buffered during this transaction.
        self.sessions.discard_pending_notifies(addr);

        // Release any staging overlay populated by statement-time point writes.
        if let (Some(txn_id), Some(vshard_id)) = (overlay_txn_id, overlay_vshard) {
            self.dispatch_drop_txn_overlay(identity.tenant_id, vshard_id, txn_id)
                .await;
        }
        Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
    }
}
