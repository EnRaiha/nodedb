// SPDX-License-Identifier: BUSL-1.1

//! Savepoint and deferred-offset handlers for `NodeDbPgHandler`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle deferred COMMIT OFFSET inside a transaction block.
    ///
    /// Returns `Some(response)` if handled, `None` if not a deferred offset commit.
    pub(super) fn try_handle_deferred_offset(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
        upper: &str,
    ) -> Option<PgWireResult<Vec<Response>>> {
        if !(upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ")) {
            return None;
        }
        if self.sessions.transaction_state(addr)
            != crate::control::server::shared::session::TransactionState::InBlock
        {
            return None;
        }

        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        let tenant_id = identity.tenant_id.as_u64();

        // Single-partition: COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>
        if parts.len() >= 11
            && parts[2].eq_ignore_ascii_case("PARTITION")
            && parts[4].eq_ignore_ascii_case("AT")
            && parts[6].eq_ignore_ascii_case("ON")
        {
            let partition_id: u32 = parts[3].parse().unwrap_or(0);
            let lsn: u64 = parts[5].parse().unwrap_or(0);
            let stream_name = parts[7].to_lowercase();
            let group_name = parts[10].to_lowercase();
            self.sessions.defer_offset_commit(
                addr,
                tenant_id,
                stream_name,
                group_name,
                partition_id,
                lsn,
            );
            return Some(Ok(vec![Response::Execution(Tag::new("COMMIT OFFSET"))]));
        }

        // Batch: COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>
        if parts.len() >= 7
            && parts[1].eq_ignore_ascii_case("OFFSETS")
            && parts[2].eq_ignore_ascii_case("ON")
        {
            let stream_name = parts[3].to_lowercase();
            let group_name = parts[6].to_lowercase();
            if let Some(buffer) = self.state.cdc_router.get_buffer(tenant_id, &stream_name) {
                let events = buffer.read_from_lsn(0, usize::MAX);
                let mut latest: std::collections::HashMap<u32, u64> =
                    std::collections::HashMap::new();
                for e in &events {
                    let entry = latest.entry(e.partition).or_insert(0);
                    if e.lsn > *entry {
                        *entry = e.lsn;
                    }
                }
                for (pid, lsn) in latest {
                    self.sessions.defer_offset_commit(
                        addr,
                        tenant_id,
                        stream_name.clone(),
                        group_name.clone(),
                        pid,
                        lsn,
                    );
                }
            }
            return Some(Ok(vec![Response::Execution(Tag::new("COMMIT OFFSETS"))]));
        }

        None
    }

    /// Reject a savepoint command issued outside a transaction block with
    /// SQLSTATE 25P01 (no_active_sql_transaction), matching PostgreSQL.
    fn require_active_txn(&self, addr: &std::net::SocketAddr) -> PgWireResult<()> {
        use crate::control::server::shared::session::TransactionState;
        if self.sessions.transaction_state(addr) == TransactionState::Idle {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P01".to_owned(),
                "SAVEPOINT can only be used in transaction blocks".to_owned(),
            ))));
        }
        Ok(())
    }

    /// Dispatch a savepoint overlay meta-op to the transaction's home vShard.
    ///
    /// Returns the response payload bytes, or `None` when no staged write has
    /// homed a vShard yet (the overlay — and its journal — is empty, so there
    /// is nothing on the Data Plane to mark or rewind).
    async fn dispatch_overlay_savepoint(
        &self,
        tenant_id: crate::types::TenantId,
        op: nodedb_physical::physical_plan::MetaOp,
        addr: &std::net::SocketAddr,
    ) -> Option<Vec<u8>> {
        let (txn_id, vshard) = self.sessions.txn_identity(addr);
        let (_txn_id, vshard_id) = (txn_id?, vshard?);
        let task = nodedb_physical::physical_task::PhysicalTask {
            tenant_id,
            vshard_id,
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: crate::bridge::envelope::PhysicalPlan::Meta(op),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        match self.dispatch_task_no_wal(task, None).await {
            Ok(resp) => Some(resp.payload.to_vec()),
            Err(e) => {
                tracing::warn!(error = %e, "savepoint overlay meta-op dispatch failed");
                None
            }
        }
    }

    /// Handle SAVEPOINT <name>.
    pub(super) async fn handle_savepoint(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        self.require_active_txn(addr)?;
        let sp_name = sql_trimmed
            .split_whitespace()
            .nth(1)
            .unwrap_or("sp")
            .to_string();
        // Capture the overlay undo-journal marker on the txn's home vShard so a
        // later ROLLBACK TO reverts staged value/TTL state to exactly here. A
        // missing/short payload (or no vShard yet) means an empty journal → 0.
        let txn_id = self.sessions.tx_id(addr);
        let journal_marker = match txn_id {
            Some(txn_id) => {
                let payload = self
                    .dispatch_overlay_savepoint(
                        identity.tenant_id,
                        nodedb_physical::physical_plan::MetaOp::MarkSavepoint { txn_id },
                        addr,
                    )
                    .await;
                payload
                    .filter(|bytes| bytes.len() == 8)
                    .map(|bytes| {
                        let mut arr = [0u8; 8];
                        arr.copy_from_slice(&bytes);
                        u64::from_le_bytes(arr) as usize
                    })
                    .unwrap_or(0)
            }
            None => 0,
        };
        self.sessions
            .create_savepoint(addr, sp_name, journal_marker);
        Ok(vec![Response::Execution(Tag::new("SAVEPOINT"))])
    }

    /// Handle RELEASE SAVEPOINT <name>.
    pub(super) fn handle_release_savepoint(
        &self,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        self.require_active_txn(addr)?;
        let sp_name = sql_trimmed
            .split_whitespace()
            .last()
            .unwrap_or("sp")
            .to_string();
        // RELEASE only pops the Control-Plane savepoint stack; the overlay
        // journal entries are retained (they merge into the enclosing scope),
        // so no Data-Plane meta-op is dispatched.
        if let Err(e) = self.sessions.release_savepoint(addr, &sp_name) {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "3B001".to_owned(),
                e.to_string(),
            ))));
        }
        Ok(vec![Response::Execution(Tag::new("RELEASE"))])
    }

    /// Handle ROLLBACK TO SAVEPOINT <name>.
    pub(super) async fn handle_rollback_to_savepoint(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        self.require_active_txn(addr)?;
        let sp_name = sql_trimmed
            .split_whitespace()
            .last()
            .unwrap_or("sp")
            .to_string();
        let journal_marker = match self.sessions.rollback_to_savepoint(addr, &sp_name) {
            Ok(marker) => marker,
            Err(msg) => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "3B001".to_owned(),
                    msg.to_string(),
                ))));
            }
        };
        // Rewind the Data-Plane value + TTL overlay to the marked journal point.
        if let Some(txn_id) = self.sessions.tx_id(addr) {
            self.dispatch_overlay_savepoint(
                identity.tenant_id,
                nodedb_physical::physical_plan::MetaOp::RollbackToSavepoint {
                    txn_id,
                    journal_marker: journal_marker as u64,
                },
                addr,
            )
            .await;
        }
        Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
    }
}
