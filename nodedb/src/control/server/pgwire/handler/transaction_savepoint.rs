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

    /// Handle SAVEPOINT <name>.
    pub(super) fn handle_savepoint(
        &self,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed
            .split_whitespace()
            .nth(1)
            .unwrap_or("sp")
            .to_string();
        self.sessions.create_savepoint(addr, sp_name);
        Ok(vec![Response::Execution(Tag::new("SAVEPOINT"))])
    }

    /// Handle RELEASE SAVEPOINT <name>.
    pub(super) fn handle_release_savepoint(
        &self,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed
            .split_whitespace()
            .last()
            .unwrap_or("sp")
            .to_string();
        self.sessions.release_savepoint(addr, &sp_name);
        Ok(vec![Response::Execution(Tag::new("RELEASE"))])
    }

    /// Handle ROLLBACK TO SAVEPOINT <name>.
    pub(super) fn handle_rollback_to_savepoint(
        &self,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed
            .split_whitespace()
            .last()
            .unwrap_or("sp")
            .to_string();
        if let Err(msg) = self.sessions.rollback_to_savepoint(addr, &sp_name) {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "3B001".to_owned(),
                msg.to_string(),
            ))));
        }
        Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
    }
}
