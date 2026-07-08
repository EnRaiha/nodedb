// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral savepoint orchestration: SAVEPOINT, RELEASE SAVEPOINT,
//! ROLLBACK TO SAVEPOINT, plus the neutral COMMIT OFFSET parse helper.
//!
//! Captures/rewinds the composite overlay undo-journal marker on the
//! transaction's home vShard (via the injected [`TxnDataPlane`]) and drives the
//! neutral `SessionStore` savepoint stack. Transports translate the returned
//! [`SavepointError`] into their own SQLSTATE (`25P01` / `3B001`).

use std::net::SocketAddr;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::outcome::TxnDataPlane;
use super::state::TransactionState;
use super::store::SessionStore;

/// Typed savepoint failure. Adapters map each variant to its SQLSTATE.
pub enum SavepointError {
    /// A savepoint command was issued outside a transaction block. → `25P01`.
    NoActiveTransaction,
    /// The named savepoint does not exist (or there is no active session).
    /// `message` is the exact human message. → `3B001`.
    NotFound { message: String },
}

/// Reject a savepoint command issued outside a transaction block.
fn require_active_txn(sessions: &SessionStore, addr: &SocketAddr) -> Result<(), SavepointError> {
    if sessions.transaction_state(addr) == TransactionState::Idle {
        return Err(SavepointError::NoActiveTransaction);
    }
    Ok(())
}

/// Dispatch a savepoint overlay meta-op to the transaction's home vShard and
/// return the raw response payload bytes, or `None` when no staged write has
/// homed a vShard yet (the overlay — and its journal — is empty, so there is
/// nothing on the Data Plane to mark or rewind).
async fn dispatch_overlay_savepoint(
    sessions: &SessionStore,
    addr: &SocketAddr,
    tenant_id: TenantId,
    dp: &impl TxnDataPlane,
    op: MetaOp,
) -> Option<Vec<u8>> {
    let (txn_id, vshard) = sessions.txn_identity(addr);
    let (_txn_id, vshard_id) = (txn_id?, vshard?);
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Meta(op),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    match dp.dispatch_no_wal(task).await {
        Ok(resp) => Some(resp.payload.to_vec()),
        Err(e) => {
            tracing::warn!(error = %e, "savepoint overlay meta-op dispatch failed");
            None
        }
    }
}

/// Decode the 16-byte composite savepoint marker payload: two LE u64s carrying
/// the value/TTL overlay journal marker followed by the graph overlay marker.
/// A missing or short payload means empty journals → `(0, 0)`.
fn decode_markers(payload: Option<Vec<u8>>) -> (usize, usize) {
    payload
        .filter(|bytes| bytes.len() == 16)
        .map(|bytes| {
            let mut value = [0u8; 8];
            value.copy_from_slice(&bytes[..8]);
            let mut graph = [0u8; 8];
            graph.copy_from_slice(&bytes[8..16]);
            (
                u64::from_le_bytes(value) as usize,
                u64::from_le_bytes(graph) as usize,
            )
        })
        .unwrap_or((0, 0))
}

/// Handle SAVEPOINT `<name>`.
///
/// Captures the composite overlay undo-journal marker on the txn's home vShard
/// so a later ROLLBACK TO reverts staged value/TTL AND graph state to exactly
/// here. A missing/short payload (or no vShard yet) means empty journals →
/// `(0, 0)`.
pub async fn run_savepoint(
    sessions: &SessionStore,
    addr: &SocketAddr,
    tenant_id: TenantId,
    dp: &impl TxnDataPlane,
    name: &str,
) -> Result<(), SavepointError> {
    require_active_txn(sessions, addr)?;
    let (value_marker, graph_marker) = match sessions.tx_id(addr) {
        Some(txn_id) => {
            let payload = dispatch_overlay_savepoint(
                sessions,
                addr,
                tenant_id,
                dp,
                MetaOp::MarkSavepoint { txn_id },
            )
            .await;
            decode_markers(payload)
        }
        None => (0, 0),
    };
    sessions.create_savepoint(addr, name.to_string(), value_marker, graph_marker);
    Ok(())
}

/// Handle RELEASE SAVEPOINT `<name>`.
///
/// RELEASE only pops the Control-Plane savepoint stack; the overlay journal
/// entries are retained (they merge into the enclosing scope), so no
/// Data-Plane meta-op is dispatched.
pub fn run_release_savepoint(
    sessions: &SessionStore,
    addr: &SocketAddr,
    name: &str,
) -> Result<(), SavepointError> {
    require_active_txn(sessions, addr)?;
    sessions
        .release_savepoint(addr, name)
        .map_err(|e| SavepointError::NotFound {
            message: e.to_string(),
        })
}

/// Handle ROLLBACK TO SAVEPOINT `<name>`.
///
/// Truncates the write buffer to the saved position and rewinds BOTH the
/// value/TTL overlay and the graph overlay to the marked journal points.
pub async fn run_rollback_to_savepoint(
    sessions: &SessionStore,
    addr: &SocketAddr,
    tenant_id: TenantId,
    dp: &impl TxnDataPlane,
    name: &str,
) -> Result<(), SavepointError> {
    require_active_txn(sessions, addr)?;
    let (value_marker, graph_marker) =
        sessions
            .rollback_to_savepoint(addr, name)
            .map_err(|e| SavepointError::NotFound {
                message: e.to_string(),
            })?;
    if let Some(txn_id) = sessions.tx_id(addr) {
        dispatch_overlay_savepoint(
            sessions,
            addr,
            tenant_id,
            dp,
            MetaOp::RollbackToSavepoint {
                txn_id,
                value_marker: value_marker as u64,
                graph_marker: graph_marker as u64,
            },
        )
        .await;
    }
    Ok(())
}

/// A parsed deferred COMMIT OFFSET / COMMIT OFFSETS command.
pub enum DeferredOffsetCmd {
    /// `COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>`.
    Single {
        stream: String,
        group: String,
        partition_id: u32,
        lsn: u64,
    },
    /// `COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>` — the caller resolves
    /// the latest LSN per partition from the CDC buffer.
    Batch { stream: String, group: String },
}

/// Parse a `COMMIT OFFSET` / `COMMIT OFFSETS` statement into a neutral command.
///
/// Returns `None` when `sql` is not a deferred-offset commit. `upper` is the
/// caller's uppercased form of `sql`, passed in to avoid re-allocating it.
pub fn parse_deferred_offset(sql: &str, upper: &str) -> Option<DeferredOffsetCmd> {
    if !(upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ")) {
        return None;
    }
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // Single-partition: COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>
    if parts.len() >= 11
        && parts[2].eq_ignore_ascii_case("PARTITION")
        && parts[4].eq_ignore_ascii_case("AT")
        && parts[6].eq_ignore_ascii_case("ON")
    {
        let partition_id: u32 = parts[3].parse().unwrap_or(0);
        let lsn: u64 = parts[5].parse().unwrap_or(0);
        return Some(DeferredOffsetCmd::Single {
            stream: parts[7].to_lowercase(),
            group: parts[10].to_lowercase(),
            partition_id,
            lsn,
        });
    }

    // Batch: COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>
    if parts.len() >= 7
        && parts[1].eq_ignore_ascii_case("OFFSETS")
        && parts[2].eq_ignore_ascii_case("ON")
    {
        return Some(DeferredOffsetCmd::Batch {
            stream: parts[3].to_lowercase(),
            group: parts[6].to_lowercase(),
        });
    }

    None
}
