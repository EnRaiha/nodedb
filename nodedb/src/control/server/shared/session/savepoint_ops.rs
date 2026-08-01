// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral savepoint orchestration: SAVEPOINT, RELEASE SAVEPOINT,
//! ROLLBACK TO SAVEPOINT, plus the neutral COMMIT OFFSET parse helper.
//!
//! Captures/rewinds the composite overlay undo-journal marker on the
//! transaction's home vShard (via the injected [`TxnDataPlane`]) and drives the
//! neutral `SessionStore` savepoint stack. Transports translate the returned
//! [`SavepointError`] into their own SQLSTATE (`25P01` / `3B001`).

use std::collections::BTreeMap;

use crate::bridge::envelope::PhysicalPlan;
use crate::event::cdc::CdcOffset;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::connection::SessionId;
use super::outcome::TxnDataPlane;
use super::state::TransactionState;
use super::store::SessionStore;

/// Typed savepoint failure. Adapters map each variant to its SQLSTATE.
#[derive(Debug)]
pub enum SavepointError {
    /// A savepoint command was issued outside a transaction block. → `25P01`.
    NoActiveTransaction,
    /// The named savepoint does not exist (or there is no active session).
    /// `message` is the exact human message. → `3B001`.
    NotFound { message: String },
}

/// Reject a savepoint command issued outside a transaction block.
fn require_active_txn(
    sessions: &SessionStore,
    session_id: SessionId,
) -> Result<(), SavepointError> {
    if sessions.transaction_state(session_id) == TransactionState::Idle {
        return Err(SavepointError::NoActiveTransaction);
    }
    Ok(())
}

/// Dispatch a savepoint overlay meta-op to a specific vShard's core and return
/// the raw response payload bytes, or `None` on dispatch failure.
async fn dispatch_overlay_savepoint(
    tenant_id: TenantId,
    vshard_id: VShardId,
    dp: &impl TxnDataPlane,
    op: MetaOp,
) -> Option<Vec<u8>> {
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Meta(op),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    // Savepoint overlay meta-ops are not writes — no WAL record, no version.
    match dp.dispatch_no_wal(task, None).await {
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
/// Captures the composite overlay undo-journal marker on EVERY vShard the
/// transaction has staged writes to, so a later ROLLBACK TO reverts staged
/// value/TTL AND graph state on all of them to exactly here. A missing/short
/// payload means empty journals → `(0, 0)`. With no vShard staged yet the
/// marker map is empty.
pub async fn run_savepoint(
    sessions: &SessionStore,
    session_id: SessionId,
    tenant_id: TenantId,
    dp: &impl TxnDataPlane,
    name: &str,
) -> Result<(), SavepointError> {
    require_active_txn(sessions, session_id)?;
    let (txn_id, vshards) = sessions.txn_identity(session_id);
    let mut markers: BTreeMap<VShardId, (usize, usize)> = BTreeMap::new();
    if let Some(txn_id) = txn_id {
        for vshard_id in vshards {
            let payload = dispatch_overlay_savepoint(
                tenant_id,
                vshard_id,
                dp,
                MetaOp::MarkSavepoint { txn_id },
            )
            .await;
            markers.insert(vshard_id, decode_markers(payload));
        }
    }
    sessions.create_savepoint(session_id, name.to_string(), markers);
    Ok(())
}

/// Handle RELEASE SAVEPOINT `<name>`.
///
/// RELEASE only pops the Control-Plane savepoint stack; the overlay journal
/// entries are retained (they merge into the enclosing scope), so no
/// Data-Plane meta-op is dispatched.
pub fn run_release_savepoint(
    sessions: &SessionStore,
    session_id: SessionId,
    name: &str,
) -> Result<(), SavepointError> {
    require_active_txn(sessions, session_id)?;
    sessions
        .release_savepoint(session_id, name)
        .map_err(|e| SavepointError::NotFound {
            message: e.to_string(),
        })
}

/// Handle ROLLBACK TO SAVEPOINT `<name>`.
///
/// Truncates the write buffer to the saved position and rewinds BOTH the
/// value/TTL overlay and the graph overlay on every vShard the transaction has
/// staged to. Iterates the CURRENT staged set (a superset of the savepoint's,
/// since writes may have staged to NEW vShards after the savepoint): a vShard
/// with a saved marker rewinds to it; a vShard first staged AFTER the savepoint
/// has no saved marker and rewinds to `(0, 0)`, dropping ALL of its staged
/// writes.
pub async fn run_rollback_to_savepoint(
    sessions: &SessionStore,
    session_id: SessionId,
    tenant_id: TenantId,
    dp: &impl TxnDataPlane,
    name: &str,
) -> Result<(), SavepointError> {
    require_active_txn(sessions, session_id)?;
    let markers = sessions
        .rollback_to_savepoint(session_id, name)
        .map_err(|e| SavepointError::NotFound {
            message: e.to_string(),
        })?;
    let (txn_id, vshards) = sessions.txn_identity(session_id);
    if let Some(txn_id) = txn_id {
        for vshard_id in vshards {
            let (value_marker, graph_marker) = markers.get(&vshard_id).copied().unwrap_or((0, 0));
            dispatch_overlay_savepoint(
                tenant_id,
                vshard_id,
                dp,
                MetaOp::RollbackToSavepoint {
                    txn_id,
                    value_marker: value_marker as u64,
                    graph_marker: graph_marker as u64,
                },
            )
            .await;
        }
    }
    Ok(())
}

/// A parsed deferred COMMIT OFFSET / COMMIT OFFSETS command.
pub enum DeferredOffsetCmd {
    /// `COMMIT OFFSET PARTITION <p> AT <lsn>:<sequence> ON <stream> CONSUMER GROUP <name>`.
    Single {
        stream: String,
        group: String,
        partition_id: u32,
        offset: CdcOffset,
    },
    /// `COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>` — the caller resolves
    /// the latest LSN per partition from the CDC buffer.
    Batch { stream: String, group: String },
}

/// Parse a `COMMIT OFFSET` / `COMMIT OFFSETS` statement into a neutral command.
///
/// Returns `Ok(None)` when `sql` is not a deferred-offset commit. `upper` is
/// the caller's uppercased form of `sql`, passed in to avoid re-allocating it.
/// A bare LSN uses the documented legacy whole-LSN acknowledgement semantics.
pub fn parse_deferred_offset(sql: &str, upper: &str) -> Result<Option<DeferredOffsetCmd>, String> {
    if !(upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ")) {
        return Ok(None);
    }
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // Single-partition: COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>
    if parts.len() >= 11
        && parts[2].eq_ignore_ascii_case("PARTITION")
        && parts[4].eq_ignore_ascii_case("AT")
        && parts[6].eq_ignore_ascii_case("ON")
    {
        let partition_id: u32 = parts[3]
            .parse()
            .map_err(|_| format!("invalid partition: '{}'", parts[3]))?;
        let offset: CdcOffset = parts[5]
            .parse()
            .map_err(|error: crate::event::cdc::offset::ParseCdcOffsetError| error.to_string())?;
        return Ok(Some(DeferredOffsetCmd::Single {
            stream: parts[7].to_lowercase(),
            group: parts[10].to_lowercase(),
            partition_id,
            offset,
        }));
    }

    // Batch: COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>
    if parts.len() >= 7
        && parts[1].eq_ignore_ascii_case("OFFSETS")
        && parts[2].eq_ignore_ascii_case("ON")
    {
        return Ok(Some(DeferredOffsetCmd::Batch {
            stream: parts[3].to_lowercase(),
            group: parts[6].to_lowercase(),
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_commit_offset_accepts_emitted_and_legacy_tokens() {
        let canonical = parse_deferred_offset(
            "COMMIT OFFSET PARTITION 2 AT 42:7 ON orders CONSUMER GROUP analytics",
            "COMMIT OFFSET PARTITION 2 AT 42:7 ON ORDERS CONSUMER GROUP ANALYTICS",
        )
        .unwrap()
        .unwrap();
        let DeferredOffsetCmd::Single { offset, .. } = canonical else {
            panic!("expected single offset commit");
        };
        assert_eq!(offset, CdcOffset::new(42, 7));

        let legacy = parse_deferred_offset(
            "COMMIT OFFSET PARTITION 2 AT 42 ON orders CONSUMER GROUP analytics",
            "COMMIT OFFSET PARTITION 2 AT 42 ON ORDERS CONSUMER GROUP ANALYTICS",
        )
        .unwrap()
        .unwrap();
        let DeferredOffsetCmd::Single { offset, .. } = legacy else {
            panic!("expected single offset commit");
        };
        assert_eq!(offset, CdcOffset::legacy_lsn(42));
    }
}
