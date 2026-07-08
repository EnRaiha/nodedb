// SPDX-License-Identifier: BUSL-1.1

//! Transaction lifecycle methods on SessionStore.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::{Lsn, TxnId, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::state::TransactionState;
use super::store::SessionStore;

/// Global monotonic counter minting `TxnId`s across all sessions on this
/// shard. Unique per shard for the lifetime of the process — sufficient
/// for keying the per-transaction staging overlay, which is scoped to a
/// single shard's in-memory state.
static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

impl SessionStore {
    /// Get transaction state for a connection.
    pub fn transaction_state(&self, addr: &SocketAddr) -> TransactionState {
        self.read_session(addr, |s| s.tx_state)
            .unwrap_or(TransactionState::Idle)
    }

    /// BEGIN — enter transaction block with snapshot isolation.
    ///
    /// Captures the current WAL LSN as the local snapshot point (single-shard
    /// fast path) and the last globally-applied Calvin `snapshot_epoch` as the
    /// cross-shard-valid version anchor. All reads within this transaction see
    /// data as of this LSN.
    pub fn begin(
        &self,
        addr: &SocketAddr,
        current_lsn: Lsn,
        snapshot_epoch: u64,
    ) -> Result<(), &'static str> {
        self.write_session(addr, |session| match session.tx_state {
            TransactionState::Idle => {
                session.tx_state = TransactionState::InBlock;
                session.tx_snapshot_lsn = Some(current_lsn);
                session.tx_snapshot_epoch = Some(snapshot_epoch);
                session.tx_read_set.clear();
                session.tx_id = Some(TxnId::new(NEXT_TXN_ID.fetch_add(1, Ordering::Relaxed)));
                session.tx_vshard = None;
                Ok(())
            }
            TransactionState::InBlock => {
                // PostgreSQL issues a WARNING here, not an error.
                Ok(())
            }
            TransactionState::Failed => Err(
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
        })
        .unwrap_or(Ok(()))
    }

    /// Record a read for write conflict detection.
    pub fn record_read(
        &self,
        addr: &SocketAddr,
        collection: String,
        document_id: String,
        read_lsn: Lsn,
    ) {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session
                    .tx_read_set
                    .push((collection, document_id, read_lsn));
            }
        });
    }

    /// Get the snapshot LSN for the current transaction.
    pub fn snapshot_lsn(&self, addr: &SocketAddr) -> Option<Lsn> {
        self.read_session(addr, |s| s.tx_snapshot_lsn)?
    }

    /// Get the cross-shard snapshot epoch for the current transaction.
    pub fn snapshot_epoch(&self, addr: &SocketAddr) -> Option<u64> {
        self.read_session(addr, |s| s.tx_snapshot_epoch)?
    }

    /// Current transaction's overlay id, for stamping a `StageWrite` task
    /// before it is dispatched. `None` outside a transaction block.
    pub fn tx_id(&self, addr: &SocketAddr) -> Option<TxnId> {
        self.read_session(addr, |s| s.tx_id).flatten()
    }

    /// Snapshot the current transaction's overlay identity (id + homing vShard)
    /// WITHOUT clearing it. Called before `rollback()` releases session state so
    /// the caller can dispatch `MetaOp::DropTxnOverlay` to the right vShard.
    pub fn txn_identity(&self, addr: &SocketAddr) -> (Option<TxnId>, Option<VShardId>) {
        self.read_session(addr, |s| (s.tx_id, s.tx_vshard))
            .unwrap_or((None, None))
    }

    /// Collect a value from each buffered write task's plan. Used at commit to
    /// gather the collections this transaction wrote, so its own reads of those
    /// collections are excluded from snapshot-isolation conflict detection
    /// (a read-your-own-write is not a serialization conflict).
    pub fn buffered_collections<F>(
        &self,
        addr: &SocketAddr,
        extract: F,
    ) -> std::collections::HashSet<String>
    where
        F: Fn(&nodedb_physical::physical_plan::PhysicalPlan) -> Option<String>,
    {
        self.read_session(addr, |s| {
            s.tx_buffer
                .iter()
                .filter_map(|task| extract(&task.plan))
                .collect()
        })
        .unwrap_or_default()
    }

    /// Drain the read-set for conflict checking at COMMIT time.
    pub fn take_read_set(&self, addr: &SocketAddr) -> Vec<(String, String, Lsn)> {
        self.write_session(addr, |session| std::mem::take(&mut session.tx_read_set))
            .unwrap_or_default()
    }

    /// COMMIT — drain the write buffer and pending offset commits, return to idle.
    ///
    /// Returns the buffered write tasks for atomic dispatch.
    pub fn commit(&self, addr: &SocketAddr) -> Result<Vec<PhysicalTask>, &'static str> {
        self.write_session(addr, |session| {
            let buffer = std::mem::take(&mut session.tx_buffer);
            session.tx_state = TransactionState::Idle;
            session.tx_snapshot_lsn = None;
            session.tx_snapshot_epoch = None;
            session.tx_id = None;
            session.tx_vshard = None;
            session.savepoints.clear();
            // Note: pending_sequence_reservations are taken separately via
            // take_pending_reservations() so the caller can finalize them
            // with the GAP_FREE manager (which requires Arc<SequenceRegistry>).
            Ok(buffer)
        })
        .unwrap_or(Ok(Vec::new()))
    }

    /// Take pending GAP_FREE sequence reservations (called after successful COMMIT).
    pub fn take_pending_reservations(
        &self,
        addr: &SocketAddr,
    ) -> Vec<crate::control::sequence::gap_free::ReservationHandle> {
        self.write_session(addr, |session| {
            std::mem::take(&mut session.pending_sequence_reservations)
        })
        .unwrap_or_default()
    }

    /// Take pending offset commits (called after successful COMMIT dispatch).
    pub fn take_pending_offsets(&self, addr: &SocketAddr) -> Vec<(u64, String, String, u32, u64)> {
        self.write_session(addr, |session| {
            std::mem::take(&mut session.pending_offset_commits)
        })
        .unwrap_or_default()
    }

    /// Defer an offset commit until the current transaction commits.
    ///
    /// Returns `true` if deferred (in transaction), `false` if not (commit immediately).
    pub fn defer_offset_commit(
        &self,
        addr: &SocketAddr,
        tenant_id: u64,
        stream: String,
        group: String,
        partition_id: u32,
        lsn: u64,
    ) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session
                    .pending_offset_commits
                    .push((tenant_id, stream, group, partition_id, lsn));
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    /// Buffer a write task during a transaction block.
    ///
    /// Stamps the task's `txn_id` from the session's active transaction
    /// identity before buffering, inside the same session-lock scope, so
    /// there is no separate lock acquisition that could race or deadlock
    /// against `buffer_write`'s own lock.
    ///
    /// Returns `true` if buffered (in transaction), `false` if not (dispatch immediately).
    pub fn buffer_write(&self, addr: &SocketAddr, mut task: PhysicalTask) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                task.txn_id = session.tx_id;
                if session.tx_vshard.is_none() {
                    session.tx_vshard = Some(task.vshard_id);
                }
                session.tx_buffer.push(task);
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    /// ROLLBACK — discard the write buffer and return to idle.
    /// Returns any pending GAP_FREE reservations that need to be rolled back.
    pub fn rollback(
        &self,
        addr: &SocketAddr,
    ) -> Result<Vec<crate::control::sequence::gap_free::ReservationHandle>, &'static str> {
        let reservations = self
            .write_session(addr, |session| {
                session.tx_buffer.clear();
                session.tx_state = TransactionState::Idle;
                session.tx_snapshot_lsn = None;
                session.tx_snapshot_epoch = None;
                session.tx_id = None;
                session.tx_vshard = None;
                session.tx_read_set.clear();
                session.savepoints.clear();
                session.pending_offset_commits.clear();
                std::mem::take(&mut session.pending_sequence_reservations)
            })
            .unwrap_or_default();
        Ok(reservations)
    }

    /// Mark the current transaction as failed (after a query error inside BEGIN).
    pub fn fail_transaction(&self, addr: &SocketAddr) {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.tx_state = TransactionState::Failed;
            }
        });
    }

    /// Create a savepoint at the current tx_buffer position.
    ///
    /// `value_marker` / `graph_marker` are the Data-Plane value/TTL and GRAPH
    /// overlay undo-journal lengths captured on the transaction's home vShard
    /// (via `MetaOp::MarkSavepoint`), so a later ROLLBACK TO can rewind both
    /// staging overlays to exactly this point.
    pub fn create_savepoint(
        &self,
        addr: &SocketAddr,
        name: String,
        value_marker: usize,
        graph_marker: usize,
    ) {
        self.write_session(addr, |session| {
            let pos = session.tx_buffer.len();
            session
                .savepoints
                .push((name, pos, value_marker, graph_marker));
        });
    }

    /// Release a savepoint: destroy the named savepoint and every savepoint
    /// established after it, keeping their buffered/staged effects (PostgreSQL
    /// semantics). Returns `Err` (SQLSTATE 3B001) if the name does not exist.
    pub fn release_savepoint(&self, addr: &SocketAddr, name: &str) -> crate::Result<()> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|(n, _, _, _)| n == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            session.savepoints.truncate(pos);
            Ok(())
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Rollback to a savepoint: truncate tx_buffer to the saved position and
    /// return the `(value_marker, graph_marker)` overlay journal markers the
    /// caller must rewind the two Data-Plane staging overlays to.
    ///
    /// Returns `Err` if the savepoint does not exist (matches PostgreSQL behavior).
    pub fn rollback_to_savepoint(
        &self,
        addr: &SocketAddr,
        name: &str,
    ) -> crate::Result<(usize, usize)> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|(n, _, _, _)| n == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            let buffer_pos = session.savepoints[pos].1;
            let value_marker = session.savepoints[pos].2;
            let graph_marker = session.savepoints[pos].3;
            session.tx_buffer.truncate(buffer_pos);
            session.savepoints.truncate(pos + 1);
            Ok((value_marker, graph_marker))
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }
}
