// SPDX-License-Identifier: BUSL-1.1

//! BEGIN / COMMIT / ROLLBACK and the transaction-state accessors.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::lease::QueryLeaseScope;
use crate::types::{Lsn, TxnId, VShardId};

use super::super::connection::SessionId;
use super::super::state::TransactionState;
use super::super::store::SessionStore;

/// Drained on COMMIT: the transaction's buffered write tasks paired with the
/// descriptor lease scope each was planned under.
pub type CommitDrain = (Vec<PhysicalTask>, Vec<Option<Arc<QueryLeaseScope>>>);

/// Global monotonic counter minting `TxnId`s across all sessions on this
/// shard. Unique per shard for the lifetime of the process — sufficient
/// for keying the per-transaction staging overlay, which is scoped to a
/// single shard's in-memory state.
static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

impl SessionStore {
    /// Get transaction state for a connection.
    pub fn transaction_state(&self, addr: impl Into<SessionId>) -> TransactionState {
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
        addr: impl Into<SessionId>,
        current_lsn: Lsn,
        snapshot_epoch: u64,
    ) -> Result<(), &'static str> {
        self.write_session(addr, |session| match session.tx_state {
            TransactionState::Idle => {
                session.tx_state = TransactionState::InBlock;
                session.tx_snapshot_lsn = Some(current_lsn);
                session.tx_snapshot_epoch = Some(snapshot_epoch);
                session.tx_read_set.clear();
                session.tx_reservation_vshards.clear();
                session.tx_reservation_owner = None;
                session.tx_id = Some(TxnId::new(NEXT_TXN_ID.fetch_add(1, Ordering::Relaxed)));
                session.tx_vshards.clear();
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

    /// Whether the connection at `addr` is inside a transaction block. Mirrors
    /// the `tx_state == InBlock` gate the read-set recording uses internally, so
    /// the hot-key reservation seam can skip autocommit reads without duplicating
    /// the predicate.
    pub fn is_in_transaction_block(&self, addr: impl Into<SessionId>) -> bool {
        self.read_session(addr, |s| s.tx_state == TransactionState::InBlock)
            .unwrap_or(false)
    }

    /// Get the snapshot LSN for the current transaction.
    pub fn snapshot_lsn(&self, addr: impl Into<SessionId>) -> Option<Lsn> {
        self.read_session(addr, |s| s.tx_snapshot_lsn)?
    }

    /// Get the cross-shard snapshot epoch for the current transaction.
    pub fn snapshot_epoch(&self, addr: impl Into<SessionId>) -> Option<u64> {
        self.read_session(addr, |s| s.tx_snapshot_epoch)?
    }

    /// Current transaction's overlay id, for stamping a `StageWrite` task
    /// before it is dispatched. `None` outside a transaction block.
    pub fn tx_id(&self, addr: impl Into<SessionId>) -> Option<TxnId> {
        self.read_session(addr, |s| s.tx_id).flatten()
    }

    /// Snapshot the current transaction's overlay identity (id + the SET of
    /// vShards it has staged writes to) WITHOUT clearing it. Called before
    /// `rollback()` releases session state so the caller can dispatch
    /// `MetaOp::DropTxnOverlay` to EVERY vShard hosting a staging overlay, and by
    /// savepoint mark/rewind to fan the overlay meta-op over all staged vShards.
    /// The returned Vec is empty when no write has staged yet.
    pub fn txn_identity(&self, addr: impl Into<SessionId>) -> (Option<TxnId>, Vec<VShardId>) {
        self.read_session(addr, |s| (s.tx_id, s.tx_vshards.iter().copied().collect()))
            .unwrap_or((None, Vec::new()))
    }

    /// COMMIT — drain the write buffer and pending offset commits, return to idle.
    ///
    /// Returns buffered write tasks and their aligned descriptor-lease scope
    /// holders. The caller retains the holders until its durable batch has
    /// flushed, then releases them before any step that drains prior-version
    /// leases — a hold the committing session still owns can never drain.
    pub fn commit(&self, addr: impl Into<SessionId>) -> Result<CommitDrain, &'static str> {
        self.write_session(addr, |session| {
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            let buffer = std::mem::take(&mut session.tx_buffer);
            let lease_scopes = std::mem::take(&mut session.tx_lease_scopes);
            session.tx_state = TransactionState::Idle;
            session.tx_snapshot_lsn = None;
            session.tx_snapshot_epoch = None;
            session.tx_id = None;
            session.tx_vshards.clear();
            session.tx_reservation_vshards.clear();
            session.tx_reservation_owner = None;
            session.savepoints.clear();
            // Note: pending_sequence_reservations and pending_field_inference
            // are taken separately (take_pending_reservations /
            // take_pending_field_inference) so the caller can finalize them
            // against services this borrow has no access to.
            Ok((buffer, lease_scopes))
        })
        .unwrap_or(Ok((Vec::new(), Vec::new())))
    }

    /// ROLLBACK — discard the write buffer and return to idle.
    /// Returns any pending GAP_FREE reservations that need to be rolled back.
    pub fn rollback(
        &self,
        addr: impl Into<SessionId>,
    ) -> Result<Vec<crate::control::sequence::gap_free::ReservationHandle>, &'static str> {
        let reservations = self
            .write_session(addr, |session| {
                debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
                session.tx_buffer.clear();
                session.tx_lease_scopes.clear();
                session.tx_state = TransactionState::Idle;
                session.tx_snapshot_lsn = None;
                session.tx_snapshot_epoch = None;
                session.tx_id = None;
                session.tx_vshards.clear();
                session.tx_read_set.clear();
                session.tx_reservation_vshards.clear();
                session.tx_reservation_owner = None;
                session.savepoints.clear();
                session.pending_offset_commits.clear();
                session.pending_field_inference.clear();
                std::mem::take(&mut session.pending_sequence_reservations)
            })
            .unwrap_or_default();
        Ok(reservations)
    }

    /// Mark the current transaction as failed (after a query error inside BEGIN).
    pub fn fail_transaction(&self, addr: impl Into<SessionId>) {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.tx_state = TransactionState::Failed;
            }
        });
    }
}
