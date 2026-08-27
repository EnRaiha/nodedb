// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard read-reservation bookkeeping for an open transaction.

use nodedb_cluster::calvin::types::TxnIdWire;

use super::super::connection::SessionId;
use super::super::store::SessionStore;

impl SessionStore {
    /// The reservation owner id minted for the current transaction, if a hot-key
    /// read has already reserved one. `None` before the first hot-key read (or
    /// outside a transaction block). Short lock scope — reads and drops.
    pub fn current_reservation_owner(&self, addr: impl Into<SessionId>) -> Option<TxnIdWire> {
        self.read_session(addr, |s| s.tx_reservation_owner)
            .flatten()
    }

    /// Record a sequenced SHARED reservation taken on a hot point key. Inserts
    /// the reservation's owning `vshard` into the transaction's touched-vShard set
    /// and, on the FIRST reservation, adopts `owner` as the transaction's single
    /// reservation owner so every later hot-key read reuses the same `lock_owner`.
    /// Short lock scope — mutates and drops.
    pub fn record_reservation(&self, addr: impl Into<SessionId>, vshard: u32, owner: TxnIdWire) {
        self.write_session(addr, |session| {
            session.tx_reservation_vshards.insert(vshard);
            if session.tx_reservation_owner.is_none() {
                session.tx_reservation_owner = Some(owner);
            }
        });
    }

    /// Drain the current transaction's read reservations for release. Takes the
    /// single reservation `owner` (leaving `None`) and drains the set of distinct
    /// vShards it reserved on (leaving empty), returning `(owner, vshards)`. Short
    /// lock scope, no await held — the async release routes one
    /// `ReleaseReservation` per vShard AFTER this returns. Draining makes a repeat
    /// call a no-op, so two graceful-exit paths releasing is idempotent.
    pub fn take_reservations(&self, addr: impl Into<SessionId>) -> (Option<TxnIdWire>, Vec<u32>) {
        self.write_session(addr, |session| {
            let owner = session.tx_reservation_owner.take();
            let vshards = std::mem::take(&mut session.tx_reservation_vshards)
                .into_iter()
                .collect();
            (owner, vshards)
        })
        .unwrap_or((None, Vec::new()))
    }

    /// Take pending GAP_FREE sequence reservations (called after successful COMMIT).
    pub fn take_pending_reservations(
        &self,
        addr: impl Into<SessionId>,
    ) -> Vec<crate::control::sequence::gap_free::ReservationHandle> {
        self.write_session(addr, |session| {
            std::mem::take(&mut session.pending_sequence_reservations)
        })
        .unwrap_or_default()
    }
}
