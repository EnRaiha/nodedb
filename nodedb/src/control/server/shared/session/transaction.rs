// SPDX-License-Identifier: BUSL-1.1

//! Transaction lifecycle methods on SessionStore.

use super::connection::SessionId;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_cluster::calvin::types::TxnIdWire;

use crate::control::lease::QueryLeaseScope;
use crate::types::{Lsn, TxnId, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::read_set::ReadSetEntry;
use super::state::{PendingOffsetCommit, SavepointEntry, TransactionState};
use super::store::SessionStore;

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

    /// Append captured read-set entries for write conflict detection.
    ///
    /// The single write path behind [`super::read_set::record_read_set`]: the
    /// neutral capture helper builds one [`ReadSetEntry`] per observed shard and
    /// hands them here. Guarded on the connection being inside a transaction
    /// block — outside one, the entries are dropped (autocommit reads never
    /// enter validation).
    pub fn record_read_entries(&self, addr: impl Into<SessionId>, entries: Vec<ReadSetEntry>) {
        if entries.is_empty() {
            return;
        }
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.tx_read_set.extend(entries);
            }
        });
    }

    /// Whether the connection at `addr` is inside a transaction block. Mirrors
    /// the `tx_state == InBlock` gate the read-set recording uses internally, so
    /// the hot-key reservation seam can skip autocommit reads without duplicating
    /// the predicate.
    pub fn is_in_transaction_block(&self, addr: impl Into<SessionId>) -> bool {
        self.read_session(addr, |s| s.tx_state == TransactionState::InBlock)
            .unwrap_or(false)
    }

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

    /// Collect a value from each buffered write task's plan. Used at commit to
    /// gather the collections this transaction wrote, so its own reads of those
    /// collections are excluded from snapshot-isolation conflict detection
    /// (a read-your-own-write is not a serialization conflict).
    pub fn buffered_collections<F>(
        &self,
        addr: impl Into<SessionId>,
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

    /// Clone the current transaction's buffered write tasks WITHOUT consuming
    /// them or transitioning session state, so COMMIT can classify dispatch off
    /// the buffered writes while still holding the option to `rollback` on a
    /// conflict. `commit()` remains the consuming drain.
    pub fn buffered_tasks(&self, addr: impl Into<SessionId>) -> Vec<PhysicalTask> {
        self.read_session(addr, |s| s.tx_buffer.clone())
            .unwrap_or_default()
    }

    /// Drain the read-set for conflict checking at COMMIT time.
    pub fn take_read_set(&self, addr: impl Into<SessionId>) -> Vec<ReadSetEntry> {
        self.write_session(addr, |session| std::mem::take(&mut session.tx_read_set))
            .unwrap_or_default()
    }

    /// COMMIT — drain the write buffer and pending offset commits, return to idle.
    ///
    /// Returns buffered write tasks and their aligned descriptor-lease scope
    /// holders. The caller must retain the holders through its complete commit
    /// response and cleanup lifecycle.
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
            // Note: pending_sequence_reservations are taken separately via
            // take_pending_reservations() so the caller can finalize them
            // with the GAP_FREE manager (which requires Arc<SequenceRegistry>).
            Ok((buffer, lease_scopes))
        })
        .unwrap_or(Ok((Vec::new(), Vec::new())))
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

    /// Take pending offset commits (called after successful COMMIT dispatch).
    pub fn take_pending_offsets(&self, addr: impl Into<SessionId>) -> Vec<PendingOffsetCommit> {
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
        addr: impl Into<SessionId>,
        pending_offset: PendingOffsetCommit,
    ) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.pending_offset_commits.push(pending_offset);
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
    pub fn buffer_write(&self, addr: impl Into<SessionId>, mut task: PhysicalTask) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                task.txn_id = session.tx_id;
                session.tx_vshards.insert(task.vshard_id);
                session.tx_buffer.push(task);
                session.tx_lease_scopes.push(None);
                debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    /// Number of tasks currently buffered for this transaction.
    pub fn buffered_task_count(&self, addr: impl Into<SessionId>) -> usize {
        self.read_session(addr, |session| {
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            session.tx_buffer.len()
        })
        .unwrap_or(0)
    }

    /// Retain a statement's descriptor lease scope for every task buffered
    /// since `start`. Fails closed when the transaction state or the aligned
    /// holders are invalid, or when a different statement already owns one.
    pub fn attach_tx_lease_scope_since(
        &self,
        addr: impl Into<SessionId>,
        start: usize,
        scope: Arc<QueryLeaseScope>,
    ) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state != TransactionState::InBlock
                || session.tx_buffer.len() != session.tx_lease_scopes.len()
                || start > session.tx_buffer.len()
            {
                return false;
            }
            for holder in &mut session.tx_lease_scopes[start..] {
                if let Some(existing) = holder
                    && !Arc::ptr_eq(existing, &scope)
                {
                    return false;
                }
            }
            for holder in &mut session.tx_lease_scopes[start..] {
                if holder.is_none() {
                    *holder = Some(Arc::clone(&scope));
                }
            }
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            true
        })
        .unwrap_or(false)
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

    /// Create a savepoint at the current tx_buffer position.
    ///
    /// `markers` maps each vShard that had staged writes at savepoint time to its
    /// Data-Plane value/TTL and GRAPH overlay undo-journal lengths (captured via
    /// `MetaOp::MarkSavepoint`), so a later ROLLBACK TO can rewind every staging
    /// overlay to exactly this point.
    pub fn create_savepoint(
        &self,
        addr: impl Into<SessionId>,
        name: String,
        markers: BTreeMap<VShardId, (usize, usize)>,
    ) {
        self.write_session(addr, |session| {
            let buffer_len = session.tx_buffer.len();
            let pending_offset_len = session.pending_offset_commits.len();
            session.savepoints.push(SavepointEntry {
                name,
                buffer_len,
                pending_offset_len,
                markers,
            });
        });
    }

    /// Release a savepoint: destroy the named savepoint and every savepoint
    /// established after it, keeping their buffered/staged effects (PostgreSQL
    /// semantics). Returns `Err` (SQLSTATE 3B001) if the name does not exist.
    pub fn release_savepoint(&self, addr: impl Into<SessionId>, name: &str) -> crate::Result<()> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|e| e.name == name)
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
    /// return the per-vShard `(value_marker, graph_marker)` overlay journal
    /// markers the caller must rewind each staged vShard's Data-Plane staging
    /// overlays to. A vShard first staged AFTER the savepoint is absent from the
    /// returned map; the caller rewinds it to `(0, 0)`.
    ///
    /// Returns `Err` if the savepoint does not exist (matches PostgreSQL behavior).
    pub fn rollback_to_savepoint(
        &self,
        addr: impl Into<SessionId>,
        name: &str,
    ) -> crate::Result<BTreeMap<VShardId, (usize, usize)>> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|e| e.name == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            let buffer_len = session.savepoints[pos].buffer_len;
            let pending_offset_len = session.savepoints[pos].pending_offset_len;
            let markers = session.savepoints[pos].markers.clone();
            if session.tx_buffer.len() != session.tx_lease_scopes.len() {
                return Err(crate::Error::Internal {
                    detail: "transaction lease scope holders are misaligned".into(),
                });
            }
            session.tx_buffer.truncate(buffer_len);
            session.tx_lease_scopes.truncate(buffer_len);
            session.pending_offset_commits.truncate(pending_offset_len);
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            session.savepoints.truncate(pos + 1);
            Ok(markers)
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};
    use nodedb_physical::physical_task::PostSetOp;

    fn task() -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(1),
            plan: PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn savepoint_rollback_truncates_aligned_lease_holders() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:6010".parse().expect("address");
        store.ensure_session(addr);
        store.begin(addr, Lsn::new(1), 0).expect("begin");

        let scope = Arc::new(QueryLeaseScope::empty());
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 0, Arc::clone(&scope)));
        store.create_savepoint(addr, "sp".into(), BTreeMap::new());
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 1, Arc::clone(&scope)));

        store
            .rollback_to_savepoint(addr, "sp")
            .expect("rollback to savepoint");
        store.read_session(addr, |session| {
            assert_eq!(session.tx_buffer.len(), 1);
            assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            assert!(session.tx_lease_scopes[0].is_some());
        });
    }

    #[test]
    fn rollback_to_savepoint_discards_deferred_offsets_after_the_mark() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:6013".parse().expect("address");
        store.ensure_session(addr);
        store.begin(addr, Lsn::new(1), 0).expect("begin");

        let before = PendingOffsetCommit {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            stream: "orders".into(),
            group: "analytics".into(),
            partition_id: 0,
            offset: crate::event::cdc::CdcOffset::new(10, 1),
        };
        assert!(store.defer_offset_commit(addr, before));
        store.create_savepoint(addr, "sp".into(), BTreeMap::new());
        assert!(store.defer_offset_commit(
            addr,
            PendingOffsetCommit {
                database_id: DatabaseId::DEFAULT,
                tenant_id: 1,
                stream: "orders".into(),
                group: "analytics".into(),
                partition_id: 0,
                offset: crate::event::cdc::CdcOffset::new(20, 1),
            },
        ));

        store
            .rollback_to_savepoint(addr, "sp")
            .expect("rollback to savepoint");
        let pending = store.take_pending_offsets(addr);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].offset, crate::event::cdc::CdcOffset::new(10, 1));
    }

    #[test]
    fn commit_returns_lease_holders_after_transitioning_session_to_idle() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:6012".parse().expect("address");
        store.ensure_session(addr);
        store.begin(addr, Lsn::new(1), 0).expect("begin");

        let scope = Arc::new(QueryLeaseScope::empty());
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 0, Arc::clone(&scope)));

        let (tasks, holders) = store.commit(addr).expect("commit");
        assert_eq!(tasks.len(), 1);
        assert_eq!(holders.len(), 1);
        assert!(
            holders[0]
                .as_ref()
                .is_some_and(|holder| Arc::ptr_eq(holder, &scope))
        );
        assert_eq!(store.transaction_state(addr), TransactionState::Idle);
        store.read_session(addr, |session| {
            assert!(session.tx_buffer.is_empty());
            assert!(session.tx_lease_scopes.is_empty());
        });

        // The returned holders, which `run_commit` owns, keep the scope alive
        // after the session has transitioned to Idle.
        assert_eq!(Arc::strong_count(&scope), 2);
        drop(holders);
        assert_eq!(Arc::strong_count(&scope), 1);
    }

    #[test]
    fn rollback_and_database_switch_clear_aligned_lease_holders() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:6011".parse().expect("address");
        store.ensure_session(addr);
        let scope = Arc::new(QueryLeaseScope::empty());
        store.begin(addr, Lsn::new(1), 0).expect("begin");
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 0, Arc::clone(&scope)));
        store.rollback(addr).expect("rollback");
        store.read_session(addr, |session| {
            assert!(session.tx_buffer.is_empty());
            assert!(session.tx_lease_scopes.is_empty());
            assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
        });

        store.begin(addr, Lsn::new(2), 0).expect("begin");
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 0, scope));
        store.reset_for_database_switch(addr, DatabaseId::new(2));
        store.read_session(addr, |session| {
            assert!(session.tx_buffer.is_empty());
            assert!(session.tx_lease_scopes.is_empty());
            assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
        });
    }
}
