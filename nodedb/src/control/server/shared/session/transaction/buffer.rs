// SPDX-License-Identifier: BUSL-1.1

//! The transaction's buffered write tasks, read-set, and descriptor leases.

use std::sync::Arc;

use nodedb_cluster::DescriptorId;
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::lease::QueryLeaseScope;

use super::super::connection::SessionId;
use super::super::read_set::ReadSetEntry;
use super::super::state::TransactionState;
use super::super::store::SessionStore;

impl SessionStore {
    /// Append captured read-set entries for write conflict detection.
    ///
    /// The single write path behind [`super::super::read_set::record_read_set`]: the
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

    /// Drain the read-set for conflict checking at COMMIT time.
    pub fn take_read_set(&self, addr: impl Into<SessionId>) -> Vec<ReadSetEntry> {
        self.write_session(addr, |session| std::mem::take(&mut session.tx_read_set))
            .unwrap_or_default()
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

    /// Every distinct `(descriptor, version)` pair this transaction's
    /// buffered tasks were planned against.
    ///
    /// Scopes are deduplicated by identity: one statement attaches the same
    /// `Arc<QueryLeaseScope>` to every task it buffered, so walking the
    /// holders without deduplicating repeats one statement's holds once per
    /// task it produced.
    pub fn tx_descriptor_versions(&self, addr: impl Into<SessionId>) -> Vec<(DescriptorId, u64)> {
        self.read_session(addr, |session| {
            let mut scopes: Vec<&Arc<QueryLeaseScope>> = Vec::new();
            for scope in session.tx_lease_scopes.iter().flatten() {
                if !scopes.iter().any(|seen| Arc::ptr_eq(seen, scope)) {
                    scopes.push(scope);
                }
            }
            scopes
                .into_iter()
                .flat_map(|scope| scope.descriptor_versions().iter().cloned())
                .collect()
        })
        .unwrap_or_default()
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
}
