// SPDX-License-Identifier: BUSL-1.1

//! Side effects deferred to COMMIT: consumer-group offsets and inferred
//! schema fields.

use super::super::connection::SessionId;
use super::super::state::{PendingFieldInference, PendingOffsetCommit, TransactionState};
use super::super::store::SessionStore;

impl SessionStore {
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

    /// Take the schema fields this transaction's writes inferred (called after
    /// a successful COMMIT dispatch).
    pub fn take_pending_field_inference(
        &self,
        addr: impl Into<SessionId>,
    ) -> Vec<PendingFieldInference> {
        self.write_session(addr, |session| {
            std::mem::take(&mut session.pending_field_inference)
        })
        .unwrap_or_default()
    }

    /// Defer recording inferred schema fields until the current transaction
    /// commits. Outside a block nothing is deferred and `pending` is handed
    /// back, for the caller to record immediately.
    pub fn defer_field_inference(
        &self,
        addr: impl Into<SessionId>,
        pending: PendingFieldInference,
    ) -> Option<PendingFieldInference> {
        // Carried rather than moved into the closure: an unknown session never
        // runs it, and dropping `pending` there would silently lose the fields.
        let mut carried = Some(pending);
        let _ran = self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock
                && let Some(pending) = carried.take()
            {
                session.pending_field_inference.push(pending);
            }
        });
        carried
    }
}
