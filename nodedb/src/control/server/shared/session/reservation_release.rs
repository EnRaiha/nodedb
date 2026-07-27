// SPDX-License-Identifier: BUSL-1.1

//! Read-reservation release and abort teardown for graceful transaction exits.

use nodedb_cluster::calvin::types::ReleaseReason;

use crate::control::state::SharedState;

use super::connection::SessionId;
use super::store::SessionStore;

/// Drain and release every read reservation this session holds, routing one
/// sequenced `ReleaseReservation` per distinct vShard. Best-effort: a failed
/// release is swallowed by `release_reservation` (lease GC backstops). Call on
/// every graceful txn exit BEFORE the session state is cleared.
pub(super) async fn release_session_reservations(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    reason: ReleaseReason,
) {
    let (owner, vshards) = sessions.take_reservations(session_id);
    let Some(owner) = owner else { return };
    for vshard in vshards {
        let _ = crate::control::planner::calvin::reservation::release_reservation(
            state, owner, vshard, reason,
        )
        .await;
    }
}

/// Standard COMMIT/abort teardown: release the transaction's read reservations
/// (with `ReleaseReason::Abort`) while the reservation owner is still set, THEN
/// roll the session back to `Idle` and free any pending GAP_FREE sequence
/// reservations. Ordering matters — `rollback_with_gap_free` clears the owner,
/// so the release must run first. Used by every COMMIT abort branch that must
/// leave the session idle without persisting; the transport adapters map
/// `Aborted` to a wire error and never roll back afterward, so each abort branch
/// owns its rollback.
pub(super) async fn release_and_rollback(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
) {
    release_session_reservations(state, sessions, session_id, ReleaseReason::Abort).await;
    rollback_with_gap_free(sessions, session_id, state);
}

/// Roll the session back to `Idle` and release any pending GAP_FREE sequence
/// reservations.
fn rollback_with_gap_free(sessions: &SessionStore, session_id: SessionId, state: &SharedState) {
    if let Ok(reservations) = sessions.rollback(session_id) {
        for handle in &reservations {
            let key = handle.sequence_key.clone();
            let registry = &state.sequence_registry;
            registry.gap_free_manager().rollback(handle, || {
                let map = registry.sequences_read();
                if let Some(h) = map.get(&key) {
                    h.rollback_one();
                }
            });
        }
    }
}
