// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral BEGIN and ROLLBACK orchestration.
//!
//! Both drive the neutral session state plus the DDL buffer and, for ROLLBACK,
//! the GAP_FREE reservation rollback, sequence-log audit, cursor/notify
//! discard, and staging-overlay release via the injected [`TxnDataPlane`].
//! Transports only shape the returned tag / error.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::connection::SessionId;
use super::ddl_buffer;
use super::outcome::TxnDataPlane;
use super::overlay_drop::drop_txn_overlay;
use super::store::SessionStore;

/// Run the neutral BEGIN sequence: anchor the snapshot LSN, activate the DDL
/// buffer, and enter the transaction block. Returns the session error (mapped
/// to the transport's `25P02`) if the connection cannot begin a transaction.
pub fn run_begin(
    sessions: &SessionStore,
    session_id: SessionId,
    state: &SharedState,
) -> Result<(), crate::Error> {
    let snapshot_lsn = {
        let next = state.wal.next_lsn();
        crate::types::Lsn::new(next.as_u64().saturating_sub(1))
    };
    // Last globally-applied Calvin epoch as the cross-shard snapshot anchor.
    // 0 in single-node / no-Calvin deployments (the atomic is never advanced).
    let snapshot_epoch = state
        .last_applied_calvin_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    ddl_buffer::activate();
    sessions
        .begin(session_id, snapshot_lsn, snapshot_epoch)
        .map_err(|msg| crate::Error::BadRequest {
            detail: msg.to_owned(),
        })
}

/// Run the neutral ROLLBACK sequence.
///
/// Discards the DDL buffer, rolls back GAP_FREE reservations (with sequence-log
/// audit), clears the write buffer + read-set, closes non-hold cursors,
/// discards buffered NOTIFY messages, and releases the staging overlay on its
/// home vShard. Infallible — every cleanup step is best-effort, mirroring the
/// original swallow-on-error behavior.
pub async fn run_rollback(
    sessions: &SessionStore,
    session_id: SessionId,
    identity: &AuthenticatedIdentity,
    state: &SharedState,
    dp: &impl TxnDataPlane,
) {
    ddl_buffer::discard();
    // Snapshot the overlay identity BEFORE `rollback()` clears session state,
    // so the staging overlay can be released on EVERY vShard the transaction
    // staged writes to (a transaction may span multiple cores).
    let (overlay_txn_id, overlay_vshards) = sessions.txn_identity(session_id);
    // Release this transaction's read reservations while the reservation owner is
    // still set — `rollback` below clears it. Best-effort; lease GC backstops.
    super::reservation_release::release_session_reservations(
        state,
        sessions,
        session_id,
        nodedb_cluster::calvin::types::ReleaseReason::Abort,
    )
    .await;
    // Keep the session's transaction identity intact until every overlay has
    // been released. Detached connection teardown can be cancelled at any
    // await point; clearing `tx_id` first would make an interrupted cleanup
    // permanently lose the only identifiers needed to reclaim the overlays.
    if let Some(txn_id) = overlay_txn_id {
        for vshard_id in overlay_vshards {
            // Teardown of an aborted transaction's overlay: surface a failure at
            // ERROR (the overlay, keyed by `txn_id`, is reclaimable by its holder)
            // and continue reaping the remaining vShards. ROLLBACK is infallible
            // for the client, so there is no outcome to change here.
            if let Err(e) = drop_txn_overlay(state, dp, identity.tenant_id, vshard_id, txn_id).await
            {
                tracing::error!(
                    vshard = vshard_id.as_u32(),
                    error = %e,
                    "failed to release per-transaction staging overlay on rollback"
                );
            }
        }
    }

    let reservations = sessions.rollback(session_id).unwrap_or_default();
    for handle in &reservations {
        let key = &handle.sequence_key;
        let registry = &state.sequence_registry;
        registry.gap_free_manager().rollback(handle, || {
            let map = registry.sequences_read();
            if let Some(h) = map.get(key.as_str()) {
                h.rollback_one();
            }
        });
        {
            let catalog = state.credentials.catalog();
            crate::control::sequence::log::log_reservation(
                catalog,
                &crate::control::sequence::log::rolled_back(
                    key,
                    handle.value,
                    &identity.username,
                    identity.tenant_id.as_u64(),
                ),
            );
        }
    }
    sessions.close_non_hold_cursors(session_id);
    // Discard NOTIFY messages buffered during this transaction.
    sessions.discard_pending_notifies(session_id);
}
