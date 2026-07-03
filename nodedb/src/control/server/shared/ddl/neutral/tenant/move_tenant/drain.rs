// SPDX-License-Identifier: BUSL-1.1

//! Drain phase for `MOVE TENANT`.
//!
//! Revokes the tenant's active sessions via the [`SessionInvalidationBus`]
//! and waits for in-flight operations to drain within a bounded timeout.
//!
//! The `SessionInvalidationBus` carries a `user_id`. For the drain phase we
//! broadcast an invalidation event for the tenant as a synthetic user so the
//! consumer task can close sessions belonging to that tenant.  In a future
//! online-move variant this phase would be replaced by dual-write; the offline
//! v1 implementation waits for the global request counter to settle.

use std::time::Duration;

use crate::control::security::buses::{SessionInvalidated, SessionInvalidationReason};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};
use nodedb_types::NodeDbError;

/// Run the drain phase.
///
/// 1. Broadcast a session-invalidation event for the tenant so active sessions
///    are revoked by the bus consumer task.
/// 2. Poll the global in-flight request counter until it reaches zero or
///    `timeout` expires.
///
/// Returns `Ok(())` on clean drain, `Err` on timeout.
pub async fn run(
    state: &SharedState,
    tenant_id: TenantId,
    _source_db_id: DatabaseId,
    timeout: Duration,
) -> Result<(), NodeDbError> {
    // Broadcast invalidation: the tenant_id is sent as user_id so the consumer
    // task can match and revoke sessions belonging to this tenant.
    state.session_invalidation_bus.publish(SessionInvalidated {
        user_id: tenant_id.as_u64(),
        reason: SessionInvalidationReason::UserDeactivated,
    });

    // Wait for in-flight depth to reach zero.
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let depth = state.tracker.in_flight();
        if depth == 0 {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(NodeDbError::move_tenant_drain_timeout(
                tenant_id.as_u64().to_string(),
                _source_db_id.as_u64().to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Release the drain state.
///
/// For the offline v1 implementation this is a no-op — there is no persistent
/// "draining" gate to lift, only the session invalidation already broadcast.
/// Called on drain timeout (compensation) and after successful cutover.
pub fn release(_state: &SharedState, _tenant_id: TenantId, _source_db_id: DatabaseId) {
    // No persistent drain gate to release in the offline-v1 implementation.
    // The session invalidation broadcast already fired; new connections from
    // this tenant will proceed normally once the move is complete (cutover
    // updates the catalog) or aborted (error returned to client).
}
