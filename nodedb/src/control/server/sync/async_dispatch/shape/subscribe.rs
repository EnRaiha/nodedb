// SPDX-License-Identifier: BUSL-1.1

//! Shape-subscription snapshot dispatch (subscribe + resync).

use tracing::{info, warn};

use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;

use super::super::super::wire::{SyncFrame, SyncMessageType};
use super::authorize::{ShapeAuthorizationFailure, authorize_shape_subscription};
use super::snapshot::{SnapshotRequest, take_shape_snapshot};

/// Handle ShapeSubscribe with real WAL LSN and Data Plane snapshot.
pub(in crate::control::server::sync) async fn handle_shape_subscribe_async(
    shared: &SharedState,
    session: &SyncSession,
    frame: &SyncFrame,
) -> Option<SyncFrame> {
    let msg: super::super::super::shape::handler::ShapeSubscribeMsg = frame.decode_body()?;

    // Authorize before the registry records anything and before any tenant
    // accounting: a subscription the session may not read must leave no trace,
    // and must not be answered with a snapshot frame of any kind.
    let identity = match authorize_shape_subscription(shared, session, &msg.shape) {
        Ok(identity) => identity,
        Err(failure) => {
            log_refusal(&session.session_id, &msg.shape.shape_id, failure);
            return None;
        }
    };
    let tenant_id = identity.tenant_id;
    let database_id = session.database_id();

    // Quota enforcement — reject before dispatch.
    if let Err(e) = shared.check_tenant_quota(tenant_id) {
        warn!(
            tenant_id = tenant_id.as_u64(),
            error = %e,
            "sync: shape subscribe rejected by quota"
        );
        return None;
    }

    // Get current WAL LSN — this is the watermark for the snapshot.
    let current_lsn = shared.wal.next_lsn().as_u64().saturating_sub(1);

    let snapshot_data = take_shape_snapshot(SnapshotRequest {
        shared,
        session_id: &session.session_id,
        shape: &msg.shape,
        identity,
        tenant_id,
        database_id,
    })
    .await?;

    // Register the shape subscription in the persistent registry.
    let response = super::super::super::shape::handler::handle_subscribe(
        &session.session_id,
        tenant_id.as_u64(),
        &msg,
        &shared.shape_registry,
        current_lsn,
        |_shape, _lsn| snapshot_data,
    );

    info!(
        session = %session.session_id,
        shape_id = %msg.shape.shape_id,
        lsn = current_lsn,
        "shape subscribed with WAL LSN watermark"
    );

    response
}

/// Re-snapshot a previously subscribed shape in response to a ResyncRequest.
///
/// Decodes the request, re-authorizes the shape, looks it up in the persistent
/// registry, runs the same snapshot machinery as subscribe, and returns a
/// ShapeSnapshot frame re-based at the current WAL LSN.
///
/// Authorization is repeated here rather than trusted from subscribe time: a
/// grant revoked between subscribing and resyncing must take effect on the next
/// read, not at the next reconnect.
pub(in crate::control::server::sync) async fn handle_resync_request_async(
    shared: &SharedState,
    session: &SyncSession,
    frame: &SyncFrame,
) -> Option<SyncFrame> {
    use nodedb_types::sync::wire::ResyncRequestMsg;

    let msg: ResyncRequestMsg = frame.decode_body()?;

    if msg.shape_id.is_empty() {
        warn!(
            session = %session.session_id,
            "resync request missing shape_id; ignoring"
        );
        return None;
    }

    let shape = match shared
        .shape_registry
        .get_shape(&session.session_id, &msg.shape_id)
    {
        Some(s) => s,
        None => {
            warn!(
                session = %session.session_id,
                shape_id = %msg.shape_id,
                "resync for unknown or unsubscribed shape; ignoring"
            );
            return None;
        }
    };

    let identity = match authorize_shape_subscription(shared, session, &shape) {
        Ok(identity) => identity,
        Err(failure) => {
            log_refusal(&session.session_id, &msg.shape_id, failure);
            return None;
        }
    };
    let tenant_id = identity.tenant_id;
    let database_id = session.database_id();

    if let Err(e) = shared.check_tenant_quota(tenant_id) {
        warn!(
            tenant_id = tenant_id.as_u64(),
            error = %e,
            "sync: resync request rejected by quota"
        );
        return None;
    }

    let current_lsn = shared.wal.next_lsn().as_u64().saturating_sub(1);

    let snapshot_data = take_shape_snapshot(SnapshotRequest {
        shared,
        session_id: &session.session_id,
        shape: &shape,
        identity,
        tenant_id,
        database_id,
    })
    .await?;

    let snapshot = super::super::super::shape::handler::ShapeSnapshotMsg {
        shape_id: msg.shape_id.clone(),
        data: snapshot_data.data,
        snapshot_lsn: current_lsn,
        doc_count: snapshot_data.doc_count,
    };

    info!(
        session = %session.session_id,
        shape_id = %msg.shape_id,
        lsn = current_lsn,
        doc_count = snapshot.doc_count,
        "resync snapshot sent"
    );

    SyncFrame::try_encode(SyncMessageType::ShapeSnapshot, &snapshot)
}

fn log_refusal(session_id: &str, shape_id: &str, failure: ShapeAuthorizationFailure) {
    match failure {
        ShapeAuthorizationFailure::IdentityNotEstablished => warn!(
            session = session_id,
            shape_id, "shape read refused: session has no established identity"
        ),
        ShapeAuthorizationFailure::PermissionDenied => warn!(
            session = session_id,
            shape_id, "shape read refused: no read grant on the shape's collection"
        ),
    }
}
