// SPDX-License-Identifier: BUSL-1.1

//! Post-frame channel registration and server-push delivery.

use std::sync::Arc;

use futures::SinkExt;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tracing::warn;

use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;

use super::channels::{Flow, SessionChannels};
use super::row_redaction::RowPushRedaction;

type Ws = WebSocketStream<tokio::net::TcpStream>;

/// Register the session's delivery channels once it has authenticated.
///
/// Every registration is gated on `session.authenticated`: an unauthenticated
/// socket must not be wired into any server-push fanout.
pub(super) async fn register_channels(
    session: &SyncSession,
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
    session_id: &str,
) {
    if !session.authenticated {
        return;
    }
    let Some(shared) = shared.as_ref() else {
        return;
    };

    // Every fanout below is registered under the session's authenticated
    // database, so a server push is only ever routed to sessions holding that
    // database — the session ID alone does not scope delivery.
    let database_id = session.database_id();

    if !channels.crdt_registered {
        let tenant_id = session.tenant_id.map(|t| t.as_u64()).unwrap_or(0);
        let peer_id = session.device_metadata.peer_id;
        let config = crate::event::crdt_sync::types::DeliveryConfig::default();
        let (drx, crx) = shared.crdt_sync_delivery.register(
            session_id.to_string(),
            peer_id,
            tenant_id,
            database_id,
            Vec::new(),
            &config,
        );
        channels.crdt_delivery_rx = Some(drx);
        channels.crdt_control_rx = Some(crx);
        channels.crdt_registered = true;
    }

    if !channels.array_delivery_registered {
        channels.array_delivery_rx = Some(shared.array_delivery.register(session_id.to_string()));
        channels.array_delivery_registered = true;
    }

    if !channels.definition_sync_registered {
        channels.definition_sync_rx = Some(shared.definition_sync_fanout.register(
            session_id.to_string(),
            session.tenant_id.map(|t| t.as_u64()).unwrap_or(0),
            database_id,
        ));
        channels.definition_sync_registered = true;
    }

    if !channels.presence_registered {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        shared.presence.write().await.register_session(
            session_id.to_string(),
            crate::control::server::sync::presence::SessionSender::new(tx),
        );
        channels.presence_rx = Some(rx);
        channels.presence_registered = true;
    }
}

/// Drain every outbound channel into the socket.
pub(super) async fn pump_deliveries(
    ws: &mut Ws,
    session: &mut SyncSession,
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
) -> Flow {
    if let Some(ref mut rx) = channels.presence_rx {
        while let Ok(bytes) = rx.try_recv() {
            if ws
                .send(Message::Binary((*bytes).clone().into()))
                .await
                .is_err()
            {
                return Flow::Break;
            }
        }
    }

    for rx in [
        channels.array_delivery_rx.as_mut(),
        channels.definition_sync_rx.as_mut(),
    ]
    .into_iter()
    .flatten()
    {
        while let Ok(frame_bytes) = rx.try_recv() {
            if ws.send(Message::Binary(frame_bytes.into())).await.is_err() {
                return Flow::Break;
            }
        }
    }

    if let Some(ref mut rx) = channels.crdt_control_rx {
        while let Ok(frame) = rx.try_recv() {
            if ws
                .send(Message::Binary(frame.to_bytes().into()))
                .await
                .is_err()
            {
                return Flow::Break;
            }
        }
    }

    pump_crdt_deltas(ws, session, channels, shared).await
}

async fn pump_crdt_deltas(
    ws: &mut Ws,
    session: &mut SyncSession,
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
) -> Flow {
    let Some(ref mut rx) = channels.crdt_delivery_rx else {
        return Flow::Continue;
    };

    // The delivery channel is only registered when `SharedState` is present,
    // so a session with deltas to drain always has one.
    let Some(state) = shared.as_ref() else {
        return Flow::Continue;
    };

    // A row push carries a stored row post-image, so the subscriber's column
    // redaction applies to it exactly as it would to the same row read over
    // SQL. Resolved once for the whole drain — see `row_redaction`.
    let Some(mut redaction) = RowPushRedaction::for_session(state, session) else {
        warn!(
            session = %session.session_id,
            "sync: row push suppressed; the session has no established identity to \
             evaluate column redaction against"
        );
        return Flow::Break;
    };

    while let Ok(delta) = rx.try_recv() {
        // Announce the collection descriptor before its first delta so schema
        // strictly precedes data on the peer. Idempotent per session; a lookup
        // miss warns and proceeds without marking.
        if let Some(schema_frame) = super::super::announce::build_collection_schema_frame(
            state,
            session,
            delta.tenant_id,
            session.database_id(),
            &delta.collection,
        ) {
            if ws
                .send(Message::Binary(schema_frame.to_bytes().into()))
                .await
                .is_err()
            {
                return Flow::Break;
            }
            session
                .announced_collections
                .insert(delta.collection.clone());
        }

        // Redact the post-image before it goes out: the device persists what
        // it receives, so an unredacted push leaves the protected value on the
        // device permanently. A payload a rule covers but that cannot be
        // rewritten is dropped rather than delivered.
        let mut payload = delta.payload;
        if !redaction.redact(&state.redaction, &delta.collection, &mut payload) {
            warn!(
                session = %session.session_id,
                collection = %delta.collection,
                "sync: row push dropped; a row covered by a redaction policy could not \
                 be rewritten"
            );
            continue;
        }

        // A row post-image, not a Loro delta — so this goes out as `RowPush`,
        // never `DeltaPush`. The two carry different encodings, and the peer
        // cannot tell them apart from the bytes alone.
        let push_msg = nodedb_types::sync::wire::RowPushMsg {
            collection: delta.collection,
            document_id: delta.document_id,
            payload,
            op: match delta.op {
                crate::event::crdt_sync::types::DeltaOp::Upsert => {
                    nodedb_types::sync::wire::RowOp::Upsert
                }
                crate::event::crdt_sync::types::DeltaOp::Delete => {
                    nodedb_types::sync::wire::RowOp::Delete
                }
            },
            lsn: delta.lsn,
            peer_id: delta.peer_id,
            sequence: delta.sequence,
        };
        if let Some(frame) = nodedb_types::sync::wire::SyncFrame::new_msgpack(
            nodedb_types::sync::wire::SyncMessageType::RowPush,
            &push_msg,
        ) && ws
            .send(Message::Binary(frame.to_bytes().into()))
            .await
            .is_err()
        {
            return Flow::Break;
        }
    }

    Flow::Continue
}
