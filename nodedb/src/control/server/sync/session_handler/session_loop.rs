// SPDX-License-Identifier: BUSL-1.1

//! WebSocket session loop for NodeDB-Lite sync connections.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tracing::{info, warn};

use super::super::listener::SyncListenerState;
use super::super::wire::{DeltaPushMsg, PresenceUpdateMsg, SyncMessageType};
use super::array::{build_array_inbound, dispatch_array_frame, is_array_frame};

use super::engine_dispatch::{EngineOutcome, dispatch_engine_frame};
use crate::control::state::SharedState;

/// Handle one sync session with full RLS, audit, DLQ wired in.
pub(in crate::control::server::sync) async fn handle_sync_session(
    mut ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    addr: SocketAddr,
    state: &SyncListenerState,
    shared: Option<Arc<SharedState>>,
) {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let session_id = format!(
        "sync-{addr}-{}",
        state.connections_accepted.load(Ordering::Relaxed)
    );
    let mut session = super::super::session::SyncSession::with_rate_limit(
        session_id.clone(),
        &state.config.rate_limit,
    );
    session.device_metadata.remote_addr = addr.to_string();

    let jwt_validator =
        crate::control::security::jwt::JwtValidator::new(state.config.jwt_config.clone());

    let mut crdt_delivery_rx: Option<
        tokio::sync::mpsc::Receiver<crate::event::crdt_sync::types::OutboundDelta>,
    > = None;
    let mut crdt_control_rx: Option<
        tokio::sync::mpsc::Receiver<nodedb_types::sync::wire::SyncFrame>,
    > = None;
    let mut crdt_registered = false;

    let mut presence_rx: Option<tokio::sync::mpsc::Receiver<std::sync::Arc<Vec<u8>>>> = None;
    let mut presence_registered = false;

    // Built lazily on the first array frame, once the handshake has established
    // the session's tenant — the inbound engine binds that tenant for Raft-log
    // routing and shape fan-out (see `build_array_inbound`).
    let mut array_inbound: Option<Arc<crate::control::array_sync::OriginArrayInbound>> = None;

    let mut array_delivery_rx: Option<tokio::sync::mpsc::Receiver<Vec<u8>>> = None;
    let mut array_delivery_registered = false;

    let mut definition_sync_rx: Option<tokio::sync::mpsc::Receiver<Vec<u8>>> = None;
    let mut definition_sync_registered = false;

    loop {
        // Flush any outbound definition-sync frames before blocking. This
        // handles the window between registration and the next WS message.
        if let Some(ref mut rx) = definition_sync_rx {
            while let Ok(frame_bytes) = rx.try_recv() {
                if ws.send(Message::Binary(frame_bytes.into())).await.is_err() {
                    return;
                }
            }
        }

        // Await the next inbound message OR a definition-sync frame, whichever
        // arrives first.  Without this select! the handler would block on
        // ws.next() indefinitely when no client traffic is expected, starving
        // the server-push delivery path.
        let msg_result = if let Some(ref mut rx) = definition_sync_rx {
            tokio::select! {
                biased;
                ws_msg = ws.next() => {
                    match ws_msg {
                        Some(r) => r,
                        None => break,
                    }
                }
                frame_bytes = rx.recv() => {
                    match frame_bytes {
                        Some(bytes) => {
                            if ws.send(Message::Binary(bytes.into())).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        None => break,
                    }
                }
            }
        } else {
            match ws.next().await {
                Some(r) => r,
                None => break,
            }
        };

        match msg_result {
            Ok(Message::Binary(data)) => {
                if let Some(frame) = super::super::wire::SyncFrame::from_bytes(&data) {
                    if frame.msg_type == SyncMessageType::ResyncRequest
                        && let Some(shared) = shared.as_ref()
                    {
                        let response = super::super::async_dispatch::handle_resync_request_async(
                            shared, &session, &frame,
                        )
                        .await;
                        if let Some(r) = response
                            && ws.send(Message::Binary(r.to_bytes().into())).await.is_err()
                        {
                            break;
                        }
                        continue;
                    }

                    if frame.msg_type == SyncMessageType::ShapeSubscribe
                        && let Some(shared) = shared.as_ref()
                        && let Some(response) =
                            super::super::async_dispatch::handle_shape_subscribe_async(
                                shared, &session, &frame,
                            )
                            .await
                    {
                        // Decode once, reused for both the presence-channel
                        // subscribe and the schema-announce below (avoids a
                        // redundant second msgpack decode of the same body).
                        let shape_sub_msg = if session.authenticated {
                            frame.decode_body::<super::super::wire::ShapeSubscribeMsg>()
                        } else {
                            None
                        };

                        if let Some(sub_msg) = shape_sub_msg.as_ref()
                            && let Some(coll) = sub_msg.shape.collection()
                        {
                            if presence_registered {
                                let channel = format!("shape:{coll}");
                                shared
                                    .presence
                                    .write()
                                    .await
                                    .subscribe_to_channel(&session_id, &channel);
                            }

                            // Announce the collection descriptor before the shape
                            // snapshot so schema strictly precedes data on the
                            // subscription path. Idempotent per session; skips shape
                            // variants that carry no single collection.
                            let tenant_id = session.tenant_id.map(|t| t.as_u64()).unwrap_or(0);
                            if let Some(schema_frame) =
                                super::announce::build_collection_schema_frame(
                                    shared, &session, tenant_id, coll,
                                )
                            {
                                if ws
                                    .send(Message::Binary(schema_frame.to_bytes().into()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                session.announced_collections.insert(coll.to_string());
                            }
                        }

                        if ws
                            .send(Message::Binary(response.to_bytes().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }

                    if frame.msg_type == SyncMessageType::PresenceUpdate
                        && session.authenticated
                        && let Some(shared) = shared.as_ref()
                    {
                        if let Some(msg) = frame.decode_body::<PresenceUpdateMsg>() {
                            let user_id = session.username.as_deref().unwrap_or("anonymous");
                            let mut mgr = shared.presence.write().await;
                            let outbound = mgr.handle_update(&session_id, user_id, &msg);
                            let senders = mgr.senders().clone();
                            drop(mgr);
                            outbound.send_all(&senders);
                        }
                        continue;
                    }

                    match dispatch_engine_frame(&mut ws, &mut session, &frame, &shared).await {
                        EngineOutcome::Break => break,
                        EngineOutcome::Handled => continue,
                        EngineOutcome::NotEngine => {}
                    }

                    if is_array_frame(frame.msg_type) {
                        // Bind the inbound array engine to the session's
                        // authenticated tenant, lazily, on first use. The gate is
                        // the tenant itself (not `authenticated`): the handshake
                        // sets `tenant_id = Some(..)` in the same step it marks the
                        // session authenticated, so a present tenant IS proof of
                        // authentication — and there is no placeholder-tenant
                        // fallback that could misroute writes under tenant 0.
                        if array_inbound.is_none()
                            && let Some(tenant) = session.tenant_id
                        {
                            array_inbound = build_array_inbound(&shared, tenant);
                        }
                        if let Some(inbound) = &array_inbound {
                            // Stamp the session's handshake-assigned identity so
                            // inbound array provenance is server-authoritative.
                            inbound
                                .set_session_identity(session.producer_id, session.accepted_epoch);
                            if let Some(f) =
                                dispatch_array_frame(&frame, inbound, &session_id).await
                                && ws.send(Message::Binary(f.to_bytes().into())).await.is_err()
                            {
                                break;
                            }
                        }
                        continue;
                    }

                    // Decode the CRDT message once for authorization and final dispatch.
                    let delta_msg = if frame.msg_type == SyncMessageType::DeltaPush {
                        frame.decode_body::<DeltaPushMsg>()
                    } else {
                        None
                    };
                    if let Some(delta_msg) = delta_msg.as_ref() {
                        let authorized = shared.as_ref().is_some_and(|shared| {
                            super::super::async_dispatch::authorize_delta_write(
                                shared,
                                session.identity.as_ref(),
                                &delta_msg.collection,
                            )
                            .is_ok()
                        });
                        if !authorized {
                            // Never run the generic handler without authorization:
                            // it mutates session accounting before its provisional ACK.
                            if let Some(reject) =
                                super::super::async_dispatch::permission_denied_delta_reject(
                                    delta_msg,
                                )
                                && ws
                                    .send(Message::Binary(reject.to_bytes().into()))
                                    .await
                                    .is_err()
                            {
                                break;
                            }
                            continue;
                        }
                    }

                    let response = if let Some(shared) = shared.as_ref() {
                        let rls_store = &shared.rls;
                        let mut audit = shared.audit.lock().unwrap_or_else(|p| p.into_inner());
                        let mut dlq = shared.sync_dlq.lock().unwrap_or_else(|p| p.into_inner());
                        session.process_frame(
                            &frame,
                            &jwt_validator,
                            Some(rls_store),
                            Some(&mut audit),
                            Some(&mut dlq),
                            Some(shared),
                        )
                    } else {
                        session.process_frame(&frame, &jwt_validator, None, None, None, None)
                    };

                    if let Some(response) = response {
                        let final_response = if response.msg_type == SyncMessageType::DeltaAck
                            && let Some(shared) = shared.as_ref()
                            && let Some(delta_msg) = delta_msg.as_ref()
                        {
                            super::super::async_dispatch::apply_delta_and_finalize(
                                shared,
                                delta_msg,
                                response,
                                session.identity.as_ref(),
                                session.producer_id,
                                session.accepted_epoch,
                            )
                            .await
                        } else {
                            Some(response)
                        };

                        if let Some(r) = final_response
                            && ws.send(Message::Binary(r.to_bytes().into())).await.is_err()
                        {
                            break;
                        }
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                let Ok(_) = ws.send(Message::Pong(data)).await else {
                    break;
                };
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                warn!(session = %session_id, error = %e, "sync: WebSocket error");
                break;
            }
            _ => {}
        }

        if session.authenticated
            && !crdt_registered
            && let Some(shared) = shared.as_ref()
        {
            let tenant_id = session.tenant_id.map(|t| t.as_u64()).unwrap_or(0);
            let peer_id = session.device_metadata.peer_id;
            let config = crate::event::crdt_sync::types::DeliveryConfig::default();
            let (drx, crx) = shared.crdt_sync_delivery.register(
                session_id.clone(),
                peer_id,
                tenant_id,
                Vec::new(),
                &config,
            );
            crdt_delivery_rx = Some(drx);
            crdt_control_rx = Some(crx);
            crdt_registered = true;
        }

        if session.authenticated
            && !array_delivery_registered
            && let Some(shared) = shared.as_ref()
        {
            let rx = shared.array_delivery.register(session_id.clone());
            array_delivery_rx = Some(rx);
            array_delivery_registered = true;
        }

        if session.authenticated
            && !definition_sync_registered
            && let Some(shared) = shared.as_ref()
        {
            let rx = shared.definition_sync_fanout.register(session_id.clone());
            definition_sync_rx = Some(rx);
            definition_sync_registered = true;
        }

        if session.authenticated
            && !presence_registered
            && let Some(shared) = shared.as_ref()
        {
            let (tx, rx) = tokio::sync::mpsc::channel(256);
            shared.presence.write().await.register_session(
                session_id.clone(),
                super::super::presence::SessionSender::new(tx),
            );
            presence_rx = Some(rx);
            presence_registered = true;
        }

        if let Some(ref mut rx) = presence_rx {
            while let Ok(bytes) = rx.try_recv() {
                if ws
                    .send(Message::Binary((*bytes).clone().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        if let Some(ref mut rx) = array_delivery_rx {
            while let Ok(frame_bytes) = rx.try_recv() {
                if ws.send(Message::Binary(frame_bytes.into())).await.is_err() {
                    break;
                }
            }
        }

        if let Some(ref mut rx) = definition_sync_rx {
            while let Ok(frame_bytes) = rx.try_recv() {
                if ws.send(Message::Binary(frame_bytes.into())).await.is_err() {
                    break;
                }
            }
        }

        if let Some(ref mut rx) = crdt_control_rx {
            while let Ok(frame) = rx.try_recv() {
                if ws
                    .send(Message::Binary(frame.to_bytes().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        if let Some(ref mut rx) = crdt_delivery_rx {
            while let Ok(delta) = rx.try_recv() {
                // Announce the collection descriptor before its first delta so
                // schema strictly precedes data on the peer. Idempotent per
                // session; a lookup miss warns and proceeds without marking.
                if let Some(shared) = shared.as_ref()
                    && let Some(schema_frame) = super::announce::build_collection_schema_frame(
                        shared,
                        &session,
                        delta.tenant_id,
                        &delta.collection,
                    )
                {
                    if ws
                        .send(Message::Binary(schema_frame.to_bytes().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    session
                        .announced_collections
                        .insert(delta.collection.clone());
                }

                let push_msg = nodedb_types::sync::wire::DeltaPushMsg {
                    collection: delta.collection,
                    document_id: delta.document_id,
                    delta: delta.payload,
                    peer_id: delta.peer_id,
                    mutation_id: delta.sequence,
                    checksum: 0,
                    device_valid_time_ms: None,
                    producer_id: 0,
                    epoch: 0,
                    seq: 0,
                };
                if let Some(frame) = nodedb_types::sync::wire::SyncFrame::new_msgpack(
                    nodedb_types::sync::wire::SyncMessageType::DeltaPush,
                    &push_msg,
                ) && ws
                    .send(Message::Binary(frame.to_bytes().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        if session.idle_secs() > state.config.idle_timeout_secs {
            info!(session = %session_id, "sync: idle timeout, closing");
            break;
        }
    }

    // Remove all shape subscriptions for this session from the persistent registry
    // so that the process-global registry does not grow unbounded.
    if let Some(shared) = shared.as_ref() {
        shared.shape_registry.remove_session(&session_id);
    }

    if crdt_registered && let Some(shared) = shared.as_ref() {
        shared.crdt_sync_delivery.unregister(&session_id);
    }

    if array_delivery_registered && let Some(shared) = shared.as_ref() {
        shared.array_delivery.unregister(&session_id);
        shared.array_subscriber_cursors.remove_session(&session_id);
    }

    if definition_sync_registered && let Some(shared) = shared.as_ref() {
        shared.definition_sync_fanout.unregister(&session_id);
    }

    if presence_registered && let Some(shared) = shared.as_ref() {
        let mut mgr = shared.presence.write().await;
        let outbound = mgr.unregister_session(&session_id);
        let senders = mgr.senders().clone();
        drop(mgr);
        outbound.send_all(&senders);
    }

    info!(
        session = %session_id,
        mutations = session.mutations_processed,
        rejected = session.mutations_rejected,
        silent_dropped = session.mutations_silent_dropped,
        uptime_secs = session.uptime_secs(),
        "sync: session closed"
    );
}
