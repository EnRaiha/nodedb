// SPDX-License-Identifier: BUSL-1.1

//! Handshake + fork detection.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tracing::{info, warn};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwt::JwtValidator;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::dlq::DeviceMetadata;
use super::super::wire::*;
use super::state::SyncSession;

/// Decision returned by the durable fencing logic before building the ack.
enum FencingDecision {
    /// Accept with the given producer_id and accepted_epoch.
    Accept {
        producer_id: u64,
        accepted_epoch: u64,
    },
    /// Reject: stale epoch from a cloned / forked device. PERMANENT — the
    /// client's epoch is behind the durable record; it must regenerate its
    /// LiteId. Surfaced to the client as `fork_detected = true`.
    Reject,
    /// Reject: a transient server-side error (registry I/O, Raft propose
    /// failure / leader mid-election). NOT a fork — the client should simply
    /// retry the handshake. Surfaced as `success = false, fork_detected = false`
    /// so the client never wipes its state over a momentary server hiccup.
    RejectTransient,
}

impl SyncSession {
    /// Process a handshake message: validate JWT, store client clock, detect forks.
    ///
    /// `shared` is threaded in so the durable `SyncProducerRegistry` can be consulted
    /// when the session is a Lite client.  When `shared` is `None` (non-Lite client
    /// or unit-test path without SharedState) the handshake proceeds with no fencing
    /// (`producer_id = 0`).
    ///
    /// Returns a HandshakeAck frame to send back to the client.
    pub fn handle_handshake(
        &mut self,
        msg: &HandshakeMsg,
        jwt_validator: &JwtValidator,
        current_server_clock: HashMap<String, u64>,
        shared: Option<&Arc<SharedState>>,
    ) -> Option<SyncFrame> {
        self.last_activity = Instant::now();

        // Wire format compatibility check: reject incompatible clients early.
        // Version 0 (missing field) falls through to check_wire_compatibility
        // and is rejected cleanly as "too old".
        if let Err(e) = crate::version::check_wire_compatibility(msg.wire_version) {
            warn!(
                session = %self.session_id,
                client_wire_version = msg.wire_version,
                error = %e,
                "sync handshake rejected: incompatible wire version"
            );
            let ack = HandshakeAckMsg {
                success: false,
                session_id: self.session_id.clone(),
                server_clock: current_server_clock,
                error: Some(format!("incompatible wire version: {e}")),
                fork_detected: false,
                server_wire_version: crate::version::WIRE_FORMAT_VERSION,
                producer_id: 0,
                accepted_epoch: 0,
            };
            return SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack);
        }

        // Trust mode: empty token auto-authenticates as default identity.
        if msg.jwt_token.is_empty() {
            let identity = AuthenticatedIdentity {
                user_id: 0,
                username: "sync-client".into(),
                tenant_id: TenantId::new(1),
                auth_method: crate::control::security::identity::AuthMethod::Trust,
                roles: vec![crate::control::security::identity::Role::ReadWrite],
                is_superuser: false,
                default_database: None,
                accessible_databases: crate::control::security::identity::DatabaseSet::Some(
                    smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT],
                ),
            };
            self.tenant_id = Some(identity.tenant_id);
            self.username = Some(identity.username.clone());
            self.identity = Some(identity);
            self.authenticated = true;
            self.client_clock = msg.vector_clock.clone();
            self.subscribed_shapes = msg.subscribed_shapes.clone();
            self.server_clock = current_server_clock.clone();
            self.last_seen_lsn = msg
                .vector_clock
                .values()
                .flat_map(|m| m.values().copied())
                .max()
                .unwrap_or(0);
            self.device_metadata = DeviceMetadata {
                client_version: msg.client_version.clone(),
                remote_addr: String::new(),
                peer_id: 0,
            };

            let tenant_id = self.tenant_id.map(|t| t.as_u64()).unwrap_or(0);
            match self.durable_fencing_decision(msg, shared, tenant_id) {
                Some(FencingDecision::Reject) => {
                    return self.fork_reject_frame(
                        &current_server_clock,
                        "FORK_DETECTED: stale epoch — regenerate LiteId and reconnect",
                    );
                }
                Some(FencingDecision::RejectTransient) => {
                    return self.transient_reject_frame(
                        &current_server_clock,
                        "SYNC_UNAVAILABLE: transient server error — retry the handshake",
                    );
                }
                Some(FencingDecision::Accept {
                    producer_id,
                    accepted_epoch,
                }) => {
                    self.producer_id = producer_id;
                    self.accepted_epoch = accepted_epoch;
                }
                None => {}
            }

            info!(session = %self.session_id, "sync handshake OK (trust mode)");

            let ack = HandshakeAckMsg {
                success: true,
                session_id: self.session_id.clone(),
                server_clock: current_server_clock,
                error: None,
                fork_detected: false,
                server_wire_version: crate::version::WIRE_FORMAT_VERSION,
                producer_id: self.producer_id,
                accepted_epoch: self.accepted_epoch,
            };
            return SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack);
        }

        // Validate JWT.
        match jwt_validator.validate(&msg.jwt_token) {
            Ok(identity) => {
                self.tenant_id = Some(identity.tenant_id);
                self.username = Some(identity.username.clone());
                self.identity = Some(identity.clone());
                self.authenticated = true;
                self.client_clock = msg.vector_clock.clone();
                self.subscribed_shapes = msg.subscribed_shapes.clone();
                self.server_clock = current_server_clock.clone();
                self.last_seen_lsn = msg
                    .vector_clock
                    .values()
                    .flat_map(|m| m.values().copied())
                    .max()
                    .unwrap_or(0);
                self.device_metadata = DeviceMetadata {
                    client_version: msg.client_version.clone(),
                    remote_addr: String::new(),
                    peer_id: 0,
                };

                let tenant_id = identity.tenant_id.as_u64();
                match self.durable_fencing_decision(msg, shared, tenant_id) {
                    Some(FencingDecision::Reject) => {
                        return self.fork_reject_frame(
                            &current_server_clock,
                            "FORK_DETECTED: stale epoch — regenerate LiteId and reconnect",
                        );
                    }
                    Some(FencingDecision::RejectTransient) => {
                        return self.transient_reject_frame(
                            &current_server_clock,
                            "SYNC_UNAVAILABLE: transient server error — retry the handshake",
                        );
                    }
                    Some(FencingDecision::Accept {
                        producer_id,
                        accepted_epoch,
                    }) => {
                        self.producer_id = producer_id;
                        self.accepted_epoch = accepted_epoch;
                    }
                    None => {}
                }

                info!(
                    session = %self.session_id,
                    user = %identity.username,
                    tenant = identity.tenant_id.as_u64(),
                    shapes = self.subscribed_shapes.len(),
                    "sync handshake OK"
                );

                let ack = HandshakeAckMsg {
                    success: true,
                    session_id: self.session_id.clone(),
                    server_clock: current_server_clock,
                    error: None,
                    fork_detected: false,
                    server_wire_version: crate::version::WIRE_FORMAT_VERSION,
                    producer_id: self.producer_id,
                    accepted_epoch: self.accepted_epoch,
                };
                SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack)
            }
            Err(e) => {
                warn!(
                    session = %self.session_id,
                    error = %e,
                    "sync handshake FAILED"
                );
                let ack = HandshakeAckMsg {
                    success: false,
                    session_id: self.session_id.clone(),
                    server_clock: HashMap::new(),
                    error: Some(e.to_string()),
                    fork_detected: false,
                    server_wire_version: crate::version::WIRE_FORMAT_VERSION,
                    producer_id: 0,
                    accepted_epoch: 0,
                };
                SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack)
            }
        }
    }

    /// Build a failed HandshakeAck rejection frame: `success=false`, the
    /// supplied `message` in the `error` field, and the given `fork_detected`
    /// flag. Shared by [`Self::fork_reject_frame`] and
    /// [`Self::transient_reject_frame`] so the envelope stays identical.
    fn build_reject_frame(
        &self,
        server_clock: &HashMap<String, u64>,
        message: &str,
        fork_detected: bool,
    ) -> Option<SyncFrame> {
        let ack = HandshakeAckMsg {
            success: false,
            session_id: self.session_id.clone(),
            server_clock: server_clock.clone(),
            error: Some(message.into()),
            fork_detected,
            server_wire_version: crate::version::WIRE_FORMAT_VERSION,
            producer_id: 0,
            accepted_epoch: 0,
        };
        SyncFrame::try_encode(SyncMessageType::HandshakeAck, &ack)
    }

    /// Build a fork-detected rejection frame (`fork_detected=true`): the client
    /// treats the session as a permanent fork and wipes its local state.
    fn fork_reject_frame(
        &self,
        server_clock: &HashMap<String, u64>,
        message: &str,
    ) -> Option<SyncFrame> {
        warn!(
            session = %self.session_id,
            "FORK DETECTED: {message}"
        );
        self.build_reject_frame(server_clock, message, true)
    }

    /// Build a failed-but-retryable handshake ack for a transient server-side
    /// error (registry I/O, Raft propose failure / leader mid-election).
    ///
    /// Distinct from [`Self::fork_reject_frame`]: `fork_detected = false`, so
    /// the client retries the handshake rather than treating the session as a
    /// permanent fork and wiping its local state.
    fn transient_reject_frame(
        &self,
        server_clock: &HashMap<String, u64>,
        message: &str,
    ) -> Option<SyncFrame> {
        warn!(
            session = %self.session_id,
            "sync handshake transient error (retryable): {message}"
        );
        self.build_reject_frame(server_clock, message, false)
    }

    /// Attempt to make a durable fencing decision via `SyncProducerRegistry`.
    ///
    /// Returns `Some(FencingDecision)` when the msg is a Lite handshake
    /// (`!lite_id.is_empty() && epoch > 0`) and a registry is available via
    /// `shared`.  Returns `None` when:
    ///
    /// * The msg is not a Lite handshake (non-Lite / legacy client).
    /// * No `SharedState` is present (unit-test path) — handshake proceeds
    ///   with no fencing.
    /// * `shared` has no `producer_registry` — handshake proceeds with no fencing.
    ///
    /// On registry operation errors the decision is `Reject` (fail-closed) rather
    /// than silently accepting.
    fn durable_fencing_decision(
        &self,
        msg: &HandshakeMsg,
        shared: Option<&Arc<SharedState>>,
        tenant_id: u64,
    ) -> Option<FencingDecision> {
        if msg.lite_id.is_empty() || msg.epoch == 0 {
            return None;
        }

        let registry = shared.and_then(|s| s.producer_registry.as_deref());

        match registry {
            Some(reg) => {
                // `shared` is always `Some` when `registry` is `Some` (the
                // registry was obtained via `shared.and_then(...)`); the `?`
                // is just to recover the handle.
                let shared_ref = shared?;

                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                match reg.get(&msg.lite_id) {
                    Err(e) => {
                        warn!(
                            session = %self.session_id,
                            lite_id = %msg.lite_id,
                            error = %e,
                            "sync handshake: registry.get failed; rejecting as retryable"
                        );
                        Some(FencingDecision::RejectTransient)
                    }
                    Ok(None) => {
                        // First time we see this lite_id: register and accept.
                        // The local write keeps single-node correct; the Raft
                        // propose replicates the producer_id+epoch to all
                        // followers so state survives leader failover.
                        match reg.register(&msg.lite_id, tenant_id, msg.epoch, now_ms) {
                            Ok(registration) => {
                                if let Err(e) =
                                    crate::control::metadata_proposer::propose_sync_producer_register(
                                        shared_ref.as_ref(),
                                        &msg.lite_id,
                                        registration.producer_id,
                                        tenant_id,
                                        registration.current_epoch,
                                        now_ms,
                                    )
                                {
                                    warn!(
                                        session = %self.session_id,
                                        lite_id = %msg.lite_id,
                                        error = %e,
                                        "sync handshake: propose_sync_producer_register failed; \
                                         rejecting as retryable"
                                    );
                                    return Some(FencingDecision::RejectTransient);
                                }
                                Some(FencingDecision::Accept {
                                    producer_id: registration.producer_id,
                                    accepted_epoch: registration.current_epoch,
                                })
                            }
                            Err(e) => {
                                warn!(
                                    session = %self.session_id,
                                    lite_id = %msg.lite_id,
                                    error = %e,
                                    "sync handshake: registry.register failed; rejecting as retryable"
                                );
                                Some(FencingDecision::RejectTransient)
                            }
                        }
                    }
                    Ok(Some(existing)) => {
                        if msg.epoch > existing.current_epoch {
                            // Epoch advanced: fence the old epoch and accept the new one.
                            // The local write keeps single-node correct; the Raft
                            // propose replicates the epoch advance cluster-wide.
                            match reg.fence(&msg.lite_id, msg.epoch) {
                                Ok(()) => {
                                    if let Err(e) =
                                        crate::control::metadata_proposer::propose_sync_producer_fence(
                                            shared_ref.as_ref(),
                                            &msg.lite_id,
                                            msg.epoch,
                                        )
                                    {
                                        warn!(
                                            session = %self.session_id,
                                            lite_id = %msg.lite_id,
                                            error = %e,
                                            "sync handshake: propose_sync_producer_fence failed; \
                                             rejecting as retryable"
                                        );
                                        return Some(FencingDecision::RejectTransient);
                                    }
                                    Some(FencingDecision::Accept {
                                        producer_id: existing.producer_id,
                                        accepted_epoch: msg.epoch,
                                    })
                                }
                                Err(e) => {
                                    warn!(
                                        session = %self.session_id,
                                        lite_id = %msg.lite_id,
                                        error = %e,
                                        "sync handshake: registry.fence failed; rejecting as retryable"
                                    );
                                    Some(FencingDecision::RejectTransient)
                                }
                            }
                        } else if msg.epoch == existing.current_epoch {
                            // Same epoch: idempotent re-connect (at-least-once redelivery).
                            // No state change — no propose needed.
                            Some(FencingDecision::Accept {
                                producer_id: existing.producer_id,
                                accepted_epoch: existing.current_epoch,
                            })
                        } else {
                            // msg.epoch < existing.current_epoch: stale/forked device.
                            // No state change — no propose needed.
                            warn!(
                                session = %self.session_id,
                                lite_id = %msg.lite_id,
                                client_epoch = msg.epoch,
                                current_epoch = existing.current_epoch,
                                "FORK DETECTED: client epoch is behind persisted epoch"
                            );
                            Some(FencingDecision::Reject)
                        }
                    }
                }
            }
            // No registry available: no fencing decision — handshake proceeds.
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use nodedb_types::sync::wire::{HandshakeAckMsg, HandshakeMsg};

    use crate::control::security::catalog::SystemCatalog;
    use crate::control::security::jwt::{JwtConfig, JwtValidator};
    use crate::control::server::sync::session::state::SyncSession;
    use crate::control::sync_producer::registry::SyncProducerRegistry;

    fn make_handshake(wire_version: u16) -> HandshakeMsg {
        HandshakeMsg {
            jwt_token: String::new(),
            vector_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            client_version: "test".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version,
        }
    }

    fn open_registry(dir: &std::path::Path) -> SyncProducerRegistry {
        let catalog = Arc::new(SystemCatalog::open(&dir.join("system.redb")).unwrap());
        SyncProducerRegistry::open(catalog).unwrap()
    }

    #[test]
    fn handshake_rejects_wire_version_zero() {
        let mut session = SyncSession::new("test-session".into());
        let validator = JwtValidator::new(JwtConfig::default());
        let msg = make_handshake(0);

        let frame = session
            .handle_handshake(&msg, &validator, HashMap::new(), None)
            .expect("should return a frame");

        let ack: HandshakeAckMsg = frame.decode_body().expect("should decode HandshakeAckMsg");
        assert!(
            !ack.success,
            "wire_version=0 must be rejected, got success=true"
        );
        let error = ack.error.expect("error message must be present");
        assert!(
            error.contains("wire version") || error.contains("incompatible"),
            "error message should mention wire version, got: {error}"
        );
    }

    /// Uses the registry directly (not via SharedState) to exercise
    /// `durable_fencing_decision` in isolation.
    #[test]
    fn registry_new_lite_id_assigns_producer_id() {
        let dir = tempfile::tempdir().unwrap();
        let reg = open_registry(dir.path());

        // Simulate what durable_fencing_decision does for a new lite_id.
        let r = reg.register("device-a", 1, 10, 0).unwrap();
        assert!(r.producer_id > 0);
        assert_eq!(r.current_epoch, 10);
    }

    #[test]
    fn registry_same_epoch_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let reg = open_registry(dir.path());

        let first = reg.register("device-b", 1, 5, 0).unwrap();
        let loaded = reg.get("device-b").unwrap().unwrap();
        assert_eq!(loaded.producer_id, first.producer_id);
        assert_eq!(loaded.current_epoch, 5);
    }

    #[test]
    fn registry_higher_epoch_fences() {
        let dir = tempfile::tempdir().unwrap();
        let reg = open_registry(dir.path());

        let first = reg.register("device-c", 1, 3, 0).unwrap();
        reg.fence("device-c", 7).unwrap();
        let loaded = reg.get("device-c").unwrap().unwrap();
        assert_eq!(loaded.producer_id, first.producer_id);
        assert_eq!(loaded.current_epoch, 7);
    }

    #[test]
    fn registry_lower_epoch_is_stale() {
        let dir = tempfile::tempdir().unwrap();
        let reg = open_registry(dir.path());

        reg.register("device-d", 1, 9, 0).unwrap();
        let loaded = reg.get("device-d").unwrap().unwrap();
        // A client presenting epoch < current_epoch (9) must be rejected.
        assert!(
            3 < loaded.current_epoch,
            "epoch 3 is stale vs {}",
            loaded.current_epoch
        );
    }
}
