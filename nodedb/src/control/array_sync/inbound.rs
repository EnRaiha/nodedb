// SPDX-License-Identifier: BUSL-1.1

//! [`OriginArrayInbound`] — dispatcher for inbound array CRDT wire messages.
//!
//! Receives decoded wire messages from the WebSocket listener, validates
//! them (schema gating, idempotency), and proposes ops through Raft for
//! durable, ordered replication to all Origin replicas.
//!
//! # Write flow
//!
//! 1. Decode op payload via `nodedb_array::sync::op_codec::decode_op`.
//! 2. Fast-path idempotency check (before proposing, to avoid wasting Raft
//!    proposals for already-seen ops; this mirrors the Document handler pattern).
//! 3. Schema-gate: reject if the array is unknown or the op's schema HLC is
//!    ahead of the local registry.
//! 4. Build `ReplicatedWrite::ArrayOp` and serialize to a `ReplicatedEntry`.
//! 5. `raft_proposer(vshard_id, bytes)` — propose to the Raft group that
//!    owns the destination vShard.
//! 6. Await Raft commit via `ProposeTracker`.
//! 7. On commit: `distributed_applier` decodes the entry, dispatches it to
//!    the Data Plane, calls `record_applied`.
//!
//! # Schema sync
//!
//! `handle_schema` builds `ReplicatedWrite::ArraySchema` and follows the same
//! propose → commit flow. After commit the `distributed_applier` calls
//! `OriginSchemaRegistry::import_snapshot`.
//!
//! # Non-Raft paths
//!
//! Acks and catchup requests are advisory / read-only and never touch Raft.
//! Snapshot chunks are buffered until a full snapshot arrives, then applied
//! as a batch of ArrayOp proposals (one per contained op).
//!
//! # Thread safety
//!
//! `OriginArrayInbound` is `Send + Sync`. The snapshot buffer uses a `Mutex`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_array::sync::apply::ApplyRejection;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::sync::wire::array::{
    ArrayDeltaBatchMsg, ArrayDeltaMsg, ArrayRejectMsg, ArrayRejectReason, ArraySchemaSyncMsg,
};
use tracing::warn;

use nodedb_cluster::array_routing::array_vshard_for_name;

use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId, VShardId};

use super::apply::OriginApplyEngine;
use super::outbound::ArrayApplyObserver;
use super::reject::build_reject;
use super::schema_registry::OriginSchemaRegistry;
use super::snapshot_assembly::SnapshotAssembly;

// ─── Outcome ─────────────────────────────────────────────────────────────────

/// Outcome returned by each [`OriginArrayInbound`] handler.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundOutcome {
    /// The op was applied to Data Plane engine state.
    Applied,
    /// The op was already present; no state was changed (idempotent replay).
    Idempotent,
    /// The op was rejected; the caller should send `ArrayRejectMsg` back.
    Rejected(ApplyRejection),
    /// A snapshot chunk was buffered; more chunks are expected.
    SnapshotPartial { received: u32, total: u32 },
    /// A snapshot was fully assembled and all contained ops applied.
    SnapshotApplied { ops_applied: u64 },
    /// A schema CRDT snapshot was imported into the local registry.
    SchemaImported,
    /// An ack was recorded into the ack-vector (GC frontier tracking).
    AckRecorded,
    /// A catchup request was received and logged (serving deferred to Phase H).
    CatchupRequested,
}

// ─── Dispatcher ──────────────────────────────────────────────────────────────

/// Dispatcher for inbound array CRDT wire messages from Lite peers.
///
/// Constructed once per sync session (or shared across sessions via `Arc`)
/// and called from the WebSocket listener arm for each array message type.
pub struct OriginArrayInbound {
    engine: Arc<OriginApplyEngine>,
    pub(super) schemas: Arc<OriginSchemaRegistry>,
    pub(super) shared: Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    identity: crate::control::security::identity::AuthenticatedIdentity,
    /// Post-apply observer for fan-out to subscribed Lite peers.
    ///
    /// `None` in configurations where no Lite subscribers are expected
    /// (e.g. pure cluster-to-cluster sync without Lite edges).
    apply_observer: Option<Arc<dyn ArrayApplyObserver>>,
    /// In-flight snapshot chunk buffers keyed by `(array, snapshot_hlc_bytes)`.
    snapshots: Mutex<HashMap<(String, [u8; 18]), SnapshotAssembly>>,
    /// Server-authoritative producer identity for this session, assigned at
    /// handshake and set via [`Self::set_session_identity`]. Inbound provenance
    /// is stamped from these — never from the wire message — so a client cannot
    /// spoof another producer's id or replay a fenced epoch. `0` = unidentified
    /// (legacy / non-fenced) producer, which the gate treats as a no-op.
    session_producer_id: std::sync::atomic::AtomicU64,
    session_epoch: std::sync::atomic::AtomicU64,
}

impl OriginArrayInbound {
    /// Accessor for the snapshot assembly buffer used by the
    /// `snapshot_assembly` sibling module.
    pub(super) fn snapshots(&self) -> &Mutex<HashMap<(String, [u8; 18]), SnapshotAssembly>> {
        &self.snapshots
    }

    pub(super) fn shared(&self) -> &Arc<SharedState> {
        &self.shared
    }

    /// The tenant this inbound engine is bound to. `pub(crate)` so the sync
    /// session builder's guard test can assert the session tenant was threaded
    /// through (see `session_handler::array`), not just the sibling propose
    /// path.
    pub(crate) fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Database selected by the authenticated sync session. This is captured
    /// once at engine construction and is the sole database scope for every
    /// inbound array authorization, Raft entry, and Data-Plane task.
    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(super) fn identity(&self) -> &crate::control::security::identity::AuthenticatedIdentity {
        &self.identity
    }

    pub(super) fn authorize_array(
        &self,
        array: &str,
        hlc: Hlc,
        permission: crate::control::security::identity::Permission,
    ) -> Result<
        crate::control::server::shared::authorization::AuthorizedCollection,
        Option<ArrayRejectMsg>,
    > {
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(Arc::clone(&self.shared.audit));
        crate::control::server::shared::authorization::authorize_collection_capability(
            &self.identity,
            self.database_id,
            array,
            permission,
            &self.shared.permissions,
            &self.shared.roles,
            &emitter,
        )
        .map_err(|error| {
            Some(build_reject(
                array,
                hlc,
                ArrayRejectReason::EngineRejected,
                error.resource().to_string(),
            ))
        })
    }

    pub(super) fn engine(&self) -> &Arc<OriginApplyEngine> {
        &self.engine
    }

    pub(super) fn schemas(&self) -> &Arc<OriginSchemaRegistry> {
        &self.schemas
    }

    pub(super) fn apply_observer(&self) -> Option<&Arc<dyn ArrayApplyObserver>> {
        self.apply_observer.as_ref()
    }
}

impl OriginArrayInbound {
    /// Construct from shared server state and session tenant.
    pub fn new(
        engine: Arc<OriginApplyEngine>,
        schemas: Arc<OriginSchemaRegistry>,
        shared: Arc<SharedState>,
        identity: crate::control::security::identity::AuthenticatedIdentity,
    ) -> Self {
        let tenant_id = identity.tenant_id;
        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        Self {
            engine,
            schemas,
            shared,
            tenant_id,
            database_id,
            identity,
            apply_observer: None,
            snapshots: Mutex::new(HashMap::new()),
            session_producer_id: std::sync::atomic::AtomicU64::new(0),
            session_epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Attach a post-apply observer (used by `ArrayFanout` for Lite fan-out).
    pub fn with_observer(mut self, observer: Arc<dyn ArrayApplyObserver>) -> Self {
        self.apply_observer = Some(observer);
        self
    }

    /// Bind this session's handshake-assigned producer identity. The session
    /// handler calls this before dispatching each array frame so inbound
    /// provenance is stamped server-authoritatively. `0` leaves the session
    /// unfenced (legacy / unidentified producer).
    pub fn set_session_identity(&self, producer_id: u64, epoch: u64) {
        use std::sync::atomic::Ordering;
        self.session_producer_id
            .store(producer_id, Ordering::Relaxed);
        self.session_epoch.store(epoch, Ordering::Relaxed);
    }

    /// Build server-authoritative provenance for an op in `array` carrying the
    /// client-supplied `seq`. `producer_id`/`epoch` come from the session's
    /// handshake identity, NOT the wire message. Returns `None` for an
    /// unidentified producer (`session_producer_id == 0`) — the gate no-ops.
    fn session_provenance(&self, array: &str, seq: u64) -> Option<SyncProvenance> {
        use std::sync::atomic::Ordering;
        let producer_id = self.session_producer_id.load(Ordering::Relaxed);
        if producer_id == 0 {
            return None;
        }
        use nodedb_types::sync::wire::stream_id::{EngineKind, stream_id_for};
        Some(SyncProvenance {
            producer_id,
            epoch: self.session_epoch.load(Ordering::Relaxed),
            stream_id: stream_id_for(EngineKind::Array, array),
            seq,
        })
    }

    // ─── Delta ───────────────────────────────────────────────────────────────

    /// Handle a single delta message from a Lite peer.
    pub async fn handle_delta(
        &self,
        msg: &ArrayDeltaMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let op = match op_codec::decode_op(&msg.op_payload) {
            Ok(op) => op,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: delta decode failed");
                return Err(Some(build_reject(
                    &msg.array,
                    Hlc::ZERO,
                    ArrayRejectReason::ShapeInvalid,
                    format!("decode error: {e}"),
                )));
            }
        };

        let provenance = self.session_provenance(&msg.array, msg.seq);
        self.apply_op(op, &msg.op_payload, provenance).await
    }

    /// Handle a batch of delta messages from a Lite peer.
    ///
    /// Returns one outcome per op. If decoding fails for an op, that op
    /// yields a reject; subsequent ops are still attempted.
    pub async fn handle_delta_batch(
        &self,
        msg: &ArrayDeltaBatchMsg,
    ) -> Vec<Result<InboundOutcome, Option<ArrayRejectMsg>>> {
        let mut outcomes = Vec::with_capacity(msg.op_payloads.len());
        for payload in &msg.op_payloads {
            let outcome = match op_codec::decode_op(payload) {
                Ok(op) => {
                    // Batch carries no per-op seq; array dedups by HLC, so seq
                    // is informational here. The epoch fence still applies.
                    let provenance = self.session_provenance(&msg.array, 0);
                    self.apply_op(op, payload, provenance).await
                }
                Err(e) => {
                    warn!(array = %msg.array, error = %e, "array_inbound: batch decode failed");
                    Err(Some(build_reject(
                        &msg.array,
                        Hlc::ZERO,
                        ArrayRejectReason::ShapeInvalid,
                        format!("batch decode error: {e}"),
                    )))
                }
            };
            outcomes.push(outcome);
        }
        outcomes
    }

    // ─── Schema ──────────────────────────────────────────────────────────────

    /// Import an array schema CRDT snapshot from a Lite peer.
    ///
    /// Proposes the schema through Raft so it is applied atomically on all
    /// replicas. Returns `SchemaImported` on successful commit.
    pub async fn handle_schema(
        &self,
        msg: &ArraySchemaSyncMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let hlc_arr: [u8; 18] = msg.schema_hlc_bytes;
        let remote_hlc = Hlc::from_bytes(&hlc_arr);
        let authorization = self.authorize_array(
            &msg.array,
            remote_hlc,
            crate::control::security::identity::Permission::Write,
        )?;

        // In single-node mode (no raft_proposer) fall back to direct import.
        if self.shared.raft_proposer.get().is_none() {
            let _authorized_scope = authorization.into_scope();
            if let Err(e) = self.schemas.import_snapshot_in_database(
                self.database_id,
                self.tenant_id.as_u64(),
                &msg.array,
                &msg.snapshot_payload,
                remote_hlc,
            ) {
                warn!(array = %msg.array, error = %e, "array_inbound: schema import failed");
                return Err(Some(build_reject(
                    &msg.array,
                    remote_hlc,
                    ArrayRejectReason::EngineRejected,
                    format!("schema import error: {e}"),
                )));
            }
            // Single-node has no Raft applier to register the array_catalog
            // entry, so this direct-import path must do it itself — mirrors
            // `raft_apply::apply_array_schema`'s post-import registration.
            // Without this, the array is importable but never openable by
            // the Data Plane and never visible to `SHOW COLLECTIONS`.
            //
            // Unlike the Raft-apply path, this path's `Result` is still live
            // and reaches the sync sender, so a registration failure is
            // propagated rather than swallowed: reporting `SchemaImported`
            // while the array stays unregistered would be a silent
            // catalog-visibility inconsistency.
            if let Err(e) = super::catalog_register::register_array_catalog_entry(
                &self.shared,
                self.tenant_id,
                self.database_id,
                &msg.array,
            ) {
                warn!(array = %msg.array, error = %e, "array_inbound: catalog registration failed");
                return Err(Some(build_reject(
                    &msg.array,
                    remote_hlc,
                    ArrayRejectReason::EngineRejected,
                    format!("catalog registration error: {e}"),
                )));
            }
            return Ok(InboundOutcome::SchemaImported);
        }

        let vshard_id = VShardId::new(array_vshard_for_name(&msg.array));
        let write = ReplicatedWrite::ArraySchema {
            array: msg.array.clone(),
            snapshot_payload: msg.snapshot_payload.clone(),
            schema_hlc_bytes: hlc_arr,
        };
        let entry = ReplicatedEntry::new(
            self.tenant_id.as_u64(),
            self.database_id.as_u64(),
            vshard_id.as_u32(),
            write,
        );

        match self
            .propose_and_await(entry, &msg.array, remote_hlc, authorization)
            .await
        {
            Ok(()) => Ok(InboundOutcome::SchemaImported),
            Err(Some(r)) => Err(Some(r)),
            Err(None) => Err(None),
        }
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    /// Validate a decoded op, then route it through Raft (or directly to the
    /// Data Plane in single-node mode) before returning.
    ///
    /// # Fast-path idempotency
    ///
    /// The idempotency check here is a *pre-proposal fast-path*: it avoids
    /// wasting a Raft round-trip for ops the local replica already knows
    /// about. The authoritative check happens in the distributed applier
    /// (after Raft commit) so replicas that missed the original entry still
    /// accept it on re-delivery.
    pub(super) async fn apply_op(
        &self,
        op: ArrayOp,
        raw_op_bytes: &[u8],
        provenance: Option<SyncProvenance>,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        // 1. Shape validation.
        if let Err(e) = op.validate_shape() {
            return Err(Some(build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::ShapeInvalid,
                format!("shape validation: {e}"),
            )));
        }

        let authorization = self.authorize_array(
            &op.header.array,
            op.header.hlc,
            crate::control::security::identity::Permission::Write,
        )?;

        // 2. Schema HLC gating.
        match self.engine.schema_hlc_in_database(
            self.database_id,
            self.tenant_id.as_u64(),
            &op.header.array,
        ) {
            None => {
                return Err(Some(build_reject(
                    &op.header.array,
                    op.header.hlc,
                    ArrayRejectReason::ArrayUnknown,
                    format!("array '{}' not known to this replica", op.header.array),
                )));
            }
            Some(local_schema) if op.header.schema_hlc > local_schema => {
                return Err(Some(build_reject(
                    &op.header.array,
                    op.header.hlc,
                    ArrayRejectReason::SchemaTooNew,
                    format!(
                        "op schema_hlc {:?} > local {:?}; request schema sync",
                        op.header.schema_hlc, local_schema
                    ),
                )));
            }
            Some(_) => {}
        }

        // 3. Fast-path idempotency check (before proposing).
        if self.engine.already_seen_in_database(
            self.database_id,
            self.tenant_id.as_u64(),
            &op.header.array,
            op.header.hlc,
        ) {
            let _authorized_scope = authorization.into_scope();
            return Ok(InboundOutcome::Idempotent);
        }

        // 4. In single-node mode (no raft_proposer): apply directly to the
        //    Data Plane, matching the pre-Raft behaviour. This path is only
        //    exercised when the cluster stack has not been started (development,
        //    single-node Origin, unit tests without a raft setup).
        if self.shared.raft_proposer.get().is_none() {
            return self.apply_op_direct(op, provenance, authorization).await;
        }

        // 5. Multi-node path: propose through Raft.
        let hlc_bytes = op.header.hlc.to_bytes();
        let write = ReplicatedWrite::ArrayOp {
            array: op.header.array.clone(),
            op_bytes: raw_op_bytes.to_vec(),
            schema_hlc_bytes: hlc_bytes,
            provenance: provenance
                .as_ref()
                .and_then(|p| zerompk::to_msgpack_vec(p).ok()),
        };
        let vshard = self.vshard_for_op(&op);
        let entry = ReplicatedEntry::new(
            self.tenant_id.as_u64(),
            self.database_id.as_u64(),
            vshard.as_u32(),
            write,
        );

        match self
            .propose_and_await(entry, &op.header.array, op.header.hlc, authorization)
            .await
        {
            Ok(()) => {
                // Notify outbound fan-out observer so subscribed Lite peers
                // receive this op. The observer enqueues; no I/O here.
                if let Some(observer) = &self.apply_observer {
                    observer.on_op_applied(&op);
                }
                Ok(InboundOutcome::Applied)
            }
            Err(e) => Err(e),
        }
    }

    // Raft propose / direct-dispatch helpers live in `inbound_propose.rs`
    // to keep this file under the size limit.
}
