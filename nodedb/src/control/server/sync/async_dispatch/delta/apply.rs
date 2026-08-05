// SPDX-License-Identifier: BUSL-1.1

//! Dispatch a CRDT delta to the Data Plane and hand its outcome to the frame
//! builder.

use std::time::Duration;

use tracing::warn;

use nodedb_types::sync::wire::{EngineKind, SyncProvenance, stream_id_for};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::wire::{
    CompensationHint, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType,
};
use super::authorize::{authorize_delta_write, permission_denied_delta_reject};
use super::outcome::frame_for_dispatch;
use super::peer_identity::{
    PeerIdentity, PeerIdentityRequest, admit_peer_identity, peer_collision_reason,
};
use super::signature::delta_signature_valid;

/// What one delta's durable dispatch produced.
///
/// The frame is what the client is told; `trimmed_ops` is what the delta
/// actually carried. They are returned together because the session needs both
/// and neither can be derived from the other: two deltas with opposite
/// consequences for the client's data can produce the same frame.
pub(crate) struct DeltaDispatchOutcome {
    pub(crate) frame: Option<SyncFrame>,
    pub(crate) trimmed_ops: u64,
}

impl DeltaDispatchOutcome {
    /// A refusal decided before any CRDT merge ran, so nothing was trimmed.
    fn refused(frame: Option<SyncFrame>) -> Self {
        Self {
            frame,
            trimmed_ops: 0,
        }
    }
}

/// The handshake-bound session state a delta apply is evaluated against.
///
/// These travel together because they all describe the same authenticated sync
/// session: the identity the delta is authorized as, the key its ops are signed
/// with, its producer/epoch position, and the peer address the blacklist is
/// checked against.
pub(crate) struct DeltaSessionContext<'a> {
    pub(crate) identity: Option<&'a AuthenticatedIdentity>,
    pub(crate) signing_key: Option<&'a [u8; 32]>,
    pub(crate) producer_id: u64,
    pub(crate) epoch: u64,
    pub(crate) peer_addr: &'a str,
}

/// Apply a CRDT delta on the Data Plane, converting the outcome into the final
/// client frame.
///
/// The in-memory session already produced a provisional `DeltaAck`; this step
/// performs the actual durable apply and finalizes the client frame. Which
/// frame that is — a `DeltaAck` (including the retryable `Gap`) or a terminal
/// `DeltaReject` — is decided in exactly one place, [`frame_for_dispatch`], so
/// the sender's obligation always matches what the server actually did.
pub(crate) async fn apply_delta_and_finalize(
    shared: &SharedState,
    delta_msg: &DeltaPushMsg,
    ack_frame: SyncFrame,
    session: DeltaSessionContext<'_>,
) -> DeltaDispatchOutcome {
    let DeltaSessionContext {
        identity,
        signing_key: session_signing_key,
        producer_id: session_producer_id,
        epoch: session_epoch,
        peer_addr,
    } = session;
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::CrdtOp;

    // Authorize before quota, surrogate allocation, catalog lookup, plan
    // construction, or dispatch. The tenant is derived only from the
    // handshake-bound identity; an absent identity fails closed without a
    // fallback tenant or reconstructed principal.
    let tenant_id = match authorize_delta_write(shared, identity, &delta_msg.collection) {
        Ok(tenant_id) => tenant_id,
        Err(_) => {
            return DeltaDispatchOutcome::refused(permission_denied_delta_reject(delta_msg));
        }
    };
    let identity = match identity {
        Some(identity) => identity,
        None => {
            return DeltaDispatchOutcome::refused(permission_denied_delta_reject(delta_msg));
        }
    };

    // Same binding the session's reads use: the principal's database, not the
    // built-in default. A delta must land in the database its subscriber will
    // read it back from.
    let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);

    // Blacklist + account status, no rate limit: CRDT delta sync is not the
    // per-query traffic the rate-limiter's cost table models, so charging it
    // against a query rate limit would throttle legitimate offline-first
    // sync traffic. A blacklisted or suspended/banned account must not be
    // able to keep pushing deltas, though — `check_blacklist_and_status`
    // runs that half of `check_request_admission`'s gate (plus the
    // internal-service exemption every other transport gets) using the sync
    // session's real remote address.
    let scope = crate::control::security::request_scope::RequestAuthScope::for_database(
        identity,
        &shared.scope_grants,
        database_id,
    );
    if let Err(e) =
        crate::control::server::session_auth::check_blacklist_and_status(shared, &scope, peer_addr)
    {
        warn!(error = %e, "sync: delta rejected by blacklist or account status");
        return terminal_reject(
            delta_msg,
            "sender is blocked",
            CompensationHint::PermissionDenied,
        );
    }

    let audit = ArcAuditEmitter(std::sync::Arc::clone(&shared.audit));
    let policy = crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy::from_identity(
        tenant_id,
        database_id,
        &delta_msg.collection,
        identity,
        "sync".into(),
        &shared.rls,
        &audit,
    );

    let (constraint_version_required, signing_required) = match shared
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), &delta_msg.collection)
    {
        Ok(Some(collection)) => (
            collection.constraint_version,
            collection.crdt_signing_required,
        ),
        Ok(None) => {
            return terminal_reject(
                delta_msg,
                "collection not found",
                CompensationHint::PermissionDenied,
            );
        }
        Err(error) => {
            warn!(%error, collection = %delta_msg.collection, "sync signing policy lookup failed");
            return terminal_reject(
                delta_msg,
                "collection security policy is temporarily unavailable",
                CompensationHint::PermissionDenied,
            );
        }
    };

    if signing_required && !shared.wal.payloads_authenticated() {
        return terminal_reject(
            delta_msg,
            "SIGNED_DELTAS requires authenticated WAL encryption",
            CompensationHint::PermissionDenied,
        );
    }

    let signing_valid = delta_signature_valid(
        delta_msg,
        identity.user_id,
        session_signing_key,
        session_producer_id,
        session_epoch,
        signing_required,
    );
    if !signing_valid {
        return terminal_reject(
            delta_msg,
            "CRDT delta signature is missing or invalid",
            CompensationHint::PermissionDenied,
        );
    }

    // Quota enforcement — reject before dispatch.
    if let Err(e) = shared.check_tenant_quota(tenant_id) {
        warn!(error = %e, "sync: delta validation rejected by quota");
        let detail = e.to_string();
        return terminal_reject(
            delta_msg,
            detail.clone(),
            CompensationHint::Custom {
                constraint: "quota".into(),
                detail,
            },
        );
    }

    // Hold the delta's Loro peer id to this session's producer before a
    // surrogate is assigned or anything is dispatched. A colliding peer id is
    // refused here because it cannot be refused later: past this point the
    // merge absorbs the delta and reports success.
    let peer_identity = admit_peer_identity(
        shared,
        PeerIdentityRequest {
            database_id,
            tenant_id,
            collection: &delta_msg.collection,
            peer_id: delta_msg.peer_id,
            producer_id: session_producer_id,
        },
    )
    .await;
    match peer_identity {
        Ok(PeerIdentity::Owned | PeerIdentity::Unbound) => {}
        Ok(PeerIdentity::Collision { owner_producer_id }) => {
            let reason =
                peer_collision_reason(&delta_msg.collection, delta_msg.peer_id, owner_producer_id);
            return terminal_reject(
                delta_msg,
                reason.clone(),
                CompensationHint::Custom {
                    constraint: CompensationHint::PEER_ID_COLLISION.into(),
                    detail: reason,
                },
            );
        }
        Err(error) => {
            // The binding could not be established, so whether this peer id is
            // safe to write under is unknown. Admitting the delta would gamble
            // the client's write on it; refusing retryably costs a re-push.
            warn!(
                %error,
                collection = %delta_msg.collection,
                peer_id = delta_msg.peer_id,
                "sync: peer-id binding unavailable; refusing the delta retryably"
            );
            return DeltaDispatchOutcome::refused(retryable_binding_refusal(delta_msg));
        }
    }

    let surrogate = match shared.surrogate_assigner.assign(
        database_id,
        tenant_id,
        &delta_msg.collection,
        delta_msg.document_id.as_bytes(),
    ) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "sync: surrogate assignment failed");
            let detail = e.to_string();
            return terminal_reject(
                delta_msg,
                detail.clone(),
                CompensationHint::Custom {
                    constraint: "surrogate".into(),
                    detail,
                },
            );
        }
    };

    // Server-authoritative provenance: producer_id + epoch come from the
    // session's handshake-assigned identity, never from the wire message — a
    // client cannot spoof another producer's id or replay a fenced epoch. Only
    // `seq` is client-owned (the per-producer monotonic counter the gate validates).
    let prov = SyncProvenance {
        producer_id: session_producer_id,
        epoch: session_epoch,
        stream_id: stream_id_for(EngineKind::Crdt, &delta_msg.collection),
        seq: delta_msg.seq,
    };

    let plan = PhysicalPlan::Crdt(CrdtOp::ApplyAuthenticated {
        collection: delta_msg.collection.clone(),
        document_id: delta_msg.document_id.clone(),
        delta: delta_msg.delta.clone(),
        peer_id: delta_msg.peer_id,
        mutation_id: delta_msg.mutation_id,
        surrogate,
        provenance: prov,
        constraint_version_required,
        expected_frontier_digest: None,
        auth_user_id: identity.user_id,
        auth_device_id: session_producer_id,
        auth_seq_no: delta_msg.seq,
        delta_signature: delta_msg.delta_signature,
        signing_required,
    });

    let vshard_id =
        crate::types::VShardId::from_collection_in_database(database_id, &delta_msg.collection);
    let authorized = super::super::super::raft_dispatch::authorize_sync_task(
        shared,
        Some(identity),
        tenant_id,
        database_id,
        vshard_id,
        plan,
    );
    let _request = shared.tenant_request_guard(tenant_id);
    let dispatch_result = match authorized {
        Ok(authorized) => {
            super::super::super::raft_dispatch::dispatch_sync_bytes(
                shared,
                &delta_msg.collection,
                authorized,
                Duration::from_secs(10),
                crate::event::EventSource::CrdtSync,
                &policy,
            )
            .await
        }
        Err(error) => Err(error),
    };

    let trimmed_ops = dispatch_result
        .as_ref()
        .map(|outcome| outcome.trimmed_ops)
        .unwrap_or(0);
    DeltaDispatchOutcome {
        frame: frame_for_dispatch(
            delta_msg,
            &ack_frame,
            dispatch_result.map(|outcome| outcome.payload),
        ),
        trimmed_ops,
    }
}

/// Refuse retryably: nothing was applied and the identical delta at the same
/// sequence should be re-pushed once the binding can be established.
fn retryable_binding_refusal(delta_msg: &DeltaPushMsg) -> Option<SyncFrame> {
    use nodedb_types::sync::wire::AckStatus;

    use super::super::super::wire::DeltaAckMsg;

    let ack = DeltaAckMsg {
        mutation_id: delta_msg.mutation_id,
        lsn: 0,
        clock_skew_warning_ms: None,
        applied_seq: delta_msg.seq.saturating_sub(1),
        status: AckStatus::Gap {
            expected: delta_msg.seq,
        },
    };
    SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack)
}

/// A refusal decided in the Control Plane before the apply was ever attempted.
///
/// Every one of these is permanent for the frame as sent — a missing
/// collection, a revoked grant, an invalid signature, an exhausted quota —
/// so the sender must compensate rather than re-push identical bytes.
fn terminal_reject(
    delta_msg: &DeltaPushMsg,
    reason: impl Into<String>,
    compensation: CompensationHint,
) -> DeltaDispatchOutcome {
    let reject = DeltaRejectMsg {
        mutation_id: delta_msg.mutation_id,
        reason: reason.into(),
        compensation: Some(compensation),
    };
    DeltaDispatchOutcome::refused(SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject))
}
