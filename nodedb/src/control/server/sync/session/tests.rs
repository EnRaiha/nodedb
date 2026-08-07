// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;
use std::sync::Arc;

use crate::control::security::audit::{AuditEvent, AuditLog};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};
use crate::control::state::SharedState;
use crate::types::TenantId;
use crate::wal::WalManager;

use super::super::dlq::{DlqConfig, SyncDlq};
use super::super::rate_limit::RateLimitConfig;
use super::super::wire::*;
use super::state::SyncSession;

fn make_session() -> SyncSession {
    SyncSession::new("test-session-1".into())
}

fn make_authenticated_session() -> SyncSession {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id = Some(TenantId::new(1));
    session.username = Some("alice".into());
    session.identity = Some(AuthenticatedIdentity::new_regular(
        1,
        "alice",
        TenantId::new(1),
        crate::control::security::identity::AuthMethod::ApiKey,
        vec![crate::control::security::identity::Role::ReadWrite],
        None,
        AuthenticatedIdentity::default_database_set(false),
    ));
    session
}

#[tokio::test]
async fn handshake_rejects_invalid_jwt() {
    let mut session = make_session();

    let msg = HandshakeMsg {
        jwt_token: "invalid.token.here".into(),
        vector_clock: HashMap::new(),
        subscribed_shapes: vec![],
        client_version: "0.1".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: 1,
    };

    let response = session
        .handle_handshake(&msg, HashMap::new(), None)
        .await
        .expect("handshake response");
    assert_eq!(response.msg_type, SyncMessageType::HandshakeAck);

    let ack: HandshakeAckMsg = response.decode_body().unwrap();
    assert!(!ack.success);
    assert!(ack.error.is_some());
    assert!(!session.authenticated);
}

#[test]
fn delta_push_rejected_before_auth() {
    let mut session = make_session();

    let msg = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d1".into(),
        delta: vec![1, 2, 3],
        peer_id: 1,
        mutation_id: 100,
        device_id: 0,
        delta_signature: [0; 32],
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    let response = session.handle_delta_push(&msg, None, None, None);
    assert!(response.is_some());
    let frame = response.unwrap();
    assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
    assert_eq!(session.mutations_rejected, 1);
}

#[tokio::test]
async fn production_process_frame_denies_unauthorized_delta_before_provisional_ack() {
    // Retain the WAL and Data-Plane dispatcher endpoints for the complete
    // SharedState lifetime, matching the production-backed test fixture.
    let _tempdir = tempfile::tempdir().expect("temporary WAL directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&_tempdir.path().join("sync-session.wal"))
            .expect("open test WAL"),
    );
    let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).expect("construct shared state");

    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id = Some(TenantId::new(1));
    session.username = Some("collection-reader".into());
    session.identity = Some(AuthenticatedIdentity::new_regular(
        7,
        "collection-reader",
        TenantId::new(1),
        crate::control::security::identity::AuthMethod::ApiKey,
        vec![Role::Custom("sync-reader".into())],
        None,
        AuthenticatedIdentity::default_database_set(false),
    ));

    let msg = DeltaPushMsg {
        collection: "orders".into(),
        document_id: "o1".into(),
        delta: nodedb_types::json_to_msgpack(&serde_json::json!({"status": "active"}))
            .expect("encode delta"),
        peer_id: 1,
        mutation_id: 77,
        device_id: 0,
        delta_signature: [0; 32],
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };
    let frame = SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg).expect("encode frame");

    // This is the production process_frame path: shared state is supplied,
    // while audit/DLQ are deliberately not pre-locked by the caller.
    let response = session
        .process_frame(&frame, Some(&shared.rls), None, None, Some(&shared))
        .await
        .expect("permission denial response");

    assert_eq!(response.msg_type, SyncMessageType::DeltaReject);
    assert_ne!(response.msg_type, SyncMessageType::DeltaAck);
    let reject: DeltaRejectMsg = response.decode_body().expect("decode rejection");
    assert_eq!(
        reject.compensation,
        Some(CompensationHint::PermissionDenied)
    );
    assert_eq!(session.mutations_processed, 0);
    assert_eq!(
        shared
            .audit
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .query_by_event(&AuditEvent::PermissionDenied)
            .len(),
        1,
        "the production authorization gate records exactly one denial"
    );
}

#[test]
fn delta_push_accepted_when_authenticated() {
    let mut session = make_authenticated_session();

    let data = serde_json::json!({"status": "active"});
    let msg = DeltaPushMsg {
        collection: "orders".into(),
        device_id: 0,
        delta_signature: [0; 32],
        document_id: "o1".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 42,
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    let response = session.handle_delta_push(&msg, None, None, None);
    assert!(response.is_some());
    assert_eq!(response.unwrap().msg_type, SyncMessageType::DeltaAck);
    assert_eq!(session.mutations_processed, 1);
    // The subscription tracker picked up the collection.
    assert!(
        session
            .tracked_collections
            .contains(&(1, "orders".to_string()))
    );
}

#[test]
fn delta_push_defers_rls_until_authoritative_admission() {
    let mut session = make_authenticated_session();

    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};

    let rls_store = RlsPolicyStore::new();
    let predicate = RlsPredicate::Compare {
        field: "status".into(),
        op: CompareOp::Eq,
        value: PredicateValue::Literal(serde_json::json!("active")),
    };
    rls_store
        .create_policy(RlsPolicy {
            name: "require_active".into(),
            collection: "orders".into(),
            tenant_id: 1,
            policy_type: PolicyType::Write,
            compiled_predicate: Some(predicate),
            mode: crate::control::security::predicate::PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        })
        .unwrap();

    let mut audit_log = AuditLog::new(100);
    let mut dlq = SyncDlq::new(DlqConfig::default());

    let data = serde_json::json!({"status": "draft"});
    let msg = DeltaPushMsg {
        collection: "orders".into(),
        device_id: 0,
        delta_signature: [0; 32],
        document_id: "o1".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 42,
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    let response =
        session.handle_delta_push(&msg, Some(&rls_store), Some(&mut audit_log), Some(&mut dlq));

    assert_eq!(
        response.expect("preliminary ack").msg_type,
        SyncMessageType::DeltaAck
    );
    assert_eq!(session.mutations_silent_dropped, 0);
    assert_eq!(session.mutations_processed, 1);
    assert_eq!(audit_log.len(), 0);
    assert_eq!(dlq.total_entries(), 0);
}

#[test]
fn oversized_delta_rejects_before_rate_limit_or_dlq_clone() {
    let mut session = make_authenticated_session();
    let mut audit_log = AuditLog::new(100);
    let mut dlq = SyncDlq::new(DlqConfig::default());
    let msg = DeltaPushMsg {
        collection: "orders".into(),
        document_id: "o1".into(),
        delta: vec![0; nodedb_crdt::DEFAULT_MAX_DELTA_BYTES + 1],
        peer_id: 1,
        mutation_id: 43,
        device_id: 0,
        delta_signature: [0; 32],
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    let response = session
        .handle_delta_push(&msg, None, Some(&mut audit_log), Some(&mut dlq))
        .expect("oversized reject");
    assert_eq!(response.msg_type, SyncMessageType::DeltaReject);
    assert_eq!(session.mutations_rejected, 1);
    assert_eq!(session.mutations_processed, 0);
    assert_eq!(session.mutations_silent_dropped, 0);
    assert_eq!(audit_log.len(), 0);
    assert_eq!(dlq.total_entries(), 0);
}

#[test]
fn delta_push_rate_limited_silent_drop() {
    let rate_config = RateLimitConfig {
        rate_per_sec: 0.0,
        burst: 1,
    };
    let mut session = SyncSession::with_rate_limit("rate-test".into(), &rate_config);
    session.authenticated = true;
    session.tenant_id = Some(TenantId::new(1));
    session.username = Some("bob".into());
    session.identity = Some(AuthenticatedIdentity::new_regular(
        2,
        "bob",
        TenantId::new(1),
        crate::control::security::identity::AuthMethod::ApiKey,
        vec![crate::control::security::identity::Role::ReadWrite],
        None,
        AuthenticatedIdentity::default_database_set(false),
    ));

    let data = serde_json::json!({"key": "value"});
    let msg = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d1".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 1,
        device_id: 0,
        delta_signature: [0; 32],
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    let r1 = session.handle_delta_push(&msg, None, None, None);
    assert!(r1.is_some());
    assert_eq!(session.mutations_processed, 1);

    let mut audit_log = AuditLog::new(100);
    let mut dlq = SyncDlq::new(DlqConfig::default());

    let msg2 = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d2".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 2,
        device_id: 0,
        delta_signature: [0; 32],
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };
    let r2 = session.handle_delta_push(&msg2, None, Some(&mut audit_log), Some(&mut dlq));
    assert!(r2.is_none());
    assert_eq!(session.mutations_silent_dropped, 1);
    assert_eq!(dlq.total_entries(), 1);
}

#[test]
fn ping_pong() {
    let mut session = make_session();

    let ping = PingPongMsg {
        timestamp_ms: 99999,
        is_pong: false,
    };
    let response = session.handle_ping(&ping).expect("ping response");
    let pong: PingPongMsg = response.decode_body().unwrap();
    assert!(pong.is_pong);
    assert_eq!(pong.timestamp_ms, 99999);
}

#[test]
fn vector_clock_sync() {
    let mut session = make_session();
    session.authenticated = true;

    let mut clocks = HashMap::new();
    clocks.insert("orders".into(), 42u64);

    let msg = VectorClockSyncMsg {
        clocks,
        sender_id: 5,
    };
    let response = session
        .handle_vector_clock_sync(&msg)
        .expect("clock sync response");
    let sync: VectorClockSyncMsg = response.decode_body().unwrap();
    assert_eq!(*sync.clocks.get("orders").unwrap(), 42);
}

/// The Control Plane no longer keeps an in-memory replay-dedup map: every
/// `handle_delta_push` is processed. Idempotency is enforced durably at the
/// Data-Plane gate (`sync_admit`, keyed on producer-assigned seq — see the
/// `sync_gate` tests); for unfenced clients (producer_id == 0, as here) Loro
/// merge is idempotent, so re-applying converges to the same state. This test
/// pins the new contract: no CP-side short-circuit on a repeated mutation_id.
#[test]
fn delta_push_has_no_cp_side_dedup() {
    let mut session = make_authenticated_session();

    let data = serde_json::json!({"key": "value"});
    let delta = nodedb_types::json_to_msgpack(&data).unwrap();

    let make = |mutation_id: u64, doc: &str| DeltaPushMsg {
        collection: "docs".into(),
        document_id: doc.into(),
        delta: delta.clone(),
        peer_id: 42,
        mutation_id,
        device_id: 0,
        delta_signature: [0; 32],
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    // Same mutation_id twice, then an older id, then a newer id — every one is
    // processed by the CP now (dedup is the gate's job, not the session's).
    for (mid, doc) in [(5u64, "d1"), (5, "d1"), (3, "d0"), (6, "d2")] {
        let r = session.handle_delta_push(&make(mid, doc), None, None, None);
        assert_eq!(
            r.expect("delta ack").msg_type,
            SyncMessageType::DeltaAck,
            "every delta push is acked"
        );
    }
    assert_eq!(
        session.mutations_processed, 4,
        "CP processes every delta — no in-memory replay-dedup short-circuit"
    );
}

#[test]
fn crc32c_mismatch_rejects_delta() {
    let mut session = make_authenticated_session();

    let data = serde_json::json!({"key": "value"});
    let delta = nodedb_types::json_to_msgpack(&data).unwrap();

    let valid_checksum = crc32c::crc32c(&delta);
    let msg_ok = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d1".into(),
        delta: delta.clone(),
        peer_id: 1,
        mutation_id: 1,
        device_id: 0,
        delta_signature: [0; 32],
        checksum: valid_checksum,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };
    let r1 = session.handle_delta_push(&msg_ok, None, None, None);
    assert!(r1.is_some());
    assert_eq!(r1.unwrap().msg_type, SyncMessageType::DeltaAck);

    let msg_bad = DeltaPushMsg {
        collection: "docs".into(),
        document_id: "d2".into(),
        delta,
        peer_id: 1,
        mutation_id: 2,
        device_id: 0,
        delta_signature: [0; 32],
        checksum: valid_checksum ^ 0xDEAD,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };
    let r2 = session.handle_delta_push(&msg_bad, None, None, None);
    assert!(r2.is_some());
    assert_eq!(r2.unwrap().msg_type, SyncMessageType::DeltaReject);
    assert_eq!(session.mutations_rejected, 1);
}

/// The session emits its `DeltaAck` before the delta has been dispatched to
/// the Data Plane, and stamps it `AckStatus::Applied`. Nothing has been
/// applied at that point — the durable apply happens afterwards, and may be
/// refused. If the connection drops in that window, or the caller forwards the
/// provisional frame, the client records a write that does not exist.
///
/// An acknowledgement must never claim `Applied` before an apply has occurred.
#[test]
fn provisional_delta_ack_does_not_claim_applied() {
    let mut session = make_authenticated_session();

    let data = serde_json::json!({"status": "active"});
    let msg = DeltaPushMsg {
        collection: "orders".into(),
        document_id: "o1".into(),
        delta: nodedb_types::json_to_msgpack(&data).unwrap(),
        peer_id: 1,
        mutation_id: 42,
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
        device_id: 0,
        delta_signature: [0; 32],
    };

    let frame = session
        .handle_delta_push(&msg, None, None, None)
        .expect("an accepted push returns a frame");
    assert_eq!(frame.msg_type, SyncMessageType::DeltaAck);

    let ack: DeltaAckMsg = frame.decode_body().expect("ack body decodes");
    assert_ne!(
        ack.status,
        nodedb_types::sync::wire::AckStatus::Applied,
        "the session acknowledged a delta as Applied before it was dispatched \
         to the Data Plane, let alone applied"
    );
}
