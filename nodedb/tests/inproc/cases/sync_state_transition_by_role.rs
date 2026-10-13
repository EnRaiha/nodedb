// SPDX-License-Identifier: BUSL-1.1

//! A role-guarded state transition binds a synced write the same way it binds
//! a SQL write. The session's authenticated roles decide it.
//!
//! The test drives the Lite sync wire end to end. A sync session pushes row
//! `a` in `draft` as a Loro delta. The collection then declares `status
//! TRANSITIONS ('draft' -> 'active' BY ROLE 'approver')`. Two sessions push the
//! same `draft -> active` update, each forked from the base. The session
//! without `approver` is refused and the row stays `draft`. The session with
//! `approver` is admitted and the row reads `active`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb_test_support::jwks_fixture::{JwksFixture, now_secs};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::sync::wire::{
    AckStatus, DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg, HandshakeAckMsg, HandshakeMsg, SyncFrame,
    SyncMessageType,
};

/// Sync sessions authenticate into tenant 1, the tenant the pgwire harness
/// superuser writes into.
const TENANT: u64 = 1;
const ISSUER: &str = "https://sync-state-transition-by-role.example/";
const AUDIENCE: &str = "nodedb-sync";

/// How long the test waits for the server's verdict on one delta.
const FRAME_WAIT: Duration = Duration::from_secs(10);

/// How long a client keeps re-pushing a delta the server answers with `Gap`.
const GAP_RETRY_WINDOW: Duration = Duration::from_secs(30);

/// Pause between re-pushes of a delta answered with `Gap`.
const GAP_RETRY_PAUSE: Duration = Duration::from_millis(100);

const COLL: &str = "sync_role_flow";
const ROW: &str = "a";

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The server's final verdict on one pushed delta.
#[derive(Debug, PartialEq)]
enum Verdict {
    Applied,
    Refused(String),
    NoAnswer,
    /// The server answered `Gap` for the whole retry window.
    StillGapped,
}

struct Harness {
    server: TestServer,
    fixture: JwksFixture,
    addr: SocketAddr,
}

impl Harness {
    async fn start() -> Self {
        let fixture = JwksFixture::spawn().await;
        let registry = JwksRegistry::init(fixture.auth_config(ISSUER, AUDIENCE, TENANT))
            .await
            .expect("test JWKS registry must initialize");
        let server = TestServer::start_with_jwks(std::sync::Arc::new(registry)).await;
        let config = SyncListenerConfig {
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            ..Default::default()
        };
        let (shutdown_bus, _shutdown_handle) = nodedb::control::shutdown::ShutdownBus::new(
            std::sync::Arc::new(nodedb::control::shutdown::ShutdownWatch::new()),
        );
        let listener = start_sync_listener(
            config,
            Some(std::sync::Arc::clone(&server.shared)),
            shutdown_bus,
        )
        .await
        .expect("start sync listener");
        let addr = listener.config.listen_addr;
        Self {
            server,
            fixture,
            addr,
        }
    }

    fn mint_token(&self, subject: &str, roles: &[&str]) -> String {
        let now = now_secs();
        self.fixture.mint(&serde_json::json!({
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": subject,
            "roles": roles,
            "iat": now,
            "exp": now + 600,
        }))
    }

    /// Open a sync session authenticated as `subject` with `roles`.
    async fn session(&self, subject: &str, roles: &[&str]) -> Ws {
        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{}", self.addr))
            .await
            .expect("sync websocket connect");
        let msg = HandshakeMsg {
            jwt_token: self.mint_token(subject, roles),
            vector_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            client_version: "sync-role-test".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: nodedb_types::wire_version::WIRE_FORMAT_VERSION,
        };
        send(&mut ws, SyncMessageType::Handshake, &msg).await;
        let frame = next_frame(&mut ws)
            .await
            .expect("handshake must be answered");
        assert_eq!(frame.msg_type, SyncMessageType::HandshakeAck);
        let ack: HandshakeAckMsg = frame.decode_body().expect("handshake ack decodes");
        assert!(ack.success, "handshake rejected: {:?}", ack.error);
        ws
    }

    /// The row's current `status`, read from the document the synced apply
    /// materializes. `crdt_state` returns the Loro snapshot, which keeps
    /// every past value in its history, so it cannot tell the current status.
    async fn status(&self) -> String {
        let rows = self
            .server
            .query_text(&format!("SELECT status FROM {COLL} WHERE id = '{ROW}'"))
            .await
            .expect("read status");
        assert_eq!(rows.len(), 1, "row {ROW} must exist: {rows:?}");
        rows[0].clone()
    }
}

async fn send<T: zerompk::ToMessagePack>(ws: &mut Ws, msg_type: SyncMessageType, body: &T) {
    let frame = SyncFrame::try_encode(msg_type, body).expect("frame encodes");
    ws.send(Message::Binary(frame.to_bytes().into()))
        .await
        .expect("frame send");
}

async fn next_frame(ws: &mut Ws) -> Option<SyncFrame> {
    let deadline = tokio::time::Instant::now() + FRAME_WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                if let Some(frame) = SyncFrame::from_bytes(&bytes) {
                    return Some(frame);
                }
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
        }
    }
}

/// One answer to one push of a delta.
enum Answer {
    Final(Verdict),
    /// Retryable: nothing applied, and the client re-pushes the same delta.
    /// A collection whose constraints this core has not installed yet
    /// answers this way.
    Gap,
}

/// Push the updates `doc` holds past `base` as a Lite client does, and wait
/// for the server's final verdict on it. A `Gap` answer is re-pushed, as a
/// Lite client re-pushes it, until the server gives a final verdict.
async fn push(
    ws: &mut Ws,
    doc: &loro::LoroDoc,
    base: &loro::VersionVector,
    mutation_id: u64,
) -> Verdict {
    let deadline = tokio::time::Instant::now() + GAP_RETRY_WINDOW;
    loop {
        match push_once(ws, doc, base, mutation_id).await {
            Answer::Final(verdict) => return verdict,
            Answer::Gap if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(GAP_RETRY_PAUSE).await;
            }
            Answer::Gap => return Verdict::StillGapped,
        }
    }
}

async fn push_once(
    ws: &mut Ws,
    doc: &loro::LoroDoc,
    base: &loro::VersionVector,
    mutation_id: u64,
) -> Answer {
    let delta = doc
        .export(loro::ExportMode::updates(base))
        .expect("export loro updates");
    let msg = DeltaPushMsg {
        collection: COLL.into(),
        document_id: ROW.into(),
        delta,
        peer_id: doc.peer_id(),
        mutation_id,
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
        device_id: 0,
        delta_signature: [0; 32],
    };
    send(ws, SyncMessageType::DeltaPush, &msg).await;
    while let Some(frame) = next_frame(ws).await {
        match frame.msg_type {
            SyncMessageType::DeltaReject => {
                let reject: DeltaRejectMsg = frame.decode_body().expect("reject decodes");
                if reject.mutation_id == mutation_id {
                    return Answer::Final(Verdict::Refused(reject.reason));
                }
            }
            SyncMessageType::DeltaAck => {
                let ack: DeltaAckMsg = frame.decode_body().expect("ack decodes");
                if ack.mutation_id != mutation_id {
                    continue;
                }
                match ack.status {
                    AckStatus::Applied => return Answer::Final(Verdict::Applied),
                    // Provisional: the durable apply has not answered yet.
                    AckStatus::Accepted => continue,
                    AckStatus::Gap { .. } => return Answer::Gap,
                    other => return Answer::Final(Verdict::Refused(format!("{other:?}"))),
                }
            }
            _ => continue,
        }
    }
    Answer::Final(Verdict::NoAnswer)
}

/// A fork of `base_doc` that moves the row to `next`.
fn status_fork(base_doc: &loro::LoroDoc, next: &str) -> loro::LoroDoc {
    let fork = base_doc.fork();
    // The row map the server's CRDT state writes: the existing container, or
    // a new one at the row key.
    let coll = fork.get_map(COLL);
    let row = match coll.get(ROW) {
        Some(loro::ValueOrContainer::Container(loro::Container::Map(m))) => m,
        _ => coll
            .insert_container(ROW, loro::LoroMap::new())
            .expect("row container"),
    };
    row.insert("status", next).expect("status field");
    fork.commit();
    fork
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synced_update_judges_the_session_role() {
    let h = Harness::start().await;
    h.server.exec("CREATE ROLE approver").await.unwrap();
    h.server
        .exec(&format!(
            "CREATE COLLECTION {COLL} (id TEXT PRIMARY KEY, status TEXT) WITH (crdt='true')"
        ))
        .await
        .unwrap();
    // The `PRIMARY KEY` makes the collection's CRDT constraint version 1. A
    // synced delta is fenced until that version is installed on the core
    // that applies it, and answers `Gap` meanwhile. The server's
    // constraint-reconcile loop installs it. This harness starts no
    // background loops, so the test runs one reconcile pass itself.
    let mut delivered = HashMap::new();
    let installed =
        nodedb::bootstrap::constraint_reconcile::reconcile_once(&h.server.shared, &mut delivered)
            .await;
    assert!(
        installed >= 1,
        "the reconcile pass must install the collection's constraints"
    );

    let mut approver = h
        .session("sync_role_approver", &["readwrite", "approver"])
        .await;
    let mut clerk = h.session("sync_role_clerk", &["readwrite"]).await;

    // The base: row `a` in `draft`. An insert has no prior row, so no
    // transition is judged.
    let doc = loro::LoroDoc::new();
    let empty = doc.oplog_vv();
    let row = doc
        .get_map(COLL)
        .insert_container(ROW, loro::LoroMap::new())
        .expect("row container");
    row.insert("id", ROW).expect("id field");
    row.insert("status", "draft").expect("status field");
    doc.commit();
    assert_eq!(push(&mut approver, &doc, &empty, 1).await, Verdict::Applied);
    assert_eq!(h.status().await, "draft");

    h.server
        .exec(&format!(
            "ALTER COLLECTION {COLL} ADD CONSTRAINT approval \
             ON COLUMN status TRANSITIONS ('draft' -> 'active' BY ROLE 'approver')"
        ))
        .await
        .unwrap();
    // Install any CRDT constraint version the ALTER changed, as the
    // reconcile loop does.
    nodedb::bootstrap::constraint_reconcile::reconcile_once(&h.server.shared, &mut delivered).await;
    let base = doc.oplog_vv();

    let refused = push(&mut clerk, &status_fork(&doc, "active"), &base, 2).await;
    assert!(
        matches!(refused, Verdict::Refused(_)),
        "a session without the role must be refused: {refused:?}"
    );
    assert_eq!(
        h.status().await,
        "draft",
        "a refused delta must leave the row in draft"
    );

    let admitted = push(&mut approver, &status_fork(&doc, "active"), &base, 3).await;
    assert_eq!(
        admitted,
        Verdict::Applied,
        "a session with the role must be admitted"
    );
    assert_eq!(
        h.status().await,
        "active",
        "an admitted delta must store active"
    );
}
