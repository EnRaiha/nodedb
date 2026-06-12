// SPDX-License-Identifier: BUSL-1.1

//! Columnar insert handler for sync sessions.
//!
//! Decodes a `ColumnarInsertMsg` from a Lite client, deserializes the
//! row payloads (MessagePack `Vec<Value>` per row), converts positional
//! rows to named `Value::Object` rows using the schema carried in
//! `schema_bytes`, and dispatches to the Data Plane via a [`ColumnarDispatcher`].
//!
//! The handler follows the same structural pattern as `timeseries_handler`:
//! a dispatcher trait keeps the ingest and the ACK generation coupled so
//! an ACK cannot be returned without at least attempting dispatch.

use async_trait::async_trait;
use tracing::{debug, error};

use nodedb_types::value::Value;

use super::session::SyncSession;
use super::wire::*;
use crate::types::{DatabaseId, TenantId, VShardId};

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Encapsulates async Data Plane dispatch for a decoded columnar insert.
///
/// Returns the raw `Response.payload` bytes from the Data Plane so that the
/// handler can decode the [`SyncAckResult`] for gate status propagation.
#[async_trait]
pub trait ColumnarDispatcher: Send + Sync {
    /// Dispatch a batch of rows to the Data Plane for columnar ingest.
    ///
    /// `rows` contains one element per accepted row, each a `Vec<Value>`
    /// in schema column order. `schema_bytes` is the MessagePack-encoded
    /// `ColumnarSchema` hint from the wire message (may be empty).
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        rows: Vec<Vec<Value>>,
        schema_bytes: Vec<u8>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;
}

// ── SharedState adapter ──────────────────────────────────────────────────────

/// Production dispatcher: routes the insert to the Data Plane via the SPSC
/// bridge using `EventSource::CrdtSync` so that AFTER triggers are not
/// re-fired on synced data.
pub struct SharedStateColumnarDispatcher<'a> {
    pub shared: &'a crate::control::state::SharedState,
}

#[async_trait]
impl<'a> ColumnarDispatcher for SharedStateColumnarDispatcher<'a> {
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        rows: Vec<Vec<Value>>,
        schema_bytes: Vec<u8>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use crate::control::server::wal_dispatch::wal_append_columnar;
        use nodedb_physical::physical_plan::columnar::{ColumnarInsertIntent, ColumnarOp};
        use nodedb_types::columnar::ColumnarSchema;
        use std::collections::HashMap;

        let prov = provenance;

        // Decode column names from schema_bytes so we can build object rows.
        // The Data Plane columnar insert handler expects rows as
        // `Value::Object(HashMap<String, Value>)`, not positional arrays.
        let column_names: Vec<String> = if schema_bytes.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack::<ColumnarSchema>(&schema_bytes)
                .map(|s| s.columns.into_iter().map(|c| c.name).collect())
                .unwrap_or_default()
        };

        // Convert each row from positional Vec<Value> to named Value::Object.
        // If column_names is empty (no schema_bytes), fall back to positional
        // "col0", "col1", ... names so rows are never silently dropped.
        let object_rows: Vec<Value> = rows
            .into_iter()
            .map(|row| {
                let mut map = HashMap::with_capacity(row.len());
                for (i, val) in row.into_iter().enumerate() {
                    let key = column_names
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| format!("col{i}"));
                    map.insert(key, val);
                }
                Value::Object(map)
            })
            .collect();

        // Encode as msgpack — the Data Plane handler calls `value_from_msgpack(payload)`
        // and expects Value::Array([Value::Object, ...]).
        let array_value = Value::Array(object_rows);
        let payload =
            nodedb_types::value_to_msgpack(&array_value).map_err(|e| crate::Error::Internal {
                detail: format!("columnar sync: msgpack serialize rows: {e}"),
            })?;

        // Allocate a WAL LSN on the Control Plane before dispatching to the
        // Data Plane. This is the canonical LSN for dedup tracking.
        let wal_lsn = wal_append_columnar(
            &self.shared.wal,
            tenant_id,
            vshard,
            DatabaseId::DEFAULT,
            &collection,
            &payload,
            Some(&prov),
        )?
        .map(|lsn| lsn.as_u64());

        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: collection.clone(),
            payload,
            format: "msgpack".to_string(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes,
            provenance: Some(prov),
            wal_lsn,
        });

        super::raft_dispatch::dispatch_sync_payload(self.shared, tenant_id, vshard, plan).await
    }
}

// ── NoOp dispatcher (loud failure) ──────────────────────────────────────────

/// Dispatcher used when `SharedState` is unavailable.
///
/// Returns a loud `Internal` error — intentionally NOT a silent no-op.
pub struct NoOpColumnarDispatcher;

#[async_trait]
impl ColumnarDispatcher for NoOpColumnarDispatcher {
    async fn dispatch_insert(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _rows: Vec<Vec<Value>>,
        _schema_bytes: Vec<u8>,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("columnar insert"))
    }
}

// ── Handler ──────────────────────────────────────────────────────────────────

impl SyncSession {
    /// Process a columnar batch insert: decode rows, dispatch to the Data
    /// Plane, and return an ACK frame.
    ///
    /// If the dispatcher fails, all rows are reported as rejected.
    /// An unauthenticated session returns a rejection ACK without calling
    /// the dispatcher.
    pub async fn handle_columnar_insert<D: ColumnarDispatcher>(
        &mut self,
        msg: &ColumnarInsertMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if !self.authenticated {
            let ack = ColumnarInsertAckMsg {
                collection: msg.collection.clone(),
                batch_id: msg.batch_id,
                accepted: 0,
                rejected: msg.rows.len() as u64,
                reject_reason: Some("unauthenticated".to_string()),
                applied_seq: 0,
                status: AckStatus::Applied,
            };
            return SyncFrame::try_encode(SyncMessageType::ColumnarInsertAck, &ack);
        }

        // Decode each row from MessagePack Vec<Value>.
        //
        // Fail-fast: the first decode failure aborts the whole batch and
        // returns a rejection ACK pinpointing the failing row index. We do
        // NOT silently shrink the batch — that would partially apply user
        // writes while reporting success on the rest.
        let total = msg.rows.len() as u64;
        let mut decoded_rows: Vec<Vec<Value>> = Vec::with_capacity(msg.rows.len());
        for (i, row_bytes) in msg.rows.iter().enumerate() {
            match zerompk::from_msgpack::<Vec<Value>>(row_bytes) {
                Ok(row) => decoded_rows.push(row),
                Err(e) => {
                    error!(
                        session = %self.session_id,
                        collection = %msg.collection,
                        batch_id = msg.batch_id,
                        row_index = i,
                        error = %e,
                        "columnar sync: row decode failed; rejecting entire batch"
                    );
                    let ack = ColumnarInsertAckMsg {
                        collection: msg.collection.clone(),
                        batch_id: msg.batch_id,
                        accepted: 0,
                        rejected: total,
                        reject_reason: Some(format!("row {i} msgpack decode failed: {e}")),
                        applied_seq: 0,
                        status: AckStatus::Applied,
                    };
                    return SyncFrame::try_encode(SyncMessageType::ColumnarInsertAck, &ack);
                }
            }
        }

        let decoded = decoded_rows.len() as u64;

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &msg.collection);

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            batch_id = msg.batch_id,
            rows = decoded,
            lite_id = %msg.lite_id,
            "columnar insert: dispatching to Data Plane"
        );

        match dispatcher
            .dispatch_insert(
                tenant_id,
                vshard,
                msg.collection.clone(),
                decoded_rows,
                msg.schema_bytes.clone(),
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Columnar,
                        &msg.collection,
                    ),
                    seq: msg.seq,
                },
            )
            .await
        {
            Ok(payload_bytes) => {
                // Decode SyncAckResult from the Data Plane response payload.
                // On decode failure fall back to Applied so the client is
                // still ACKed (the insert succeeded).
                let gate_result = super::ack_decode::decode_sync_ack(
                    &payload_bytes,
                    "columnar",
                    &self.session_id,
                    &msg.collection,
                    msg.seq,
                );

                self.mutations_processed += decoded;
                let ack = ColumnarInsertAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted: decoded,
                    rejected: total.saturating_sub(decoded),
                    reject_reason: None,
                    applied_seq: gate_result.applied_seq,
                    status: gate_result.status,
                };
                SyncFrame::try_encode(SyncMessageType::ColumnarInsertAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    batch_id = msg.batch_id,
                    error = %e,
                    "columnar insert dispatch failed; reporting rows as rejected"
                );
                let ack = ColumnarInsertAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted: 0,
                    rejected: total,
                    reject_reason: Some(e.to_string()),
                    applied_seq: 0,
                    status: AckStatus::Applied,
                };
                SyncFrame::try_encode(SyncMessageType::ColumnarInsertAck, &ack)
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    type MockCallLog = Arc<Mutex<Vec<(TenantId, String, Vec<Vec<Value>>)>>>;

    struct MockDispatcher {
        calls: MockCallLog,
        result: crate::Result<Vec<u8>>,
    }

    impl MockDispatcher {
        fn ok(n: u64) -> (Self, MockCallLog) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            // Encode a SyncAckResult so the handler can decode it.
            let ack_result = nodedb_types::sync::wire::SyncAckResult {
                status: AckStatus::Applied,
                applied_seq: n,
            };
            let payload = zerompk::to_msgpack_vec(&ack_result).expect("encode SyncAckResult");
            (
                Self {
                    calls: calls.clone(),
                    result: Ok(payload),
                },
                calls,
            )
        }

        fn err() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                result: Err(crate::Error::Internal {
                    detail: "mock failure".to_string(),
                }),
            }
        }
    }

    #[async_trait]
    impl ColumnarDispatcher for MockDispatcher {
        async fn dispatch_insert(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            collection: String,
            rows: Vec<Vec<Value>>,
            _schema_bytes: Vec<u8>,
            _provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            self.calls
                .lock()
                .unwrap()
                .push((tenant_id, collection, rows));
            match &self.result {
                Ok(b) => Ok(b.clone()),
                Err(e) => Err(crate::Error::Internal {
                    detail: e.to_string(),
                }),
            }
        }
    }

    fn make_session() -> SyncSession {
        SyncSession::new("test-columnar-session".to_string())
    }

    fn encode_row(values: Vec<Value>) -> Vec<u8> {
        zerompk::to_msgpack_vec(&values).expect("encode row")
    }

    fn make_insert_msg(collection: &str, rows: Vec<Vec<Value>>) -> ColumnarInsertMsg {
        ColumnarInsertMsg {
            lite_id: "lite-test".to_string(),
            collection: collection.to_string(),
            rows: rows.iter().map(|r| encode_row(r.clone())).collect(),
            batch_id: 1,
            schema_bytes: Vec::new(),
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    #[tokio::test]
    async fn unauthenticated_returns_rejection() {
        let mut session = make_session();
        let (mock, calls) = MockDispatcher::ok(0);
        let msg = make_insert_msg(
            "metrics",
            vec![vec![Value::Integer(1), Value::Float(std::f64::consts::PI)]],
        );

        let frame = session.handle_columnar_insert(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: ColumnarInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(ack.accepted, 0);
        assert_eq!(ack.rejected, 1);
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn authenticated_dispatches_and_acks() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, calls) = MockDispatcher::ok(2);
        let msg = make_insert_msg(
            "metrics",
            vec![
                vec![Value::Integer(1), Value::Float(1.0)],
                vec![Value::Integer(2), Value::Float(2.0)],
            ],
        );

        let frame = session.handle_columnar_insert(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: ColumnarInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(ack.accepted, 2);
        assert_eq!(ack.rejected, 0);

        let log = calls.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].1, "metrics");
        assert_eq!(log[0].2.len(), 2);
    }

    #[tokio::test]
    async fn dispatch_failure_rejects_all() {
        let mut session = make_session();
        session.authenticated = true;
        let mock = MockDispatcher::err();
        let msg = make_insert_msg("metrics", vec![vec![Value::Integer(1), Value::Float(1.0)]]);

        let frame = session.handle_columnar_insert(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: ColumnarInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(ack.accepted, 0);
        assert_eq!(ack.rejected, 1);
        assert!(ack.reject_reason.is_some());
    }
}
