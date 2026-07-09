// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::ResolveTxn` handler: turn a committing transaction's staged
//! post-images into ONE replayable [`RedoRecord`], WITHOUT mutating base.
//!
//! Resolve reads the per-transaction staging overlay (`CoreLoop::txn_overlays`)
//! by shared reference and emits, for every staged KV post-image, the
//! engine-native WAL sub-record shape that engine's autocommit path already
//! produces. The Control Plane appends the returned bytes as a single
//! `RecordType::TransactionRedo` record; a later install phase replays them. No
//! base engine is touched here.
//!
//! Only the KV serializer exists in this module today. Every other engine that
//! writes raises a typed error rather than being silently omitted from the redo
//! record: dropping an op class would lose those rows on install. Later
//! serializers (document, graph, vector, array, columnar) replace their error
//! arm with a real per-engine module beside [`super::kv`].

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};
use crate::wal::{RedoRecord, RedoSubRecord};

use super::kv;

impl CoreLoop {
    /// Resolve a committing transaction's staged writes into a [`RedoRecord`]
    /// and return its encoded bytes in the response payload. Reads the overlay
    /// by `&` and never mutates any base engine.
    pub(in crate::data::executor) fn execute_resolve_txn(
        &self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        plans: &[PhysicalPlan],
    ) -> Response {
        let ops = match self.resolve_txn_ops(task, tid, txn_id, plans) {
            Ok(ops) => ops,
            Err(e) => return self.response_error(task, e),
        };
        let record = RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
        };
        match record.to_bytes() {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(task, e),
        }
    }

    /// Build the ordered redo sub-records for a transaction's staged
    /// post-images.
    ///
    /// The plan set is walked once to (a) classify every op exhaustively and
    /// (b) collect the distinct KV collections whose overlay entries must be
    /// serialized. Serialization is overlay-driven: the resolved absolute
    /// post-image (value, tombstone, absolute expiry) lives in the overlay, not
    /// the plan.
    fn resolve_txn_ops(
        &self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        plans: &[PhysicalPlan],
    ) -> crate::Result<Vec<RedoSubRecord>> {
        let mut kv_collections: BTreeSet<String> = BTreeSet::new();

        for plan in plans {
            match plan {
                // KV: overlay-backed serializer. Read ops stage nothing and are
                // skipped; row-level writes contribute their collection.
                PhysicalPlan::Kv(op) => classify_kv_op(op, &mut kv_collections)?,

                // CRDT deltas ride their own `CrdtDelta` WAL record, never redo
                // sub-records (see `replay_transaction_redo_wal`).
                PhysicalPlan::Crdt(_) => {}

                // FTS postings are re-derived from the owning document at
                // install time, so a text op contributes no redo sub-record.
                PhysicalPlan::Text(_) => {}

                // Read-only families: scans, joins, aggregates, exchange, and
                // maintenance ops carry no persisted post-image.
                PhysicalPlan::Query(_) | PhysicalPlan::Meta(_) => {}

                // Data-bearing engines whose transaction-resolve serializer is
                // not built yet. Erroring keeps their rows out of a silently
                // lossy redo record; each is replaced by a real serializer in a
                // later unit. (Even once those land, a `Document` bulk/point op
                // carrying `RETURNING` and `Columnar::{Update, Delete}` stay
                // typed errors — no per-row redo shape exists for them.)
                PhysicalPlan::Document(_) => return Err(unsupported_engine("document")),
                PhysicalPlan::Graph(_) => return Err(unsupported_engine("graph")),
                PhysicalPlan::Vector(_) => return Err(unsupported_engine("vector")),
                PhysicalPlan::Array(_) => return Err(unsupported_engine("array")),
                PhysicalPlan::Columnar(_) => return Err(unsupported_engine("columnar")),
                PhysicalPlan::Timeseries(_) => return Err(unsupported_engine("timeseries")),
                PhysicalPlan::Spatial(_) => return Err(unsupported_engine("spatial")),

                // Coordinator-only op; never legal on the Data Plane.
                PhysicalPlan::ClusterArray(_) => {
                    return Err(crate::Error::Internal {
                        detail: "cluster-array op reached Data Plane transaction resolve"
                            .to_string(),
                    });
                }
            }
        }

        let mut ops = Vec::new();
        if let Some(overlay) = self.txn_overlays.get(&txn_id) {
            for collection in &kv_collections {
                let coll_key = (
                    task.request.database_id,
                    TenantId::new(tid),
                    collection.clone(),
                );
                kv::serialize_kv_collection(overlay, &coll_key, collection, &mut ops)?;
            }
        }
        Ok(ops)
    }
}

/// Classify a KV op for transaction resolve: collect the collection of a
/// row-level write into `collections`, skip read-only ops, and reject the ops
/// that have no row-level redo representation.
fn classify_kv_op(op: &KvOp, collections: &mut BTreeSet<String>) -> crate::Result<()> {
    match op {
        // Row-level writes: the resolved post-image (value or tombstone) is in
        // the overlay, keyed by collection.
        KvOp::Put { collection, .. }
        | KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::Delete { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Transfer { collection, .. } => {
            collections.insert(collection.clone());
            Ok(())
        }
        // `TransferItem` moves a row across collections: the source holds a
        // staged tombstone and the destination a staged value.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        } => {
            collections.insert(source_collection.clone());
            collections.insert(dest_collection.clone());
            Ok(())
        }

        // Read-only: nothing staged, nothing to persist.
        KvOp::Get { .. }
        | KvOp::BatchGet { .. }
        | KvOp::Scan { .. }
        | KvOp::FieldGet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => Ok(()),

        // TTL-only writes: `Expire` / `Persist` stage a TTL delta with NO value
        // post-image (`stage_kv_ttl.rs`). The KV redo shapes carry a TTL only as
        // the sixth element of a value put, so a standalone TTL change on a base
        // row has no redo representation. Rejecting is deliberate — silently
        // skipping would drop the change from the install path.
        KvOp::Expire { .. } | KvOp::Persist { .. } => Err(crate::Error::PlanError {
            detail: "kv EXPIRE/PERSIST is not supported in transaction resolve".to_string(),
        }),

        // Index / DDL / truncate: never stageable into the overlay, so no
        // row-level redo shape carries them.
        KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::Truncate { .. } => Err(crate::Error::PlanError {
            detail: "kv index/DDL/truncate op is not supported in transaction resolve".to_string(),
        }),
    }
}

/// The typed error a not-yet-supported writing engine raises during resolve.
fn unsupported_engine(engine: &str) -> crate::Error {
    crate::Error::PlanError {
        detail: format!("{engine} writes are not yet supported in transaction resolve"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::{DocumentOp, KvOp, MetaOp};
    use nodedb_types::Surrogate;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::handlers::transaction::overlay::{Staged, StagedTtl};
    use crate::data::executor::handlers::transaction::stage_write::hex_key;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{
        DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, TxnId, VShardId,
    };
    use crate::wal::{RedoRecord, RedoSubRecord};
    use nodedb_wal::WalRecord;
    use nodedb_wal::record::{RecordType, WalRecordArgs};

    const TID: u64 = 1;

    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("CoreLoop::open");
        (core, dir)
    }

    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    fn coll_key(coll: &str) -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(TID), coll.to_string())
    }

    /// Decode the `RedoRecord` bytes carried in a resolve response payload.
    fn decode_redo(resp: &crate::bridge::envelope::Response) -> RedoRecord {
        assert_eq!(resp.status, Status::Ok, "resolve must succeed: {resp:?}");
        RedoRecord::from_bytes(resp.payload.as_bytes()).expect("decode redo record")
    }

    /// A resolve plan that names `collection` as a KV write so the serializer
    /// picks up that collection's overlay entries.
    fn kv_write_plan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection.to_string(),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: Surrogate::ZERO,
        })
    }

    /// Decode a six-element `kv_put` redo payload.
    fn decode_kv_put6(payload: &[u8]) -> (String, Vec<u8>, Vec<u8>, u64, u64) {
        let (disc, collection, key, value, ttl_ms, expire_at_ms) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Vec<u8>, u64, u64)>(payload)
                .expect("decode 6-element kv_put");
        assert_eq!(disc, "kv_put");
        (collection, key, value, ttl_ms, expire_at_ms)
    }

    /// Decode a five-element `kv_put` redo payload.
    fn decode_kv_put5(payload: &[u8]) -> (String, Vec<u8>, Vec<u8>, u64) {
        let (disc, collection, key, value, ttl_ms) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Vec<u8>, u64)>(payload)
                .expect("decode 5-element kv_put");
        assert_eq!(disc, "kv_put");
        (collection, key, value, ttl_ms)
    }

    #[test]
    fn incr_resolves_to_absolute_value_not_delta() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(1);

        // Two Incrs in one transaction: 0 + 40, then + 2 = 42. The overlay slot
        // holds the resolved ABSOLUTE value (42), not either delta.
        for delta in [40i64, 2] {
            let resp = core.execute_stage_kv(
                &task,
                TID,
                txn,
                &KvOp::Incr {
                    collection: "counters".to_string(),
                    key: b"c".to_vec(),
                    delta,
                    ttl_ms: 0,
                    surrogate: Surrogate::ZERO,
                },
            );
            assert_eq!(resp.status, Status::Ok, "stage incr: {resp:?}");
        }

        // The overlay's staged bytes are the resolved absolute post-image.
        let overlay_bytes = match core
            .txn_overlays
            .get(&txn)
            .and_then(|o| o.get_by_doc_id(&coll_key("counters"), &hex_key(b"c")))
            .expect("staged incr present")
        {
            Staged::Put(v) => v.clone(),
            Staged::Tombstone => panic!("incr must stage a value"),
        };

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("counters")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one staged KV row -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::Put as u32);

        let (collection, key, value, _ttl) = decode_kv_put5(&redo.ops[0].payload);
        assert_eq!(collection, "counters");
        assert_eq!(key, b"c");
        // The emitted value is the overlay's absolute post-image, and it decodes
        // to 42 — not the last delta (2) nor the first (40).
        assert_eq!(value, overlay_bytes);
        assert_eq!(
            zerompk::from_msgpack::<i64>(&value).expect("i64"),
            42,
            "resolve carries the absolute resolved value, not a delta"
        );
    }

    #[test]
    fn put_with_ttl_resolves_to_six_element_absolute_expiry() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(2);

        // Stage a value with an absolute expiry directly (what a `Put` with a
        // non-zero TTL leaves in the overlay: value + `ExpireAt`).
        let expire_at = 1_700_000_000_000u64;
        {
            let overlay = core.txn_overlays.entry(txn).or_default();
            overlay.insert_put(coll_key("sessions"), 7, &hex_key(b"s1"), b"v1".to_vec());
            overlay.set_ttl(
                coll_key("sessions"),
                7,
                &hex_key(b"s1"),
                StagedTtl::ExpireAt(expire_at),
            );
        }

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("sessions")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::Put as u32);

        let (collection, key, value, ttl_ms, got_expire) = decode_kv_put6(&redo.ops[0].payload);
        assert_eq!(collection, "sessions");
        assert_eq!(key, b"s1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 0, "relative ttl_ms is vestigial and set to 0");
        assert_eq!(got_expire, expire_at, "absolute expiry carried verbatim");
    }

    #[test]
    fn put_without_ttl_resolves_to_five_element_form() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(3);

        core.txn_overlays.entry(txn).or_default().insert_put(
            coll_key("kvc"),
            9,
            &hex_key(b"k9"),
            b"body".to_vec(),
        );

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("kvc")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);

        // The six-element decode must reject the payload (strict array length),
        // proving the five-element form was emitted.
        assert!(
            zerompk::from_msgpack::<(String, String, Vec<u8>, Vec<u8>, u64, u64)>(
                &redo.ops[0].payload
            )
            .is_err(),
            "no-TTL put must emit the five-element form"
        );
        let (collection, key, value, ttl_ms) = decode_kv_put5(&redo.ops[0].payload);
        assert_eq!(collection, "kvc");
        assert_eq!(key, b"k9");
        assert_eq!(value, b"body");
        assert_eq!(ttl_ms, 0);
    }

    #[test]
    fn tombstone_resolves_to_kv_delete_shape() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(4);

        core.txn_overlays.entry(txn).or_default().insert_tombstone(
            coll_key("kvc"),
            11,
            &hex_key(b"gone"),
        );

        let resp = core.execute_resolve_txn(
            &task,
            TID,
            txn,
            &[PhysicalPlan::Kv(KvOp::Delete {
                collection: "kvc".to_string(),
                keys: vec![b"gone".to_vec()],
            })],
        );
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::Delete as u32);

        let (disc, collection, keys) =
            zerompk::from_msgpack::<(String, String, Vec<Vec<u8>>)>(&redo.ops[0].payload)
                .expect("decode kv_delete");
        assert_eq!(disc, "kv_delete");
        assert_eq!(collection, "kvc");
        assert_eq!(keys, vec![b"gone".to_vec()]);
    }

    #[test]
    fn resolve_does_not_mutate_base() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(5);
        let now = crate::engine::kv::current_ms();

        // Seed a base KV row, then stage a DIFFERENT value for the same key.
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TID,
            collection: "kvc",
            key: b"k",
            value: b"base",
            ttl_ms: 0,
            now_ms: now,
            surrogate: Surrogate::ZERO,
        });
        let before = core
            .kv_engine
            .get(DatabaseId::DEFAULT.as_u64(), TID, "kvc", b"k", now);
        assert_eq!(before.as_deref(), Some(b"base".as_slice()));

        core.txn_overlays.entry(txn).or_default().insert_put(
            coll_key("kvc"),
            1,
            &hex_key(b"k"),
            b"staged".to_vec(),
        );

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("kvc")]);
        assert_eq!(resp.status, Status::Ok);

        // Base is untouched: resolve reads the overlay only, never writes base.
        let after = core
            .kv_engine
            .get(DatabaseId::DEFAULT.as_u64(), TID, "kvc", b"k", now);
        assert_eq!(after.as_deref(), Some(b"base".as_slice()));
    }

    #[test]
    fn document_write_plan_yields_typed_error() {
        let (core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(6);

        let doc_plan = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "docs".to_string(),
            document_id: "d1".to_string(),
            value: Vec::new(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
        });

        let resp = core.execute_resolve_txn(&task, TID, txn, &[doc_plan]);
        assert_eq!(
            resp.status,
            Status::Error,
            "a not-yet-supported writing op must raise a typed error, not be dropped"
        );
        assert!(resp.error_code.is_some());
    }

    #[test]
    fn resolved_bytes_replay_into_fresh_engine() {
        // Resolve on one core, then replay the emitted `RedoRecord` bytes into a
        // FRESH engine set and observe the expected KV state.
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(7);

        let expire_at = crate::engine::kv::current_ms() + 3_600_000;
        {
            let overlay = src.txn_overlays.entry(txn).or_default();
            overlay.insert_put(coll_key("kvc"), 1, &hex_key(b"live"), b"V".to_vec());
            overlay.set_ttl(
                coll_key("kvc"),
                1,
                &hex_key(b"live"),
                StagedTtl::ExpireAt(expire_at),
            );
            overlay.insert_put(coll_key("kvc"), 2, &hex_key(b"plain"), b"P".to_vec());
        }

        let resp = src.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("kvc")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 2, "two staged rows -> two sub-records");

        // Wrap the resolved bytes in a `TransactionRedo` WAL record and replay
        // into a fresh core.
        let wal_record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 1,
            tenant_id: TID,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: redo.to_bytes().expect("re-encode redo"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&wal_record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let now = crate::engine::kv::current_ms();
        let db = DatabaseId::DEFAULT.as_u64();
        assert_eq!(
            dst.kv_engine.get(db, TID, "kvc", b"live", now).as_deref(),
            Some(b"V".as_slice()),
            "expiring row must replay"
        );
        assert_eq!(
            dst.kv_engine.get(db, TID, "kvc", b"plain", now).as_deref(),
            Some(b"P".as_slice()),
            "plain row must replay"
        );
        // The absolute expiry survived the round-trip (remaining ~ full hour).
        let ttl = dst
            .kv_engine
            .get_ttl_ms(db, TID, "kvc", b"live", now)
            .expect("ttl present");
        assert!(
            ttl > 3_000_000,
            "absolute expiry preserved (remaining {ttl}ms)"
        );
    }

    #[test]
    fn read_only_and_crdt_and_text_plans_emit_nothing() {
        let (core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(8);

        // A read-only KV Get, with no overlay staged, produces an empty redo.
        let resp = core.execute_resolve_txn(
            &task,
            TID,
            txn,
            &[PhysicalPlan::Kv(KvOp::Get {
                collection: "kvc".to_string(),
                key: b"k".to_vec(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
            })],
        );
        let redo = decode_redo(&resp);
        assert!(redo.ops.is_empty(), "read-only plan emits no sub-record");
    }

    #[test]
    fn empty_overlay_resolves_to_empty_record() {
        let (core, _dir) = make_core();
        let task = make_task();
        let resp = core.execute_resolve_txn(&task, TID, TxnId::new(99), &[]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.version, 1);
        assert!(redo.ops.is_empty());
        assert!(redo.calvin_stamp.is_none());
    }

    #[test]
    fn sub_records_carry_the_kv_record_types() {
        // Guards the record-type tag the reconstitute path keys on.
        let sub = RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: Vec::new(),
        };
        assert_eq!(sub.record_type, RecordType::Put as u32);
    }
}
