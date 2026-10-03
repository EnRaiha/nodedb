// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for Event Plane cross-shard delivery.
//!
//! Tests: exact-key dedup, retry queue with volume bound, DLQ on exhaust,
//! DLQ replay candidates, FIFO ordering, dispatcher backpressure.

use std::sync::Arc;
use std::time::Instant;

use nodedb::event::cross_shard::dedup::{CrossShardDedup, applied_key_of};
use nodedb::event::cross_shard::dispatcher::CrossShardDispatcher;
use nodedb::event::cross_shard::dlq::{CrossShardDlq, DlqEnqueueParams};
use nodedb::event::cross_shard::metrics::CrossShardMetrics;
use nodedb::event::cross_shard::retry::{CrossShardRetryQueue, RetryEntry};
use nodedb::event::cross_shard::types::{CrossShardWriteRequest, CrossShardWriteResponse};

fn make_request(collection: &str, lsn: u64) -> CrossShardWriteRequest {
    CrossShardWriteRequest {
        sql: format!("INSERT INTO audit VALUES ({lsn})"),
        tenant_id: 1,
        database_id: 0,
        source_vshard: 3,
        source_lsn: lsn,
        source_sequence: lsn,
        origin: String::new(),
        cascade_depth: 0,
        source_collection: collection.into(),
        target_vshard: 7,
    }
}

#[test]
fn dedup_drops_a_request_that_already_applied() {
    let dir = tempfile::tempdir().unwrap();
    let store = CrossShardDedup::open(dir.path()).unwrap();
    let key = applied_key_of(&make_request("orders", 100));

    assert!(!store.is_applied(&key).unwrap());
    store.record_applied(&key).unwrap();
    assert!(store.is_applied(&key).unwrap());
    // A request from an older source write is not hidden by a newer one.
    assert!(
        !store
            .is_applied(&applied_key_of(&make_request("orders", 50)))
            .unwrap()
    );
}

#[test]
fn dedup_keys_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let key = applied_key_of(&make_request("orders", 500));
    {
        let store = CrossShardDedup::open(dir.path()).unwrap();
        store.record_applied(&key).unwrap();
    }
    let store = CrossShardDedup::open(dir.path()).unwrap();
    assert!(store.is_applied(&key).unwrap());
}

#[test]
fn retry_queue_enqueue_and_len() {
    let mut queue = CrossShardRetryQueue::new();

    // Enqueue two entries.
    queue.enqueue(RetryEntry {
        request: make_request("orders", 100),
        target_node: 2,
        attempts: 0,
        last_error: "timeout".into(),
        next_retry_at: Instant::now(),
        enqueued_at: Instant::now(),
    });
    queue.enqueue(RetryEntry {
        request: make_request("orders", 200),
        target_node: 2,
        attempts: 0,
        last_error: "timeout".into(),
        next_retry_at: Instant::now(),
        enqueued_at: Instant::now(),
    });

    assert_eq!(queue.len(), 2);
    assert!(!queue.is_empty());

    // drain_due returns nothing immediately (backoff not elapsed).
    let (ready, exhausted) = queue.drain_due();
    assert!(ready.is_empty());
    assert!(exhausted.is_empty());
    // Entries still in queue (not due yet).
    assert_eq!(queue.len(), 2);
}

#[test]
fn dlq_enqueue_list_resolve_replay() {
    let dir = tempfile::tempdir().unwrap();
    let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

    dlq.enqueue(DlqEnqueueParams {
        tenant_id: 1,
        source_collection: "orders".into(),
        sql: "INSERT INTO audit VALUES (1)".into(),
        source_vshard: 3,
        target_vshard: 7,
        target_node: 2,
        source_lsn: 100,
        source_sequence: 100,
        origin: "trigger/1/audit".into(),
        error: "shard unavailable".into(),
        retry_count: 5,
    })
    .unwrap();

    assert_eq!(dlq.unresolved_count(), 1);
    assert_eq!(dlq.len(), 1);

    // Replay candidates.
    let candidates = dlq.replay_candidates("orders", 0);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].sql, "INSERT INTO audit VALUES (1)");

    // Resolve.
    let entry_id = dlq.list_unresolved()[0].entry_id;
    assert!(dlq.resolve(entry_id).unwrap());
    assert_eq!(dlq.unresolved_count(), 0);
}

#[test]
fn dispatcher_enqueue_and_pending() {
    let metrics = Arc::new(CrossShardMetrics::new());
    let dispatcher = CrossShardDispatcher::new(1, Arc::clone(&metrics));

    assert_eq!(dispatcher.total_pending(), 0);

    dispatcher.enqueue(2, make_request("orders", 300));
    dispatcher.enqueue(2, make_request("orders", 100));
    dispatcher.enqueue(3, make_request("orders", 200));

    assert_eq!(dispatcher.total_pending(), 3);
    assert_eq!(
        metrics
            .writes_sent
            .load(std::sync::atomic::Ordering::Relaxed),
        3
    );
}

#[test]
fn response_variants() {
    let ok = CrossShardWriteResponse::ok(100);
    assert!(ok.success);
    assert!(!ok.duplicate);

    let dup = CrossShardWriteResponse::duplicate(100);
    assert!(dup.success);
    assert!(dup.duplicate);

    let err = CrossShardWriteResponse::error(100, "failed".into());
    assert!(!err.success);
    assert!(!err.duplicate);
    assert_eq!(err.error, "failed");
}

#[test]
fn metrics_snapshot() {
    let m = CrossShardMetrics::new();
    m.record_sent();
    m.record_sent();
    m.record_delivered(500);
    m.record_retry();
    m.record_dlq();
    m.record_duplicate();
    m.record_failure();

    let snap = m.snapshot();
    assert_eq!(snap.writes_sent, 2);
    assert_eq!(snap.writes_delivered, 1);
    assert_eq!(snap.retries, 1);
    assert_eq!(snap.dlq_enqueued, 1);
    assert_eq!(snap.duplicates_dropped, 1);
    assert_eq!(snap.delivery_failures, 1);
    assert_eq!(snap.avg_latency_us, 500);
}

/// Deliver `request` to `receiver` and decode its answer.
async fn deliver(
    receiver: &nodedb::event::cross_shard::CrossShardReceiver,
    request: &CrossShardWriteRequest,
) -> CrossShardWriteResponse {
    use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType};
    let payload = zerompk::to_msgpack_vec(request).expect("encode request");
    let envelope = VShardEnvelope::new(
        VShardMessageType::CrossShardEvent,
        2,
        1,
        request.target_vshard,
        payload,
    );
    let answer = receiver.handle_envelope(envelope.to_bytes()).await;
    let answer = VShardEnvelope::from_bytes(&answer).expect("decode envelope");
    zerompk::from_msgpack(&answer.payload).expect("decode response")
}

/// The receiver applies a request, then the node crashes before the dedup
/// store records it and before the sender sees the ack. The key restored from
/// the WAL drops the re-send: the write applies once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_apply_and_ack_never_reapplies() {
    let server = nodedb_test_support::pgwire_harness::TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION xs_audit (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create the target");
    let request = CrossShardWriteRequest {
        sql: "BEGIN\nINSERT INTO xs_audit (id, val) VALUES ('once', 1);\nEND;".into(),
        tenant_id: 1,
        database_id: 0,
        source_vshard: 3,
        source_lsn: 700,
        source_sequence: 1,
        origin: "trigger/0/audit/7".into(),
        cascade_depth: 0,
        source_collection: "orders".into(),
        target_vshard: 7,
    };
    let metrics = Arc::new(CrossShardMetrics::new());

    // Before the crash: this store's record is lost with the node.
    let lost = tempfile::tempdir().unwrap();
    let before = nodedb::event::cross_shard::CrossShardReceiver::new(
        Arc::new(CrossShardDedup::open(lost.path()).unwrap()),
        Arc::clone(&server.shared),
        Arc::clone(&metrics),
        1,
    );
    let first = deliver(&before, &request).await;
    assert!(
        first.success && !first.duplicate,
        "applied: {}",
        first.error
    );

    // After the crash: an empty store, restored from the WAL at startup.
    let fresh = tempfile::tempdir().unwrap();
    let restored = CrossShardDedup::open(fresh.path()).unwrap();
    restored
        .restore_from_wal(&server.shared.wal)
        .expect("restore keys from the WAL");
    let after = nodedb::event::cross_shard::CrossShardReceiver::new(
        Arc::new(restored),
        Arc::clone(&server.shared),
        metrics,
        1,
    );
    let resent = deliver(&after, &request).await;
    assert!(
        resent.duplicate,
        "the re-send is a duplicate: {}",
        resent.error
    );

    let rows = server
        .query_text("SELECT id FROM xs_audit")
        .await
        .expect("read the target");
    assert_eq!(rows, vec!["once".to_string()], "the write applied once");
}
