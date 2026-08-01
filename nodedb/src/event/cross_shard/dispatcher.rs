// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard event dispatcher: reliable outbound delivery via QUIC.
//!
//! Maintains a per-target-node bounded send queue. A background Tokio task
//! drains queues, sends via `NexarTransport`, handles retries and DLQ.
//! FIFO per source partition: events from the same source vShard are sent
//! in LSN order.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType};
use nodedb_cluster::{NexarTransport, RaftRpc};

use super::dlq::{CrossShardDlq, DlqEnqueueParams};
use super::metrics::CrossShardMetrics;
use super::retry::{CrossShardRetryQueue, RetryEntry};
use super::types::{CrossShardWriteRequest, CrossShardWriteResponse};

/// Maximum pending writes per target node.
const MAX_QUEUE_PER_TARGET: usize = 10_000;

/// How often the dispatcher drains queues (milliseconds).
const DRAIN_INTERVAL: Duration = Duration::from_millis(50);

/// Maximum writes to send per drain cycle per target.
const BATCH_SIZE: usize = 64;

/// Backpressure threshold: 85% of max queue → warn.
const THROTTLE_THRESHOLD: f64 = 0.85;

/// Backpressure threshold: 95% of max queue → reject new writes.
const SUSPEND_THRESHOLD: f64 = 0.95;

/// Per-target send queue.
struct TargetQueue {
    entries: VecDeque<QueuedWrite>,
}

impl TargetQueue {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn utilization(&self) -> f64 {
        self.len() as f64 / MAX_QUEUE_PER_TARGET as f64
    }
}

/// A write queued for delivery to a specific target node.
struct QueuedWrite {
    request: CrossShardWriteRequest,
    enqueued_at: Instant,
}

/// Outbound cross-shard event dispatcher.
///
/// Thread-safe: `enqueue()` is called from Event Plane consumer tasks,
/// the background drain task runs on its own Tokio task.
pub struct CrossShardDispatcher {
    /// Per-target-node bounded queues.
    queues: Mutex<HashMap<u64, TargetQueue>>,
    /// This node's ID (for source_node in VShardEnvelope).
    node_id: u64,
    /// Shared metrics.
    metrics: Arc<CrossShardMetrics>,
}

impl CrossShardDispatcher {
    pub fn new(node_id: u64, metrics: Arc<CrossShardMetrics>) -> Self {
        Self {
            queues: Mutex::new(HashMap::new()),
            node_id,
            metrics,
        }
    }

    /// Enqueue a cross-shard write for delivery to a target node.
    ///
    /// Returns `false` if the target queue is full (backpressure SUSPEND).
    pub fn enqueue(&self, target_node: u64, request: CrossShardWriteRequest) -> bool {
        let mut queues = self.queues.lock().unwrap_or_else(|p| p.into_inner());
        let queue = queues.entry(target_node).or_insert_with(TargetQueue::new);

        let util = queue.utilization();
        if util >= SUSPEND_THRESHOLD {
            warn!(
                target_node,
                queue_len = queue.len(),
                "cross-shard queue SUSPENDED (95%), rejecting write"
            );
            return false;
        }

        if util >= THROTTLE_THRESHOLD {
            debug!(
                target_node,
                queue_len = queue.len(),
                "cross-shard queue THROTTLED (85%)"
            );
        }

        queue.entries.push_back(QueuedWrite {
            request,
            enqueued_at: Instant::now(),
        });

        self.metrics.record_sent();
        true
    }

    /// Total pending writes across all target queues.
    pub fn total_pending(&self) -> usize {
        let queues = self.queues.lock().unwrap_or_else(|p| p.into_inner());
        queues.values().map(|q| q.len()).sum()
    }

    /// Drain a batch of writes for a target node (called by background task).
    fn drain_batch(&self, target_node: u64, limit: usize) -> Vec<QueuedWrite> {
        let mut queues = self.queues.lock().unwrap_or_else(|p| p.into_inner());
        let Some(queue) = queues.get_mut(&target_node) else {
            return Vec::new();
        };

        let count = queue.entries.len().min(limit);
        queue.entries.drain(..count).collect()
    }

    /// Get all target nodes that have pending writes.
    pub(crate) fn active_targets(&self) -> Vec<u64> {
        let queues = self.queues.lock().unwrap_or_else(|p| p.into_inner());
        queues
            .iter()
            .filter(|(_, q)| !q.entries.is_empty())
            .map(|(&node_id, _)| node_id)
            .collect()
    }

    /// Test-only: snapshot every pending request queued for a target node, in
    /// FIFO order, without draining the queue. Lets origination tests assert
    /// on the exact `CrossShardWriteRequest` fields an `enqueue()` produced.
    #[cfg(test)]
    pub(crate) fn peek_pending(&self, target_node: u64) -> Vec<CrossShardWriteRequest> {
        let queues = self.queues.lock().unwrap_or_else(|p| p.into_inner());
        queues
            .get(&target_node)
            .map(|q| q.entries.iter().map(|w| w.request.clone()).collect())
            .unwrap_or_default()
    }
}

/// Send a single cross-shard write via QUIC and return the response.
async fn send_write(
    transport: &NexarTransport,
    source_node: u64,
    target_node: u64,
    request: &CrossShardWriteRequest,
) -> crate::Result<CrossShardWriteResponse> {
    let payload = zerompk::to_msgpack_vec(request).map_err(|e| crate::Error::Dispatch {
        detail: format!("serialize request: {e}"),
    })?;

    let envelope = VShardEnvelope::new(
        VShardMessageType::CrossShardEvent,
        source_node,
        target_node,
        request.target_vshard,
        payload,
    );

    let rpc = RaftRpc::VShardEnvelope(envelope.to_bytes());
    let response_rpc =
        transport
            .send_rpc(target_node, rpc)
            .await
            .map_err(|e| crate::Error::Dispatch {
                detail: format!("transport: {e}"),
            })?;

    let RaftRpc::VShardEnvelope(response_bytes) = response_rpc else {
        return Err(crate::Error::Dispatch {
            detail: "unexpected RPC response type".to_string(),
        });
    };

    let response_env =
        VShardEnvelope::from_bytes(&response_bytes).ok_or_else(|| crate::Error::Dispatch {
            detail: "malformed VShardEnvelope response".to_string(),
        })?;

    zerompk::from_msgpack::<CrossShardWriteResponse>(&response_env.payload).map_err(|e| {
        crate::Error::Dispatch {
            detail: format!("deserialize response: {e}"),
        }
    })
}

/// Spawn the background dispatcher task.
///
/// Drains queues, sends writes via QUIC, retries on failure, DLQs on exhaust.
/// Updates the Event Plane budget with the pending write count each cycle.
pub fn spawn_dispatcher_task(
    dispatcher: Arc<CrossShardDispatcher>,
    transport: Arc<NexarTransport>,
    metrics: Arc<CrossShardMetrics>,
    dlq: Arc<Mutex<CrossShardDlq>>,
    budget: Arc<crate::event::budget::EventPlaneBudget>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        debug!("cross-shard dispatcher task started");
        let mut retry_queue = CrossShardRetryQueue::new();

        loop {
            tokio::select! {
                _ = tokio::time::sleep(DRAIN_INTERVAL) => {
                    // Update budget with pending write count.
                    let pending = dispatcher.total_pending() as u64 + retry_queue.len() as u64;
                    budget.update_pending_cross_shard(pending);

                    // Process retries first.
                    process_retries(
                        &retry_queue,
                        &transport,
                        &metrics,
                        &dlq,
                        dispatcher.node_id,
                    ).await;
                    drain_retry_queue(&mut retry_queue, &transport, &metrics, &dlq, dispatcher.node_id).await;

                    // Drain primary queues for all active targets.
                    let targets = dispatcher.active_targets();
                    for target_node in targets {
                        let batch = dispatcher.drain_batch(target_node, BATCH_SIZE);
                        for write in batch {
                            let start = Instant::now();
                            match send_write(
                                &transport,
                                dispatcher.node_id,
                                target_node,
                                &write.request,
                            ).await {
                                Ok(resp) if resp.success => {
                                    let latency_us = start.elapsed().as_micros() as u64;
                                    metrics.record_delivered(latency_us);
                                    if resp.duplicate {
                                        trace!(
                                            source_lsn = resp.source_lsn,
                                            "cross-shard write was duplicate (HWM dedup)"
                                        );
                                    }
                                }
                                Ok(resp) => {
                                    // Execution error on target — retry.
                                    metrics.record_failure();
                                    retry_queue.enqueue(RetryEntry {
                                        request: write.request,
                                        target_node,
                                        attempts: 0,
                                        last_error: resp.error,
                                        next_retry_at: Instant::now(),
                                        enqueued_at: write.enqueued_at,
                                    });
                                }
                                Err(e) => {
                                    // Transport error — retry.
                                    metrics.record_failure();
                                    retry_queue.enqueue(RetryEntry {
                                        request: write.request,
                                        target_node,
                                        attempts: 0,
                                        last_error: e.to_string(),
                                        next_retry_at: Instant::now(),
                                        enqueued_at: write.enqueued_at,
                                    });
                                }
                            }
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        // Final drain attempt — best effort.
                        info!("cross-shard dispatcher shutting down");
                        return;
                    }
                }
            }
        }
    })
}

/// Process retry queue: drain due entries, retry, DLQ exhausted.
async fn drain_retry_queue(
    retry_queue: &mut CrossShardRetryQueue,
    transport: &NexarTransport,
    metrics: &CrossShardMetrics,
    dlq: &Mutex<CrossShardDlq>,
    source_node: u64,
) {
    let (ready, exhausted) = retry_queue.drain_due();

    // DLQ exhausted entries.
    if !exhausted.is_empty() {
        let mut dlq_guard = dlq.lock().unwrap_or_else(|p| p.into_inner());
        for entry in &exhausted {
            metrics.record_dlq();
            let _ = dlq_guard.enqueue(DlqEnqueueParams {
                tenant_id: entry.request.tenant_id,
                source_collection: entry.request.source_collection.clone(),
                sql: entry.request.sql.clone(),
                source_vshard: entry.request.source_vshard,
                target_vshard: entry.request.target_vshard,
                target_node: entry.target_node,
                source_lsn: entry.request.source_lsn,
                source_sequence: entry.request.source_sequence,
                error: entry.last_error.clone(),
                retry_count: entry.attempts,
            });
        }
    }

    // Retry ready entries.
    for mut entry in ready {
        metrics.record_retry();
        match send_write(transport, source_node, entry.target_node, &entry.request).await {
            Ok(resp) if resp.success => {
                let latency_us = entry.enqueued_at.elapsed().as_micros() as u64;
                metrics.record_delivered(latency_us);
            }
            Ok(resp) => {
                entry.last_error = resp.error;
                retry_queue.enqueue(entry);
            }
            Err(e) => {
                entry.last_error = e.to_string();
                retry_queue.enqueue(entry);
            }
        }
    }
}

/// Process retries is a no-op placeholder — actual work is in drain_retry_queue.
/// Split to avoid holding mutable ref to retry_queue across await points.
async fn process_retries(
    _retry_queue: &CrossShardRetryQueue,
    _transport: &NexarTransport,
    _metrics: &CrossShardMetrics,
    _dlq: &Mutex<CrossShardDlq>,
    _source_node: u64,
) {
    // Retry processing is done in drain_retry_queue which takes &mut.
    // This function exists to clarify the separation in the main loop.
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(lsn: u64) -> CrossShardWriteRequest {
        CrossShardWriteRequest {
            sql: "INSERT INTO audit VALUES (1)".into(),
            tenant_id: 1,
            database_id: 0,
            source_vshard: 3,
            source_lsn: lsn,
            source_sequence: lsn,
            cascade_depth: 0,
            source_collection: "orders".into(),
            target_vshard: 7,
        }
    }

    #[test]
    fn enqueue_and_drain() {
        let metrics = Arc::new(CrossShardMetrics::new());
        let dispatcher = CrossShardDispatcher::new(1, Arc::clone(&metrics));

        assert!(dispatcher.enqueue(2, make_request(100)));
        assert!(dispatcher.enqueue(2, make_request(200)));
        assert!(dispatcher.enqueue(3, make_request(300)));

        let batch = dispatcher.drain_batch(2, 10);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].request.source_lsn, 100);
        assert_eq!(batch[1].request.source_lsn, 200);

        let batch = dispatcher.drain_batch(3, 10);
        assert_eq!(batch.len(), 1);

        assert_eq!(
            metrics
                .writes_sent
                .load(std::sync::atomic::Ordering::Relaxed),
            3
        );
    }

    #[test]
    fn active_targets() {
        let metrics = Arc::new(CrossShardMetrics::new());
        let dispatcher = CrossShardDispatcher::new(1, Arc::clone(&metrics));

        dispatcher.enqueue(2, make_request(100));
        dispatcher.enqueue(3, make_request(200));

        let mut targets = dispatcher.active_targets();
        targets.sort();
        assert_eq!(targets, vec![2, 3]);
    }

    #[test]
    fn fifo_ordering() {
        let metrics = Arc::new(CrossShardMetrics::new());
        let dispatcher = CrossShardDispatcher::new(1, Arc::clone(&metrics));

        for lsn in [300, 100, 200] {
            dispatcher.enqueue(2, make_request(lsn));
        }

        let batch = dispatcher.drain_batch(2, 10);
        let lsns: Vec<u64> = batch.iter().map(|w| w.request.source_lsn).collect();
        assert_eq!(lsns, vec![300, 100, 200]); // FIFO, not sorted.
    }

    #[test]
    fn drain_batch_limit() {
        let metrics = Arc::new(CrossShardMetrics::new());
        let dispatcher = CrossShardDispatcher::new(1, Arc::clone(&metrics));

        for lsn in 1..=10 {
            dispatcher.enqueue(2, make_request(lsn * 100));
        }

        let batch = dispatcher.drain_batch(2, 3);
        assert_eq!(batch.len(), 3);
        // Remaining in queue.
        let remaining = dispatcher.drain_batch(2, 100);
        assert_eq!(remaining.len(), 7);
    }
}
