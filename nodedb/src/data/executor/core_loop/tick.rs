// SPDX-License-Identifier: BUSL-1.1

use tracing::warn;

use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::io::io_metrics::{TIER_CRITICAL, TIER_HIGH, TIER_LOW};

use super::super::task::{ExecutionTask, TaskState};
use super::CoreLoop;

impl CoreLoop {
    /// Drain incoming requests from the SPSC bridge into the priority queues.
    ///
    /// The number of requests drained per call is bounded by `spsc_read_depth`,
    /// which is reduced under Critical memory pressure and set to zero under
    /// Emergency. Each request is routed to the Critical, High, or Low tier
    /// based on `Request.priority`.
    pub fn drain_requests(&mut self) {
        if self.pressure_suspend_reads {
            // Emergency pressure: do not accept new requests.
            return;
        }
        let depth = self.spsc_read_depth.max(1);
        let mut batch = Vec::new();
        self.request_rx.drain_into(&mut batch, depth);
        for br in batch {
            self.task_queue.push(ExecutionTask::new(br.inner));
        }
    }

    /// Process the next pending task using the 8:4:2 priority drain ratio.
    ///
    /// Advances `self.drain_cycle` by one slot and returns `true` if a task
    /// was processed.
    pub fn poll_one(&mut self) -> bool {
        let Some(qt) = self.task_queue.pop_next(&mut self.drain_cycle) else {
            return false;
        };

        // Record IO wait from enqueue to execution start.
        let wait_ns = qt.enqueued_at.elapsed().as_nanos() as u64;
        use crate::bridge::envelope::Priority;
        let tier = match qt.task.request.priority {
            Priority::Background | Priority::Normal => TIER_LOW,
            Priority::High => TIER_HIGH,
            Priority::Critical => TIER_CRITICAL,
        };
        self.io_metrics.record_wait(tier, wait_ns);

        let mut task = qt.task;

        if let Some(key) = task.request.idempotency_key
            && let Some(&succeeded) = self.idempotency_cache.get(&key)
        {
            let response = if succeeded {
                self.response_ok(&task)
            } else {
                self.response_error(&task, ErrorCode::DuplicateWrite)
            };
            if let Err(e) = self
                .response_tx
                .try_push(BridgeResponse { inner: response })
            {
                warn!(core = self.core_id, error = %e, "failed to send idempotent response");
            }
            return true;
        }

        let response = if task.is_expired() {
            task.state = TaskState::Failed;
            Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: Payload::empty(),
                watermark_lsn: self.watermark,
                error_code: Some(Box::new(ErrorCode::DeadlineExceeded)),
                read_set_valid: None,
                read_version_lsn: crate::types::Lsn::ZERO,
                write_set: Vec::new(),
            }
        } else {
            task.state = TaskState::Running;
            let resp = self.execute(&task);
            task.state = TaskState::Completed;
            resp
        };

        if let Some(key) = task.request.idempotency_key {
            let succeeded = response.status == Status::Ok;
            if self.idempotency_cache.len() >= 16_384
                && let Some(oldest_key) = self.idempotency_order.pop_front()
            {
                self.idempotency_cache.remove(&oldest_key);
            }
            self.idempotency_cache.insert(key, succeeded);
            self.idempotency_order.push_back(key);
        }

        // Bound the dangling-edge tracker across all tenants. Count
        // all entries; when the aggregate exceeds the cap, drop the
        // whole map — callers are tolerant of false negatives (an
        // `EdgePut` to a recently-deleted node races the tracker, so
        // the semantics are advisory regardless).
        let total: usize = self.deleted_nodes.values().map(|s| s.len()).sum();
        if total > 100_000 {
            self.deleted_nodes.clear();
        }

        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core = self.core_id, error = %e, "failed to send response — response queue full");
        }

        true
    }

    /// Run one iteration of the event loop: drain requests, process tasks.
    ///
    /// After processing, update the per-priority queue-depth gauges so the
    /// Prometheus endpoint reflects the post-tick state.
    pub fn tick(&mut self) -> usize {
        self.poll_build_completions();
        self.poll_pending_reindex();
        // Adjust SPSC read depth based on current memory pressure.
        self.apply_spsc_pressure();
        self.drain_requests();
        let mut processed = 0;
        while !self.task_queue.is_empty() {
            let batched = self.poll_write_batch();
            if batched > 0 {
                processed += batched;
                continue;
            }
            if self.poll_one() {
                processed += 1;
            } else {
                break;
            }
        }

        // Update queue-depth gauges after draining.
        self.io_metrics
            .record_queue_depth(TIER_CRITICAL, self.task_queue.critical_len() as u64);
        self.io_metrics
            .record_queue_depth(TIER_HIGH, self.task_queue.high_len() as u64);
        self.io_metrics
            .record_queue_depth(TIER_LOW, self.task_queue.low_len() as u64);

        processed
    }
}
