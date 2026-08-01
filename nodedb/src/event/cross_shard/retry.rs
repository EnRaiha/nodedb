// SPDX-License-Identifier: BUSL-1.1

//! Volume-bounded retry queue for cross-shard writes.
//!
//! Exponential backoff (100ms base, 2x, capped at 10s).
//! Volume bound: max 1000 retries/sec per source collection to prevent
//! retry amplification from saturating the Event Plane when a target
//! shard is persistently down.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::types::CrossShardWriteRequest;

/// Default maximum retries before DLQ.
const DEFAULT_MAX_RETRIES: u32 = 5;

/// Base backoff delay.
const BASE_BACKOFF: Duration = Duration::from_millis(100);

/// Maximum backoff delay.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Maximum retries per second per source collection.
const MAX_RETRIES_PER_SEC_PER_COLLECTION: u32 = 1000;

/// A cross-shard write pending retry.
#[derive(Debug, Clone)]
pub struct RetryEntry {
    /// The original write request.
    pub request: CrossShardWriteRequest,
    /// Target node for delivery.
    pub target_node: u64,
    /// Number of delivery attempts so far.
    pub attempts: u32,
    /// Last error from the most recent attempt.
    pub last_error: String,
    /// When this entry becomes eligible for retry.
    pub next_retry_at: Instant,
    /// When this entry was first enqueued.
    pub enqueued_at: Instant,
}

/// Volume-bounded retry queue for cross-shard writes.
pub struct CrossShardRetryQueue {
    queue: VecDeque<RetryEntry>,
    max_retries: u32,
    /// Per-collection retry counts in the current 1-second window.
    volume_counts: HashMap<String, u32>,
    /// Start of the current 1-second volume window.
    volume_window_start: Instant,
}

impl CrossShardRetryQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            max_retries: DEFAULT_MAX_RETRIES,
            volume_counts: HashMap::new(),
            volume_window_start: Instant::now(),
        }
    }

    /// Enqueue a write for retry. Automatically computes backoff delay.
    pub fn enqueue(&mut self, mut entry: RetryEntry) {
        entry.attempts += 1;
        entry.next_retry_at = Instant::now() + compute_backoff(entry.attempts);
        self.queue.push_back(entry);
    }

    /// Drain entries that are due for retry, respecting volume bounds.
    ///
    /// Returns `(ready_for_retry, exhausted_for_dlq)`.
    /// - `ready`: entries within retry budget, eligible for another attempt.
    /// - `exhausted`: entries that exceeded max_retries, should go to DLQ.
    pub fn drain_due(&mut self) -> (Vec<RetryEntry>, Vec<RetryEntry>) {
        let now = Instant::now();

        // Reset volume window if >1s has elapsed.
        if now.duration_since(self.volume_window_start) >= Duration::from_secs(1) {
            self.volume_counts.clear();
            self.volume_window_start = now;
        }

        let mut ready = Vec::new();
        let mut exhausted = Vec::new();
        let mut not_due = VecDeque::new();

        while let Some(entry) = self.queue.pop_front() {
            if entry.next_retry_at > now {
                not_due.push_back(entry);
                continue;
            }

            if entry.attempts >= self.max_retries {
                exhausted.push(entry);
                continue;
            }

            // Volume bounding: check per-collection rate.
            let count = self
                .volume_counts
                .entry(entry.request.source_collection.clone())
                .or_insert(0);

            if *count >= MAX_RETRIES_PER_SEC_PER_COLLECTION {
                // Volume exceeded — send directly to DLQ instead of retrying.
                exhausted.push(entry);
                continue;
            }

            *count += 1;
            ready.push(entry);
        }

        self.queue = not_due;
        (ready, exhausted)
    }

    /// Number of entries pending retry.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl Default for CrossShardRetryQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute exponential backoff delay for the given attempt number.
fn compute_backoff(attempt: u32) -> Duration {
    let delay = BASE_BACKOFF
        .checked_mul(
            1u32.checked_shl(attempt.saturating_sub(1))
                .unwrap_or(u32::MAX),
        )
        .unwrap_or(MAX_BACKOFF);
    delay.min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(collection: &str, lsn: u64) -> CrossShardWriteRequest {
        CrossShardWriteRequest {
            sql: "INSERT INTO audit VALUES (1)".into(),
            tenant_id: 1,
            database_id: 0,
            source_vshard: 3,
            source_lsn: lsn,
            source_sequence: lsn,
            cascade_depth: 0,
            source_collection: collection.into(),
            target_vshard: 7,
        }
    }

    fn make_entry(collection: &str, lsn: u64, attempts: u32) -> RetryEntry {
        RetryEntry {
            request: make_request(collection, lsn),
            target_node: 2,
            attempts,
            last_error: "timeout".into(),
            next_retry_at: Instant::now(), // Due now.
            enqueued_at: Instant::now(),
        }
    }

    #[test]
    fn backoff_progression() {
        assert_eq!(compute_backoff(1), Duration::from_millis(100));
        assert_eq!(compute_backoff(2), Duration::from_millis(200));
        assert_eq!(compute_backoff(3), Duration::from_millis(400));
        assert_eq!(compute_backoff(4), Duration::from_millis(800));
        assert_eq!(compute_backoff(5), Duration::from_millis(1600));
        // Capped at 10s.
        assert_eq!(compute_backoff(20), Duration::from_secs(10));
    }

    #[test]
    fn enqueue_increments_attempts() {
        let mut q = CrossShardRetryQueue::new();
        let entry = make_entry("orders", 100, 0);
        q.enqueue(entry);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn drain_due_separates_ready_and_exhausted() {
        let mut q = CrossShardRetryQueue::new();
        // Ready entry (1 attempt, well under max_retries=5).
        q.queue.push_back(make_entry("orders", 100, 1));
        // Exhausted entry (at max_retries).
        q.queue.push_back(make_entry("orders", 200, 5));

        let (ready, exhausted) = q.drain_due();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].request.source_lsn, 100);
        assert_eq!(exhausted.len(), 1);
        assert_eq!(exhausted[0].request.source_lsn, 200);
    }

    #[test]
    fn not_due_stays_in_queue() {
        let mut q = CrossShardRetryQueue::new();
        let mut entry = make_entry("orders", 100, 1);
        entry.next_retry_at = Instant::now() + Duration::from_secs(60);
        q.queue.push_back(entry);

        let (ready, exhausted) = q.drain_due();
        assert!(ready.is_empty());
        assert!(exhausted.is_empty());
        assert_eq!(q.len(), 1);
    }
}
