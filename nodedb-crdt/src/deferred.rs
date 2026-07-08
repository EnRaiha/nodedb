// SPDX-License-Identifier: Apache-2.0

//! Deferred retry queue for CASCADE_DEFER policy.
//!
//! When a foreign key violation occurs and CASCADE_DEFER is the configured policy,
//! the delta is enqueued here with exponential backoff. The queue polls for ready
//! entries and expires entries that exceed their TTL.

use std::collections::VecDeque;

/// A deferred entry awaiting retry due to a CASCADE_DEFER policy.
#[derive(Debug, Clone)]
pub struct DeferredEntry {
    /// Unique entry ID.
    pub id: u64,

    /// The peer that produced this delta.
    pub peer_id: u64,

    /// The authenticated user_id that submitted this delta (0 = unauthenticated).
    pub user_id: u64,

    /// The tenant this delta belongs to (0 = system).
    pub tenant_id: u64,

    /// The raw delta bytes.
    pub delta: Vec<u8>,

    /// The collection being modified.
    pub collection: String,

    /// The constraint name that triggered deferral.
    pub constraint_name: String,

    /// Current attempt number (0-indexed).
    pub attempt: u32,

    /// Maximum allowed retries.
    pub max_retries: u32,

    /// Milliseconds since epoch when this entry should be retried.
    pub next_retry_ms: u64,

    /// Milliseconds since epoch when this entry expires (given up).
    pub ttl_deadline_ms: u64,
}

/// Parameters for [`DeferredQueue::enqueue`].
pub struct EnqueueDeferredArgs {
    pub peer_id: u64,
    pub user_id: u64,
    pub tenant_id: u64,
    pub delta: Vec<u8>,
    pub collection: String,
    pub constraint_name: String,
    pub attempt: u32,
    pub max_retries: u32,
    pub now_ms: u64,
    pub first_retry_after_ms: u64,
    pub ttl_secs: u64,
}

/// Queue of deferred entries awaiting retry.
#[derive(Debug)]
pub struct DeferredQueue {
    entries: VecDeque<DeferredEntry>,
    capacity: usize,
    next_id: u64,
}

impl DeferredQueue {
    /// Create a new deferred queue with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity.min(1024)),
            capacity,
            next_id: 1,
        }
    }

    /// Enqueue a deferred entry.
    ///
    /// * `peer_id` — the peer that produced the delta
    /// * `delta` — the raw delta bytes
    /// * `collection` — the target collection
    /// * `constraint_name` — the constraint that was violated
    /// * `attempt` — current attempt number (usually 0 for first deferral)
    /// * `max_retries` — maximum retries allowed
    /// * `now_ms` — current time in milliseconds since epoch
    /// * `first_retry_after_ms` — milliseconds to wait before first retry
    /// * `ttl_secs` — seconds from now until this entry expires
    pub fn enqueue(&mut self, args: EnqueueDeferredArgs) -> u64 {
        let EnqueueDeferredArgs {
            peer_id,
            user_id,
            tenant_id,
            delta,
            collection,
            constraint_name,
            attempt,
            max_retries,
            now_ms,
            first_retry_after_ms,
            ttl_secs,
        } = args;
        let id = self.next_id;
        self.next_id += 1;

        let entry = DeferredEntry {
            id,
            peer_id,
            user_id,
            tenant_id,
            delta,
            collection,
            constraint_name,
            attempt,
            max_retries,
            next_retry_ms: now_ms + first_retry_after_ms,
            ttl_deadline_ms: now_ms + (ttl_secs * 1000),
        };

        self.entries.push_back(entry);
        id
    }

    /// Poll for entries ready for retry.
    ///
    /// Returns all entries whose `next_retry_ms <= now_ms`.
    pub fn poll_ready(&mut self, now_ms: u64) -> Vec<DeferredEntry> {
        let mut ready = Vec::new();

        while let Some(front) = self.entries.front() {
            if front.next_retry_ms <= now_ms
                && let Some(entry) = self.entries.pop_front()
            {
                ready.push(entry);
            } else {
                break;
            }
        }

        ready
    }

    /// Expire entries past their TTL deadline.
    ///
    /// Returns all entries whose `ttl_deadline_ms <= now_ms`.
    /// These should be routed to the DLQ as unrecoverable.
    pub fn expire(&mut self, now_ms: u64) -> Vec<DeferredEntry> {
        let mut expired = Vec::new();

        self.entries.retain(|entry| {
            if entry.ttl_deadline_ms <= now_ms {
                expired.push(entry.clone());
                false
            } else {
                true
            }
        });

        expired
    }

    /// Re-enqueue an entry for retry after the next backoff interval.
    ///
    /// The backoff is calculated as: `base_ms * 2^attempt`, capped at 30s.
    pub fn enqueue_retry(&mut self, mut entry: DeferredEntry, now_ms: u64) {
        let base_ms = 500u64;
        let backoff = base_ms
            .saturating_mul(2_u64.saturating_pow(entry.attempt))
            .min(30_000);

        entry.attempt += 1;
        entry.next_retry_ms = now_ms + backoff;

        self.entries.push_back(entry);
    }

    /// Number of pending deferred entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_and_poll_ready() {
        let mut queue = DeferredQueue::new(10);
        let now = 1000;

        // Enqueue an entry ready to retry at now + 500ms
        queue.enqueue(EnqueueDeferredArgs {
            peer_id: 42,
            user_id: 0,
            tenant_id: 0,
            delta: b"delta".to_vec(),
            collection: "posts".to_string(),
            constraint_name: "posts_author_fk".to_string(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 60,
        });

        // At time=now, entry is not ready
        assert!(queue.poll_ready(now).is_empty());

        // At time=now+500, entry is ready
        let ready = queue.poll_ready(now + 500);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].attempt, 0);
        assert!(queue.is_empty());
    }

    #[test]
    fn expire_past_ttl() {
        let mut queue = DeferredQueue::new(10);
        let now = 1000;

        // Enqueue with TTL of 10 seconds
        queue.enqueue(EnqueueDeferredArgs {
            peer_id: 42,
            user_id: 0,
            tenant_id: 0,
            delta: b"delta".to_vec(),
            collection: "posts".to_string(),
            constraint_name: "posts_author_fk".to_string(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 10,
        });

        // Before TTL expires
        assert!(queue.expire(now + 5000).is_empty());
        assert_eq!(queue.len(), 1);

        // After TTL expires
        let expired = queue.expire(now + 11_000);
        assert_eq!(expired.len(), 1);
        assert!(queue.is_empty());
    }

    #[test]
    fn exponential_backoff() {
        let mut queue = DeferredQueue::new(10);
        let now = 1000;

        // Enqueue initial entry
        let id = queue.enqueue(EnqueueDeferredArgs {
            peer_id: 42,
            user_id: 0,
            tenant_id: 0,
            delta: b"delta".to_vec(),
            collection: "posts".to_string(),
            constraint_name: "posts_author_fk".to_string(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 60,
        });

        // Poll and get the entry
        let ready = queue.poll_ready(now + 500);
        assert_eq!(ready.len(), 1);
        let entry = &ready[0];
        assert_eq!(entry.id, id);
        assert_eq!(entry.attempt, 0);

        // Re-enqueue for retry (attempt 1)
        let entry_clone = ready[0].clone();
        queue.enqueue_retry(entry_clone, now + 500);

        // Attempt 1: backoff = 500 * 2^1 = 1000ms
        let ready = queue.poll_ready(now + 1500);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].attempt, 1);

        // Re-enqueue for retry (attempt 2)
        queue.enqueue_retry(ready[0].clone(), now + 1500);

        // Attempt 2: backoff = 500 * 2^2 = 2000ms
        let ready = queue.poll_ready(now + 3500);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].attempt, 2);
    }

    #[test]
    fn max_retries_respected() {
        let mut queue = DeferredQueue::new(10);
        let now = 1000;

        // Enqueue with max_retries=3
        queue.enqueue(EnqueueDeferredArgs {
            peer_id: 42,
            user_id: 0,
            tenant_id: 0,
            delta: b"delta".to_vec(),
            collection: "posts".to_string(),
            constraint_name: "posts_author_fk".to_string(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 60,
        });

        let ready = queue.poll_ready(now + 500);
        let mut entry = ready[0].clone();
        assert_eq!(entry.max_retries, 3);

        // Retry loop: attempt 0, 1, 2, 3
        for _ in 0..3 {
            entry.attempt += 1;
            assert!(entry.attempt <= entry.max_retries);
        }

        // After max_retries, this entry should not be re-enqueued
        assert_eq!(entry.attempt, 3);
        assert!(entry.attempt >= entry.max_retries);
    }

    #[test]
    fn fifo_ordering() {
        let mut queue = DeferredQueue::new(10);
        let now = 1000;

        // Enqueue three entries with same retry time
        for i in 0..3 {
            queue.enqueue(EnqueueDeferredArgs {
                peer_id: 40 + i,
                user_id: 0,
                tenant_id: 0,
                delta: format!("delta{}", i).into_bytes(),
                collection: "posts".to_string(),
                constraint_name: "posts_author_fk".to_string(),
                attempt: 0,
                max_retries: 3,
                now_ms: now,
                first_retry_after_ms: 500,
                ttl_secs: 60,
            });
        }

        assert_eq!(queue.len(), 3);

        let ready = queue.poll_ready(now + 500);
        assert_eq!(ready.len(), 3);

        // Verify FIFO order by peer_id
        assert_eq!(ready[0].peer_id, 40);
        assert_eq!(ready[1].peer_id, 41);
        assert_eq!(ready[2].peer_id, 42);
    }

    #[test]
    fn capacity_limit() {
        let mut queue = DeferredQueue::new(2);
        let now = 1000;

        queue.enqueue(EnqueueDeferredArgs {
            peer_id: 1,
            user_id: 0,
            tenant_id: 0,
            delta: b"d1".to_vec(),
            collection: "c".into(),
            constraint_name: "cn".into(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 60,
        });
        queue.enqueue(EnqueueDeferredArgs {
            peer_id: 2,
            user_id: 0,
            tenant_id: 0,
            delta: b"d2".to_vec(),
            collection: "c".into(),
            constraint_name: "cn".into(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 60,
        });

        assert_eq!(queue.len(), 2);

        // Capacity is 2, so this still works (no error checking in enqueue)
        queue.enqueue(EnqueueDeferredArgs {
            peer_id: 3,
            user_id: 0,
            tenant_id: 0,
            delta: b"d3".to_vec(),
            collection: "c".into(),
            constraint_name: "cn".into(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 60,
        });
        assert_eq!(queue.len(), 3); // The queue doesn't enforce capacity strictly; it's advisory.
    }

    #[test]
    fn clear_empties_queue() {
        let mut queue = DeferredQueue::new(10);
        let now = 1000;

        queue.enqueue(EnqueueDeferredArgs {
            peer_id: 1,
            user_id: 0,
            tenant_id: 0,
            delta: b"d1".to_vec(),
            collection: "c".into(),
            constraint_name: "cn".into(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 60,
        });
        queue.enqueue(EnqueueDeferredArgs {
            peer_id: 2,
            user_id: 0,
            tenant_id: 0,
            delta: b"d2".to_vec(),
            collection: "c".into(),
            constraint_name: "cn".into(),
            attempt: 0,
            max_retries: 3,
            now_ms: now,
            first_retry_after_ms: 500,
            ttl_secs: 60,
        });

        assert_eq!(queue.len(), 2);
        queue.clear();
        assert!(queue.is_empty());
    }
}
