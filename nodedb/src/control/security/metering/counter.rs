// SPDX-License-Identifier: BUSL-1.1

//! Per-core atomic usage counters with periodic flush.
//!
//! Each Data Plane core has its own set of counters (no contention).
//! Periodically flushed to the Control Plane `_system.usage` store.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// A single metering event with dimension values.
#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub auth_user_id: String,
    pub org_id: String,
    pub tenant_id: u64,
    pub collection: String,
    pub engine: String,
    pub operation: String,
    pub tokens: u64,
    pub timestamp_secs: u64,
}

/// Aggregation key for bucketing usage events.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct BucketKey {
    auth_user_id: String,
    org_id: String,
    tenant_id: u64,
    collection: String,
    engine: String,
    operation: String,
}

impl BucketKey {
    /// Compare against an event's dimension fields without cloning either side.
    fn matches(&self, event: &UsageEvent) -> bool {
        self.auth_user_id == event.auth_user_id
            && self.org_id == event.org_id
            && self.tenant_id == event.tenant_id
            && self.collection == event.collection
            && self.engine == event.engine
            && self.operation == event.operation
    }
}

/// Hash an event's dimension tuple without allocating (no `BucketKey` clone).
/// Used as the map key so the hot path can look up an existing bucket from
/// borrowed fields; the owned `BucketKey` is only materialized on insert.
fn dimension_hash(event: &UsageEvent) -> u64 {
    let mut hasher = DefaultHasher::new();
    event.auth_user_id.hash(&mut hasher);
    event.org_id.hash(&mut hasher);
    event.tenant_id.hash(&mut hasher);
    event.collection.hash(&mut hasher);
    event.engine.hash(&mut hasher);
    event.operation.hash(&mut hasher);
    hasher.finish()
}

/// One aggregation bucket: the owned dimension key plus its running total.
struct Bucket {
    key: BucketKey,
    counter: AtomicU64,
}

/// Per-core usage counter that aggregates events into buckets.
pub struct UsageCounter {
    /// Aggregated token counts, keyed by a hash of the dimension tuple.
    /// A `Vec` chain per hash absorbs the (rare) 64-bit hash collision
    /// without ever misattributing tokens across distinct dimension keys.
    buckets: RwLock<HashMap<u64, Vec<Bucket>>>,
    /// Total tokens metered since last flush.
    total_tokens: AtomicU64,
}

impl UsageCounter {
    pub fn new() -> Self {
        Self {
            buckets: RwLock::new(HashMap::new()),
            total_tokens: AtomicU64::new(0),
        }
    }

    /// Record a usage event. Lock-free (read-lock only, no allocation) on
    /// the hot path where the bucket already exists; the owned `BucketKey`
    /// (5 `String` clones) is built only on the slow path that inserts a
    /// brand-new bucket.
    pub fn record(&self, event: &UsageEvent) {
        let hash = dimension_hash(event);

        // Fast path: bucket exists, atomic add, zero allocation.
        {
            let buckets = self.buckets.read().unwrap_or_else(|p| p.into_inner());
            if let Some(chain) = buckets.get(&hash)
                && let Some(bucket) = chain.iter().find(|b| b.key.matches(event))
            {
                bucket.counter.fetch_add(event.tokens, Ordering::Relaxed);
                self.total_tokens.fetch_add(event.tokens, Ordering::Relaxed);
                return;
            }
        }

        // Slow path: bucket didn't exist (or hash collided with a different
        // key) — build the owned key and insert.
        let key = BucketKey {
            auth_user_id: event.auth_user_id.clone(),
            org_id: event.org_id.clone(),
            tenant_id: event.tenant_id,
            collection: event.collection.clone(),
            engine: event.engine.clone(),
            operation: event.operation.clone(),
        };

        let mut buckets = self.buckets.write().unwrap_or_else(|p| p.into_inner());
        let chain = buckets.entry(hash).or_default();
        match chain.iter().find(|b| b.key == key) {
            Some(bucket) => {
                bucket.counter.fetch_add(event.tokens, Ordering::Relaxed);
            }
            None => {
                chain.push(Bucket {
                    key,
                    counter: AtomicU64::new(event.tokens),
                });
            }
        }
        self.total_tokens.fetch_add(event.tokens, Ordering::Relaxed);
    }

    /// Drain all accumulated counters for flushing to the store.
    /// Returns aggregated events and resets counters to zero.
    pub fn drain(&self) -> Vec<UsageEvent> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let buckets = self.buckets.read().unwrap_or_else(|p| p.into_inner());
        let mut events = Vec::with_capacity(buckets.values().map(Vec::len).sum());

        for chain in buckets.values() {
            for bucket in chain {
                let tokens = bucket.counter.swap(0, Ordering::Relaxed);
                if tokens > 0 {
                    events.push(UsageEvent {
                        auth_user_id: bucket.key.auth_user_id.clone(),
                        org_id: bucket.key.org_id.clone(),
                        tenant_id: bucket.key.tenant_id,
                        collection: bucket.key.collection.clone(),
                        engine: bucket.key.engine.clone(),
                        operation: bucket.key.operation.clone(),
                        tokens,
                        timestamp_secs: now,
                    });
                }
            }
        }

        self.total_tokens.store(0, Ordering::Relaxed);
        events
    }

    /// Total tokens metered since last flush.
    pub fn total_tokens(&self) -> u64 {
        self.total_tokens.load(Ordering::Relaxed)
    }
}

impl Default for UsageCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the periodic usage flush task.
pub fn spawn_flush_task(
    counter: std::sync::Arc<UsageCounter>,
    store: std::sync::Arc<super::store::UsageStore>,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(10)));
        ticker.tick().await; // Skip first immediate tick.
        loop {
            ticker.tick().await;
            let events = counter.drain();
            if !events.is_empty() {
                store.ingest(events);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event(user: &str, op: &str, tokens: u64) -> UsageEvent {
        UsageEvent {
            auth_user_id: user.into(),
            org_id: "acme".into(),
            tenant_id: 1,
            collection: "orders".into(),
            engine: "document_schemaless".into(),
            operation: op.into(),
            tokens,
            timestamp_secs: 0,
        }
    }

    #[test]
    fn record_and_drain() {
        let counter = UsageCounter::new();
        counter.record(&test_event("u1", "point_get", 1));
        counter.record(&test_event("u1", "point_get", 1));
        counter.record(&test_event("u1", "vector_search", 20));

        assert_eq!(counter.total_tokens(), 22);

        let events = counter.drain();
        assert_eq!(events.len(), 2); // 2 unique bucket keys.
        assert_eq!(counter.total_tokens(), 0); // Reset after drain.

        let get_tokens: u64 = events
            .iter()
            .filter(|e| e.operation == "point_get")
            .map(|e| e.tokens)
            .sum();
        assert_eq!(get_tokens, 2);
    }

    #[test]
    fn different_users_separate_buckets() {
        let counter = UsageCounter::new();
        counter.record(&test_event("u1", "point_get", 1));
        counter.record(&test_event("u2", "point_get", 1));

        let events = counter.drain();
        assert_eq!(events.len(), 2);
    }

    /// After the first `record()` call takes the slow (insert) path, every
    /// subsequent call for the same dimension key must take the hash-lookup
    /// fast path and still land in the same bucket with the correct total.
    /// A borrow-vs-clone regression that produced a different owned key each
    /// call (e.g. reintroducing a per-call `BucketKey` used only for
    /// insertion, or a hash function that isn't stable per event) would
    /// fragment these into separate buckets and this assertion would fail.
    #[test]
    fn repeated_fast_path_hits_accumulate_into_one_bucket() {
        let counter = UsageCounter::new();
        for _ in 0..1000 {
            counter.record(&test_event("u1", "point_get", 1));
        }

        assert_eq!(counter.total_tokens(), 1000);
        let events = counter.drain();
        assert_eq!(events.len(), 1, "all 1000 events must fold into one bucket");
        assert_eq!(events[0].tokens, 1000);
    }
}
