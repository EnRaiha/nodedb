// SPDX-License-Identifier: BUSL-1.1

//! Usage store: accumulates flushed usage events for querying.
//!
//! In production, this would write to `_system.usage` as a timeseries
//! collection with rollups and retention. For now, it stores in-memory
//! with a ring buffer for the most recent events.

use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::counter::UsageEvent;

/// Default ring-buffer capacity for raw usage events.
pub const DEFAULT_MAX_EVENTS: usize = 100_000;

/// Default cap on the number of distinct users/orgs/tenants tracked in the
/// totals maps. This is a process-lifetime store, so the maps must be
/// bounded; once a map hits this cap, new entities are refused (existing
/// ones keep updating) and the refusal is surfaced via a dropped-entry
/// counter plus a one-time `tracing::warn!`.
pub const DEFAULT_MAX_TRACKED_SCOPES: usize = 100_000;

/// In-memory usage store with ring buffer.
pub struct UsageStore {
    /// Recent events (ring buffer, max capacity). `VecDeque` gives O(1)
    /// eviction of the oldest event at capacity, vs. `Vec::remove(0)`'s
    /// O(n) shift.
    events: RwLock<VecDeque<UsageEvent>>,
    /// Maximum events to retain.
    max_events: usize,
    /// Maximum distinct keys retained per totals map below.
    max_tracked_scopes: usize,
    /// Aggregated totals per user for quota checking.
    user_totals: RwLock<HashMap<String, u64>>,
    /// Aggregated totals per org.
    org_totals: RwLock<HashMap<String, u64>>,
    /// Persistent per-tenant usage summaries. Updated on every `ingest()` call.
    /// Serves as the queryable aggregation layer for `SHOW USAGE FOR TENANT`
    /// and `EXPORT USAGE`. Survives ring buffer eviction.
    tenant_totals: RwLock<HashMap<u64, TenantUsageSummary>>,
    /// Count of user-total entries refused because `user_totals` was at
    /// `max_tracked_scopes`. Existing users keep accumulating normally;
    /// only *new* distinct users are dropped.
    dropped_user_entries: AtomicU64,
    /// Same as `dropped_user_entries`, for `org_totals`.
    dropped_org_entries: AtomicU64,
    /// Same as `dropped_user_entries`, for `tenant_totals`.
    dropped_tenant_entries: AtomicU64,
    /// Ensures the capacity warning is logged once, not once per dropped event.
    warned_totals_capacity: AtomicBool,
}

impl UsageStore {
    /// Construct with the default totals-map bound (see `DEFAULT_MAX_TRACKED_SCOPES`).
    pub fn new(max_events: usize) -> Self {
        Self::with_bounds(max_events, DEFAULT_MAX_TRACKED_SCOPES)
    }

    /// Construct with explicit bounds on both the event ring buffer and the
    /// per-entity totals maps.
    pub fn with_bounds(max_events: usize, max_tracked_scopes: usize) -> Self {
        Self {
            events: RwLock::new(VecDeque::with_capacity(max_events.min(100_000))),
            max_events,
            max_tracked_scopes,
            user_totals: RwLock::new(HashMap::new()),
            org_totals: RwLock::new(HashMap::new()),
            tenant_totals: RwLock::new(HashMap::new()),
            dropped_user_entries: AtomicU64::new(0),
            dropped_org_entries: AtomicU64::new(0),
            dropped_tenant_entries: AtomicU64::new(0),
            warned_totals_capacity: AtomicBool::new(false),
        }
    }

    /// Ingest flushed events from the counter.
    ///
    /// Updates three aggregation layers:
    /// 1. Per-user totals (for `SHOW USAGE FOR AUTH USER`)
    /// 2. Per-org totals (for `SHOW USAGE FOR ORG`)
    /// 3. Per-tenant summaries (for `SHOW USAGE FOR TENANT` / `EXPORT USAGE`)
    pub fn ingest(&self, events: Vec<UsageEvent>) {
        // Update per-user and per-org totals.
        {
            let mut user_totals = self.user_totals.write().unwrap_or_else(|p| p.into_inner());
            let mut org_totals = self.org_totals.write().unwrap_or_else(|p| p.into_inner());
            for e in &events {
                let tracked = bounded_upsert(
                    &mut user_totals,
                    e.auth_user_id.clone(),
                    self.max_tracked_scopes,
                    || 0u64,
                    |v| *v += e.tokens,
                );
                if !tracked {
                    self.dropped_user_entries.fetch_add(1, Ordering::Relaxed);
                }

                if !e.org_id.is_empty() {
                    let tracked = bounded_upsert(
                        &mut org_totals,
                        e.org_id.clone(),
                        self.max_tracked_scopes,
                        || 0u64,
                        |v| *v += e.tokens,
                    );
                    if !tracked {
                        self.dropped_org_entries.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        // Update per-tenant summaries (persistent aggregation).
        {
            let mut tenant_totals = self
                .tenant_totals
                .write()
                .unwrap_or_else(|p| p.into_inner());
            for e in &events {
                let tracked = bounded_upsert(
                    &mut tenant_totals,
                    e.tenant_id,
                    self.max_tracked_scopes,
                    TenantUsageSummary::default,
                    |v| accumulate_event(v, e),
                );
                if !tracked {
                    self.dropped_tenant_entries.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if self.dropped_user_entries.load(Ordering::Relaxed) > 0
            || self.dropped_org_entries.load(Ordering::Relaxed) > 0
            || self.dropped_tenant_entries.load(Ordering::Relaxed) > 0
        {
            self.warn_capacity_once();
        }

        // Store events in ring buffer. O(1) eviction via VecDeque::pop_front,
        // vs. Vec::remove(0)'s O(n) shift on every insert once full.
        let mut stored = self.events.write().unwrap_or_else(|p| p.into_inner());
        for e in events {
            if stored.len() >= self.max_events {
                stored.pop_front();
            }
            stored.push_back(e);
        }
    }

    /// Log the totals-capacity warning exactly once for this store's lifetime.
    fn warn_capacity_once(&self) {
        if !self.warned_totals_capacity.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                cap = self.max_tracked_scopes,
                dropped_users = self.dropped_user_entries.load(Ordering::Relaxed),
                dropped_orgs = self.dropped_org_entries.load(Ordering::Relaxed),
                dropped_tenants = self.dropped_tenant_entries.load(Ordering::Relaxed),
                "usage store totals at capacity — new distinct users/orgs/tenants are no \
                 longer being tracked in aggregation (existing entries keep updating); see \
                 dropped_user_entries()/dropped_org_entries()/dropped_tenant_entries()"
            );
        }
    }

    /// Count of new distinct users refused since the totals map hit capacity.
    pub fn dropped_user_entries(&self) -> u64 {
        self.dropped_user_entries.load(Ordering::Relaxed)
    }

    /// Count of new distinct orgs refused since the totals map hit capacity.
    pub fn dropped_org_entries(&self) -> u64 {
        self.dropped_org_entries.load(Ordering::Relaxed)
    }

    /// Count of new distinct tenants refused since the totals map hit capacity.
    pub fn dropped_tenant_entries(&self) -> u64 {
        self.dropped_tenant_entries.load(Ordering::Relaxed)
    }

    /// The configured cap on distinct keys per totals map (see
    /// `max_tracked_scopes` on `MeteringConfig`). Exposed so observability
    /// surfaces can report drop counts alongside the ceiling that produced them.
    pub fn max_tracked_scopes(&self) -> usize {
        self.max_tracked_scopes
    }

    /// Get total tokens used by a user.
    pub fn user_total(&self, user_id: &str) -> u64 {
        let totals = self.user_totals.read().unwrap_or_else(|p| p.into_inner());
        *totals.get(user_id).unwrap_or(&0)
    }

    /// Get total tokens used by an org.
    pub fn org_total(&self, org_id: &str) -> u64 {
        let totals = self.org_totals.read().unwrap_or_else(|p| p.into_inner());
        *totals.get(org_id).unwrap_or(&0)
    }

    /// Query usage events filtered by user and/or time range.
    pub fn query(
        &self,
        user_filter: Option<&str>,
        org_filter: Option<&str>,
        since_secs: u64,
    ) -> Vec<UsageEvent> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events
            .iter()
            .filter(|e| {
                let user_ok = user_filter.is_none_or(|u| e.auth_user_id == u);
                let org_ok = org_filter.is_none_or(|o| e.org_id == o);
                let time_ok = since_secs == 0 || e.timestamp_secs >= since_secs;
                user_ok && org_ok && time_ok
            })
            .cloned()
            .collect()
    }

    /// Export usage as NDJSON (newline-delimited JSON).
    pub fn export_ndjson(&self, user_filter: Option<&str>, since_secs: u64) -> String {
        let events = self.query(user_filter, None, since_secs);
        events
            .iter()
            .map(|e| {
                serde_json::json!({
                    "auth_user_id": e.auth_user_id,
                    "org_id": e.org_id,
                    "tenant_id": e.tenant_id,
                    "collection": e.collection,
                    "engine": e.engine,
                    "operation": e.operation,
                    "tokens": e.tokens,
                    "timestamp": e.timestamp_secs,
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Query usage events filtered by tenant_id and optional time range.
    pub fn query_by_tenant(&self, tenant_id: u64, since_secs: u64) -> Vec<UsageEvent> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events
            .iter()
            .filter(|e| {
                e.tenant_id == tenant_id && (since_secs == 0 || e.timestamp_secs >= since_secs)
            })
            .cloned()
            .collect()
    }

    /// Export usage for a specific tenant as JSON (billing integration format).
    pub fn export_tenant_json(&self, tenant_id: u64, since_secs: u64) -> String {
        let events = self.query_by_tenant(tenant_id, since_secs);

        let mut summary = TenantUsageSummary::default();
        for e in &events {
            accumulate_event(&mut summary, e);
        }

        serde_json::json!({
            "tenant_id": tenant_id,
            "reads": { "count": summary.reads_count, "tokens": summary.reads_tokens },
            "writes": { "count": summary.writes_count, "tokens": summary.writes_tokens },
            "vector_searches": summary.vector_searches,
            "graph_traversals": summary.graph_traversals,
            "total_events": summary.total_events,
        })
        .to_string()
    }

    /// Aggregate usage events by tenant_id.
    ///
    /// Returns a map of `tenant_id → TenantUsageSummary` with rolled-up
    /// read/write counts and engine-specific metrics.
    pub fn aggregate_by_tenant(&self) -> HashMap<u64, TenantUsageSummary> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        let mut summaries: HashMap<u64, TenantUsageSummary> = HashMap::new();

        for e in events.iter() {
            let summary = summaries.entry(e.tenant_id).or_default();
            accumulate_event(summary, e);
        }

        summaries
    }

    /// Get the persistent usage summary for a specific tenant.
    ///
    /// Unlike `aggregate_by_tenant()` which recomputes from the event ring buffer,
    /// this reads from the persistent aggregation layer that survives ring buffer eviction.
    pub fn tenant_summary(&self, tenant_id: u64) -> Option<TenantUsageSummary> {
        let totals = self.tenant_totals.read().unwrap_or_else(|p| p.into_inner());
        totals.get(&tenant_id).cloned()
    }

    /// Total events stored.
    pub fn count(&self) -> usize {
        self.events.read().unwrap_or_else(|p| p.into_inner()).len()
    }
}

/// Update `map[key]` via `update`, inserting `default()` first if the key is
/// new. If the key is new and `map` already has `cap` entries, the update is
/// refused entirely (returns `false`) rather than growing the map further;
/// existing keys always update regardless of `cap`.
fn bounded_upsert<K, V>(
    map: &mut HashMap<K, V>,
    key: K,
    cap: usize,
    default: impl FnOnce() -> V,
    update: impl FnOnce(&mut V),
) -> bool
where
    K: std::hash::Hash + Eq,
{
    if let Some(v) = map.get_mut(&key) {
        update(v);
        true
    } else if map.len() < cap {
        let mut v = default();
        update(&mut v);
        map.insert(key, v);
        true
    } else {
        false
    }
}

/// Accumulate a single usage event into a summary.
fn accumulate_event(summary: &mut TenantUsageSummary, e: &UsageEvent) {
    summary.total_tokens += e.tokens;
    summary.total_events += 1;
    if is_read_operation(&e.operation) {
        summary.reads_count += 1;
        summary.reads_tokens += e.tokens;
    } else {
        summary.writes_count += 1;
        summary.writes_tokens += e.tokens;
    }
    if e.operation == "vector_search" {
        summary.vector_searches += 1;
    }
    if e.engine == "graph" {
        summary.graph_traversals += 1;
    }
}

/// Whether an operation is a read (vs. write) based on the operation name.
fn is_read_operation(operation: &str) -> bool {
    matches!(
        operation,
        "point_get"
            | "range_scan"
            | "kv_get"
            | "kv_scan"
            | "text_search"
            | "vector_search"
            | "timeseries_scan"
            | "columnar_scan"
    )
}

/// Aggregated usage summary for a single tenant.
///
/// Computed by [`UsageStore::aggregate_by_tenant()`] from raw usage events.
/// Used by billing integration, quota enforcement, and the SHOW TENANT USAGE DDL.
#[derive(Debug, Default, Clone)]
pub struct TenantUsageSummary {
    /// Total tokens consumed across all operations.
    pub total_tokens: u64,
    /// Total number of metering events recorded.
    pub total_events: u64,
    /// Number of read operations (point get, scan, search).
    pub reads_count: u64,
    /// Tokens consumed by read operations.
    pub reads_tokens: u64,
    /// Number of write operations (put, delete, bulk).
    pub writes_count: u64,
    /// Tokens consumed by write operations.
    pub writes_tokens: u64,
    /// Number of vector similarity searches.
    pub vector_searches: u64,
    /// Number of graph traversal operations.
    pub graph_traversals: u64,
}

impl Default for UsageStore {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_EVENTS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event(user: &str, tokens: u64) -> UsageEvent {
        UsageEvent {
            auth_user_id: user.into(),
            org_id: "acme".into(),
            tenant_id: 1,
            collection: "orders".into(),
            engine: "document_schemaless".into(),
            operation: "point_get".into(),
            tokens,
            timestamp_secs: 1700000000,
        }
    }

    fn test_event_for(user: &str, org: &str, tenant_id: u64, tokens: u64) -> UsageEvent {
        UsageEvent {
            auth_user_id: user.into(),
            org_id: org.into(),
            tenant_id,
            collection: "orders".into(),
            engine: "document_schemaless".into(),
            operation: "point_get".into(),
            tokens,
            timestamp_secs: 1700000000,
        }
    }

    #[test]
    fn ingest_and_query() {
        let store = UsageStore::new(1000);
        store.ingest(vec![test_event("u1", 10), test_event("u2", 20)]);

        assert_eq!(store.count(), 2);
        assert_eq!(store.user_total("u1"), 10);
        assert_eq!(store.user_total("u2"), 20);
        assert_eq!(store.org_total("acme"), 30);
    }

    #[test]
    fn query_with_filter() {
        let store = UsageStore::new(1000);
        store.ingest(vec![test_event("u1", 10), test_event("u2", 20)]);

        let u1_events = store.query(Some("u1"), None, 0);
        assert_eq!(u1_events.len(), 1);
        assert_eq!(u1_events[0].tokens, 10);
    }

    #[test]
    fn ring_buffer_drops_oldest() {
        let store = UsageStore::new(2);
        store.ingest(vec![
            test_event("u1", 1),
            test_event("u2", 2),
            test_event("u3", 3),
        ]);

        assert_eq!(store.count(), 2); // Only last 2 retained.
    }

    #[test]
    fn ring_buffer_retains_newest_and_stays_stable_across_many_inserts() {
        let store = UsageStore::new(3);
        for i in 0..10u64 {
            store.ingest(vec![test_event(&format!("u{i}"), i)]);
            assert!(store.count() <= 3, "ring buffer must never exceed capacity");
        }

        assert_eq!(store.count(), 3);
        let all: Vec<u64> = store
            .query(None, None, 0)
            .into_iter()
            .map(|e| e.tokens)
            .collect();
        // Oldest (0..=6) dropped; newest 3 (7, 8, 9) retained, order preserved.
        assert_eq!(all, vec![7, 8, 9]);
    }

    #[test]
    fn totals_stop_growing_past_bound_and_overflow_is_observable() {
        let store = UsageStore::with_bounds(10_000, 2);

        store.ingest(vec![
            test_event_for("u1", "org1", 1, 10),
            test_event_for("u2", "org2", 2, 10),
        ]);
        assert_eq!(store.dropped_user_entries(), 0);
        assert_eq!(store.dropped_tenant_entries(), 0);

        // A third distinct user/org/tenant exceeds the cap of 2.
        store.ingest(vec![test_event_for("u3", "org3", 3, 10)]);

        assert_eq!(store.dropped_user_entries(), 1);
        assert_eq!(store.dropped_org_entries(), 1);
        assert_eq!(store.dropped_tenant_entries(), 1);
        // The dropped entity's totals were never recorded — not silently
        // fabricated as zero-and-forgotten, genuinely absent.
        assert_eq!(store.user_total("u3"), 0);

        // Existing entries keep updating past the cap being hit.
        store.ingest(vec![test_event_for("u1", "org1", 1, 5)]);
        assert_eq!(store.user_total("u1"), 15);
        assert_eq!(store.dropped_user_entries(), 1); // No new drop for an existing key.
    }
}
