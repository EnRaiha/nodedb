// SPDX-License-Identifier: BUSL-1.1

//! Cache isolation across tenants.
//!
//! Tests that the timeseries query_cache and recent_cache do not leak
//! data between tenants. Two tenants with the same collection name and
//! partition IDs must have independent cache entries.

use super::helpers::{TENANT_A, TENANT_B};
use nodedb::engine::timeseries::query_cache::{self, QueryCache};
use nodedb::engine::timeseries::recent_cache::{CachedPartition, RecentWindowCache};

#[test]
fn query_cache_tenant_isolation() {
    let mut cache = QueryCache::new(1024 * 1024);

    let data_a = vec![1u8, 2, 3, 4];
    let data_b = vec![5u8, 6, 7, 8];
    let qhash = query_cache::query_hash("cpu", 0, 1000, 60000);

    // Both tenants cache for the same partition_id and query_hash.
    cache.insert(TENANT_A, 100, qhash, data_a.clone());
    cache.insert(TENANT_B, 100, qhash, data_b.clone());

    // Each tenant sees only their own data.
    assert_eq!(cache.get(TENANT_A, 100, qhash), Some(data_a.as_slice()));
    assert_eq!(cache.get(TENANT_B, 100, qhash), Some(data_b.as_slice()));

    // Cross-tenant lookup must miss.
    assert!(cache.get(TENANT_A, 100, qhash + 1).is_none());
    assert!(cache.get(99, 100, qhash).is_none());
}

#[test]
fn query_cache_invalidate_scoped_to_tenant() {
    let mut cache = QueryCache::new(1024 * 1024);

    cache.insert(TENANT_A, 100, 1, vec![1]);
    cache.insert(TENANT_B, 100, 1, vec![2]);
    assert_eq!(cache.len(), 2);

    // Invalidate Tenant A's partition 100 — Tenant B's entry must survive.
    cache.invalidate_partition(TENANT_A, 100);
    assert_eq!(cache.len(), 1);
    assert!(cache.get(TENANT_A, 100, 1).is_none());
    assert!(cache.get(TENANT_B, 100, 1).is_some());
}

#[test]
fn query_cache_evict_tenant_purge() {
    let mut cache = QueryCache::new(1024 * 1024);

    cache.insert(TENANT_A, 100, 1, vec![1]);
    cache.insert(TENANT_A, 200, 2, vec![2]);
    cache.insert(TENANT_B, 100, 1, vec![3]);
    assert_eq!(cache.len(), 3);

    cache.evict_tenant(TENANT_A);
    assert_eq!(cache.len(), 1);
    assert!(cache.get(TENANT_B, 100, 1).is_some());
}

#[test]
fn recent_cache_tenant_isolation() {
    let mut cache = RecentWindowCache::new(1_000_000, 3_600_000);

    let partition_a = CachedPartition {
        min_ts: 1000,
        max_ts: 2000,
        timestamps: vec![1000, 1500, 2000],
        values: vec![10.0, 20.0, 30.0],
        memory_bytes: 48,
    };
    let partition_b = CachedPartition {
        min_ts: 1000,
        max_ts: 2000,
        timestamps: vec![1000, 1500, 2000],
        values: vec![99.0, 88.0, 77.0],
        memory_bytes: 48,
    };

    // Same collection, same min_ts — different tenants.
    cache.insert(TENANT_A, "metrics", partition_a);
    cache.insert(TENANT_B, "metrics", partition_b);
    assert_eq!(cache.len(), 2);

    // Each tenant sees their own data.
    let a = cache.get(TENANT_A, "metrics", 1000).unwrap();
    assert_eq!(a.values[0], 10.0);
    let b = cache.get(TENANT_B, "metrics", 1000).unwrap();
    assert_eq!(b.values[0], 99.0);

    // Cross-tenant lookup misses.
    assert!(cache.get(99, "metrics", 1000).is_none());
}

#[test]
fn recent_cache_evict_tenant_purge() {
    let mut cache = RecentWindowCache::new(1_000_000, 3_600_000);

    let make = |val: f64| CachedPartition {
        min_ts: 1000,
        max_ts: 2000,
        timestamps: vec![1000],
        values: vec![val],
        memory_bytes: 16,
    };

    cache.insert(TENANT_A, "m", make(1.0));
    cache.insert(TENANT_B, "m", make(2.0));

    cache.evict_tenant(TENANT_A);
    assert_eq!(cache.len(), 1);
    assert!(cache.get(TENANT_A, "m", 1000).is_none());
    assert!(cache.get(TENANT_B, "m", 1000).is_some());
}
