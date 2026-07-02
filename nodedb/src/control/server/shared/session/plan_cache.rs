// SPDX-License-Identifier: BUSL-1.1

//! Per-session plan cache for prepared statements.
//!
//! Caches compiled `Vec<PhysicalTask>` per SQL string, keyed by
//! the SQL text hash. Each cached entry records the
//! [`DescriptorVersionSet`] the plan was built against; on
//! lookup the cache validates each `(descriptor_id, version)`
//! pair against a caller-supplied lookup and evicts the entry
//! on any mismatch. Unrelated DDLs no longer invalidate
//! unrelated cached plans.

use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_cluster::DescriptorId;

use crate::control::planner::descriptor_set::DescriptorVersionSet;
use nodedb_physical::physical_task::PhysicalTask;

/// Monotonic schema version counter retained for backwards
/// compatibility with metrics and tracing callers that still
/// snapshot "the schema version" as an opaque token. The plan
/// cache no longer keys on this value — see
/// [`PlanCache`] for the per-descriptor invalidation path.
pub struct SchemaVersion {
    version: AtomicU64,
}

impl SchemaVersion {
    pub fn new() -> Self {
        Self {
            version: AtomicU64::new(1),
        }
    }

    /// Get current schema version.
    pub fn current(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Bump the schema version. Called on CREATE/DROP/ALTER DDL.
    pub fn bump(&self) -> u64 {
        self.version.fetch_add(1, Ordering::AcqRel) + 1
    }
}

impl Default for SchemaVersion {
    fn default() -> Self {
        Self::new()
    }
}

/// Cached entry: compiled physical tasks and the descriptor
/// version set they were built against.
struct CachedEntry {
    tasks: Vec<PhysicalTask>,
    versions: DescriptorVersionSet,
}

/// Per-session LRU plan cache.
///
/// Keyed by SQL hash. Each entry records the
/// [`DescriptorVersionSet`] the plan was built against. On
/// lookup, the caller supplies a `current_version` closure that
/// maps each recorded descriptor id to its current version (or
/// `None` if the descriptor has been dropped). If any
/// `(id, version)` no longer matches, the entry is evicted and
/// the caller falls through to a fresh plan.
pub struct PlanCache {
    entries: HashMap<u64, CachedEntry>,
    max_entries: usize,
    /// Insertion order for LRU eviction (oldest at front).
    order: VecDeque<u64>,
}

impl PlanCache {
    /// Create a new plan cache.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
            order: VecDeque::new(),
        }
    }

    /// Look up cached physical tasks for the given SQL.
    ///
    /// On a hit returns both the cached tasks and the
    /// `DescriptorVersionSet` they were built against — the
    /// caller feeds the set into
    /// `SharedState::acquire_plan_lease_scope` so cache hits
    /// and fresh plans share the same lease-acquisition path.
    ///
    /// Returns `None` if not cached or if any recorded
    /// descriptor version has bumped / been dropped. Stale
    /// entries are evicted automatically.
    pub fn get<F>(
        &mut self,
        sql: &str,
        current_version: F,
    ) -> Option<(Vec<PhysicalTask>, DescriptorVersionSet)>
    where
        F: Fn(&DescriptorId) -> Option<u64>,
    {
        let key = hash_sql(sql);
        let entry = self.entries.get(&key)?;
        if entry.versions.all_fresh(&current_version) {
            return Some((entry.tasks.clone(), entry.versions.clone()));
        }
        // Stale — evict.
        self.entries.remove(&key);
        self.order.retain(|k| *k != key);
        None
    }

    /// Store compiled physical tasks in the cache along with
    /// the descriptor version set they were built against.
    pub fn put(&mut self, sql: &str, tasks: Vec<PhysicalTask>, versions: DescriptorVersionSet) {
        let key = hash_sql(sql);

        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(key) {
            e.insert(CachedEntry { tasks, versions });
            return;
        }

        // Evict oldest if at capacity.
        while self.entries.len() >= self.max_entries {
            if let Some(oldest_key) = self.order.pop_front() {
                self.entries.remove(&oldest_key);
            } else {
                break;
            }
        }

        self.entries.insert(key, CachedEntry { tasks, versions });
        self.order.push_back(key);
    }

    /// Invalidate all entries (called on DISCARD ALL, session reset, etc.).
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

fn hash_sql(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::types::{TenantId, VShardId};
    use nodedb_cluster::DescriptorKind;
    use nodedb_physical::physical_plan::MetaOp;
    use nodedb_physical::physical_task::PostSetOp;

    fn dummy_tasks() -> Vec<PhysicalTask> {
        vec![PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Meta(MetaOp::Checkpoint),
            post_set_op: PostSetOp::None,
        }]
    }

    fn collection_id(name: &str) -> DescriptorId {
        DescriptorId::new(1, DescriptorKind::Collection, name.to_string())
    }

    fn versions_for(pairs: &[(&str, u64)]) -> DescriptorVersionSet {
        let mut set = DescriptorVersionSet::new();
        for (name, v) in pairs {
            set.record(collection_id(name), *v);
        }
        set
    }

    fn always_v(expected: u64) -> impl Fn(&DescriptorId) -> Option<u64> {
        move |_| Some(expected)
    }

    fn version_map(map: Vec<(&'static str, u64)>) -> impl Fn(&DescriptorId) -> Option<u64> {
        move |id: &DescriptorId| {
            map.iter()
                .find(|(name, _)| *name == id.name)
                .map(|(_, v)| *v)
        }
    }

    #[test]
    fn cache_hit_same_version() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_tasks(), versions_for(&[("foo", 1)]));
        assert!(cache.get("SELECT 1", always_v(1)).is_some());
    }

    #[test]
    fn cache_miss_version_bump() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_tasks(), versions_for(&[("foo", 1)]));
        assert!(cache.get("SELECT 1", always_v(2)).is_none());
        // Re-lookup returns None — the stale entry was evicted.
        assert!(cache.get("SELECT 1", always_v(1)).is_none());
    }

    #[test]
    fn cache_miss_descriptor_dropped() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_tasks(), versions_for(&[("foo", 1)]));
        assert!(cache.get("SELECT 1", |_: &DescriptorId| None).is_none());
    }

    #[test]
    fn unrelated_descriptor_bump_does_not_invalidate() {
        let mut cache = PlanCache::new(10);
        cache.put(
            "SELECT FROM foo",
            dummy_tasks(),
            versions_for(&[("foo", 1)]),
        );
        // bar bumps but we only track foo → cache hit still.
        let lookup = version_map(vec![("foo", 1), ("bar", 99)]);
        assert!(cache.get("SELECT FROM foo", lookup).is_some());
    }

    #[test]
    fn lru_eviction() {
        let mut cache = PlanCache::new(2);
        cache.put("SELECT 1", dummy_tasks(), versions_for(&[("a", 1)]));
        cache.put("SELECT 2", dummy_tasks(), versions_for(&[("b", 1)]));
        cache.put("SELECT 3", dummy_tasks(), versions_for(&[("c", 1)]));
        assert!(cache.get("SELECT 1", always_v(1)).is_none());
        assert!(cache.get("SELECT 2", always_v(1)).is_some());
        assert!(cache.get("SELECT 3", always_v(1)).is_some());
    }

    #[test]
    fn clear_empties_cache() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_tasks(), versions_for(&[("foo", 1)]));
        cache.clear();
        assert!(cache.get("SELECT 1", always_v(1)).is_none());
    }

    #[test]
    fn schema_version_bump() {
        let sv = SchemaVersion::new();
        assert_eq!(sv.current(), 1);
        assert_eq!(sv.bump(), 2);
        assert_eq!(sv.current(), 2);
    }
}
