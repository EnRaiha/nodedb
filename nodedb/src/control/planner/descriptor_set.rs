// SPDX-License-Identifier: BUSL-1.1

//! Per-plan descriptor version set — the C.6 plan-cache key.
//!
//! When the planner resolves a collection through
//! [`crate::control::planner::catalog_adapter::OriginCatalog`], it
//! records the descriptor id and version into a
//! [`DescriptorVersionSet`]. The resulting set is handed back
//! alongside the compiled physical tasks and cached. On
//! subsequent lookups the cache validates each recorded
//! `(id, version)` against the current `SystemCatalog` — if any
//! version has bumped, that one entry is evicted and the plan
//! is re-planned. Unrelated DDLs no longer invalidate unrelated
//! cached plans the way a global schema counter would.
//!
//! The set is ordered and de-duped by `(object_type, database_id,
//! tenant_id, name)` so two plans that touch the same descriptors
//! in the same version produce byte-identical cache keys regardless
//! of resolution order.

use nodedb_cluster::DescriptorId;

/// A stable, de-duped set of `(descriptor_id, version)` pairs
/// the planner read while compiling a plan.
///
/// Stable == entries are sorted by the
/// `(kind, database_id, tenant_id, name)` tuple so equality
/// comparisons between two sets are insertion-order independent. De-duped
/// == a second insertion of the same descriptor id with the
/// same version is a no-op; a second insertion with a
/// different version panics (in debug) / replaces (in release)
/// because a single plan compilation should never observe two
/// different versions of the same descriptor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescriptorVersionSet {
    /// Sorted by `(kind, database_id, tenant_id, name)`.
    entries: Vec<(DescriptorId, u64)>,
}

impl DescriptorVersionSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a descriptor read at the given version. O(log n)
    /// binary search + O(n) worst-case insert on first
    /// observation; O(log n) no-op on re-observation of the
    /// same `(id, version)`.
    ///
    /// Panics in debug builds if the same `id` is recorded with
    /// a different version — that would indicate the planner
    /// read the same descriptor twice under different catalog
    /// snapshots, which is a planning bug. In release builds
    /// the later value wins.
    pub fn record(&mut self, id: DescriptorId, version: u64) {
        match self.entries.binary_search_by(|(k, _)| cmp_id(k, &id)) {
            Ok(idx) => {
                debug_assert_eq!(
                    self.entries[idx].1, version,
                    "planner observed two versions of the same descriptor in one plan"
                );
                self.entries[idx].1 = version;
            }
            Err(idx) => {
                self.entries.insert(idx, (id, version));
            }
        }
    }

    /// Iterate `(id, version)` pairs in stable order.
    pub fn iter(&self) -> impl Iterator<Item = (&DescriptorId, u64)> {
        self.entries.iter().map(|(id, v)| (id, *v))
    }

    /// Number of distinct descriptors recorded.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Validate the recorded versions against a current-version
    /// lookup. Returns `true` iff every `(id, version)` still
    /// matches. Returns `false` on the first mismatch or if any
    /// recorded descriptor no longer exists.
    ///
    /// Used by the plan cache on every hit — a cached plan is
    /// only reusable while every descriptor it was built
    /// against still exists at the same version.
    pub fn all_fresh<F>(&self, current: F) -> bool
    where
        F: Fn(&DescriptorId) -> Option<u64>,
    {
        self.entries.iter().all(|(id, v)| current(id) == Some(*v))
    }
}

/// Total-order comparison for `DescriptorId` since the cluster
/// type does not derive `Ord` (only `Hash + Eq`). We define our
/// own by `(kind discriminant, database_id, tenant_id, name)`.
fn cmp_id(a: &DescriptorId, b: &DescriptorId) -> std::cmp::Ordering {
    (a.kind as u8, a.database_id, a.tenant_id, &a.name).cmp(&(
        b.kind as u8,
        b.database_id,
        b.tenant_id,
        &b.name,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::DescriptorKind;

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    #[test]
    fn record_sorts_and_dedupes() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("b"), 2);
        set.record(id("a"), 1);
        set.record(id("c"), 3);
        set.record(id("a"), 1); // duplicate, no-op

        let names: Vec<&str> = set.iter().map(|(id, _)| id.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn record_different_kinds_coexist() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("orders"), 1);
        set.record(
            DescriptorId::new(0, 1, DescriptorKind::Function, "orders".to_string()),
            1,
        );
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn same_name_in_different_databases_coexist_in_stable_order() {
        let mut set = DescriptorVersionSet::new();
        set.record(
            DescriptorId::new(9, 1, DescriptorKind::Collection, "orders"),
            2,
        );
        set.record(
            DescriptorId::new(3, 1, DescriptorKind::Collection, "orders"),
            1,
        );

        let entries: Vec<_> = set
            .iter()
            .map(|(id, version)| (id.database_id, version))
            .collect();
        assert_eq!(entries, vec![(3, 1), (9, 2)]);
    }

    #[test]
    fn all_fresh_true_when_versions_match() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("a"), 1);
        set.record(id("b"), 2);
        let lookup = |id: &DescriptorId| match id.name.as_str() {
            "a" => Some(1),
            "b" => Some(2),
            _ => None,
        };
        assert!(set.all_fresh(lookup));
    }

    #[test]
    fn all_fresh_false_on_version_mismatch() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("a"), 1);
        set.record(id("b"), 2);
        let lookup = |id: &DescriptorId| match id.name.as_str() {
            "a" => Some(1),
            "b" => Some(3), // bumped
            _ => None,
        };
        assert!(!set.all_fresh(lookup));
    }

    #[test]
    fn all_fresh_false_on_missing_descriptor() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("a"), 1);
        let lookup = |_: &DescriptorId| None;
        assert!(!set.all_fresh(lookup));
    }

    #[test]
    fn empty_set_is_always_fresh() {
        let set = DescriptorVersionSet::new();
        let lookup = |_: &DescriptorId| None;
        assert!(set.all_fresh(lookup));
    }

    #[test]
    fn equal_sets_are_byte_equal_regardless_of_insert_order() {
        let mut a = DescriptorVersionSet::new();
        a.record(id("z"), 1);
        a.record(id("a"), 2);
        let mut b = DescriptorVersionSet::new();
        b.record(id("a"), 2);
        b.record(id("z"), 1);
        assert_eq!(a, b);
    }
}
