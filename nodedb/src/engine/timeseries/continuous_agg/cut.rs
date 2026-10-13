// SPDX-License-Identifier: BUSL-1.1

//! Refresh cuts: the replicated refresh points of continuous aggregates.
//!
//! A cut is an entry in the root collection's data-group log. Every replica
//! applies it in log order. At apply the replica flushes the root's
//! memtable, then records the partitions present at that moment that the
//! aggregate has not folded. The aggregate folds exactly those, on the
//! maintenance tick. Every row before the cut is in one of them or already
//! folded, and no row after the cut is. Partition names differ between
//! replicas, but the rows do not, so every replica reaches the same buckets.
//!
//! Retention can remove a partition that a `Manual` or `Periodic`
//! aggregate has not folded and that no pending cut names. Its rows are all
//! after the last cut. They fold into a parked delta, which merges into the
//! buckets at the next cut: the cut where every other replica folds the
//! same rows.

use std::collections::{HashSet, VecDeque};

use nodedb_physical::physical_plan::RefreshCut;

use super::definition::RefreshPolicy;
use super::manager::{AggKey, ContinuousAggregateManager};
use super::partial::ColumnLayout;
use super::refresh::{self, Buckets};

/// Completed cut ids each aggregate keeps for waiters.
pub const COMPLETED_CUTS_KEPT: usize = 256;

/// Refresh-cut state of one aggregate.
#[derive(Debug)]
pub struct CutState {
    /// Ids of the cuts applied and not yet fully folded, in log order.
    pub pending_ids: Vec<u64>,
    /// Partitions the pending cuts still fold.
    pub pending: HashSet<String>,
    /// Ids of the most recently completed cuts, oldest first.
    pub completed: VecDeque<u64>,
    /// Highest periodic slot applied. `0` when none was.
    pub last_slot: u64,
    /// Fold of the partitions retention removed after the last cut.
    pub parked: Buckets,
    /// Rows in `parked`.
    pub parked_rows: u64,
    /// Highest source timestamp in `parked`.
    pub parked_max_ts: i64,
}

impl Default for CutState {
    fn default() -> Self {
        Self {
            pending_ids: Vec::new(),
            pending: HashSet::new(),
            completed: VecDeque::new(),
            last_slot: 0,
            parked: Buckets::new(),
            parked_rows: 0,
            parked_max_ts: i64::MIN,
        }
    }
}

impl CutState {
    /// Whether this state carries nothing worth persisting.
    pub fn is_empty(&self) -> bool {
        self.pending_ids.is_empty()
            && self.completed.is_empty()
            && self.last_slot == 0
            && self.parked.is_empty()
    }

    /// Whether cut `refresh_id` (or, for a periodic cut, its slot) was
    /// already applied here.
    fn applied(&self, cut: &RefreshCut) -> bool {
        if cut.periodic {
            return cut.refresh_id <= self.last_slot;
        }
        self.pending_ids.contains(&cut.refresh_id) || self.completed.contains(&cut.refresh_id)
    }

    pub(super) fn complete_pending(&mut self) {
        self.pending.clear();
        for id in self.pending_ids.drain(..) {
            self.completed.push_back(id);
        }
        while self.completed.len() > COMPLETED_CUTS_KEPT {
            self.completed.pop_front();
        }
    }
}

/// A cut for an aggregate not registered on this core when it applied, or
/// registered with an older incarnation. It applies when the aggregate of
/// that incarnation registers and loads.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct OrphanCut {
    pub cut: RefreshCut,
    /// Partitions of the root present when the cut applied.
    pub partitions: Vec<String>,
}

/// What applying a cut did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutOutcome {
    /// The cut is recorded, pending or already complete.
    Applied,
    /// The same cut, or a periodic slot not above the last, applied before.
    Duplicate,
    /// The aggregate is not registered here at the cut's incarnation yet.
    /// The cut waits for it.
    Orphaned,
    /// The cut names an incarnation older than the registered aggregate.
    Superseded,
}

/// Where cut `refresh_id` of one aggregate stands on this core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutStatus {
    /// Fully folded.
    Complete,
    /// Applied and still folding, or not applied here yet.
    Waiting,
    /// The aggregate is stale: it takes no fold, so the cut never completes.
    Stale,
}

/// Whether `policy` folds only at cuts.
pub fn folds_at_cuts(policy: &RefreshPolicy) -> bool {
    !matches!(policy, RefreshPolicy::OnFlush | RefreshPolicy::OnSeal)
}

impl ContinuousAggregateManager {
    /// Apply `cut` to its aggregate, rooted at `root`. `live` lists every
    /// partition of `root` present now, after the cut's flush.
    pub fn apply_cut(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        cut: &RefreshCut,
        live: &[String],
    ) -> CutOutcome {
        let key = (database_id, tenant_id, cut.aggregate.clone());
        let registered = self.definitions.get(&key).map(|def| def.incarnation);
        let loaded = self.loaded.contains(&key);
        match registered {
            Some(incarnation) if incarnation > cut.incarnation => return CutOutcome::Superseded,
            // An unloaded aggregate's state is about to be replaced by its
            // image, so the cut waits for the load like an orphan.
            Some(incarnation) if incarnation == cut.incarnation && loaded => {}
            _ => {
                self.orphan_cut(database_id, tenant_id, root, cut, live);
                return CutOutcome::Orphaned;
            }
        }
        let state = self.cuts.entry(key.clone()).or_default();
        if state.applied(cut) {
            return CutOutcome::Duplicate;
        }
        if cut.periodic {
            state.last_slot = cut.refresh_id;
        }
        self.merge_parked(&key);
        let folded = self.folded.get(&key);
        let unfolded: Vec<String> = live
            .iter()
            .filter(|p| !folded.is_some_and(|set| set.contains(*p)))
            .cloned()
            .collect();
        let state = self.cuts.entry(key.clone()).or_default();
        state.pending.extend(unfolded);
        state.pending_ids.push(cut.refresh_id);
        if state.pending.is_empty() {
            state.complete_pending();
        }
        self.dirty_images
            .insert((database_id, tenant_id, root.to_string()));
        CutOutcome::Applied
    }

    fn orphan_cut(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        cut: &RefreshCut,
        live: &[String],
    ) {
        let root_key = (database_id, tenant_id, root.to_string());
        let orphans = self.orphan_cuts.entry(root_key.clone()).or_default();
        if orphans.iter().any(|o| o.cut == *cut) {
            return;
        }
        orphans.push(OrphanCut {
            cut: cut.clone(),
            partitions: live.to_vec(),
        });
        self.dirty_images.insert(root_key);
    }

    /// Apply every orphan cut of `root` whose aggregate is now registered
    /// and loaded at the cut's incarnation, in the order the cuts applied.
    /// Orphans of an incarnation older than the registered one are dropped.
    pub(super) fn adopt_orphan_cuts(&mut self, database_id: u64, tenant_id: u64, root: &str) {
        let root_key = (database_id, tenant_id, root.to_string());
        let Some(orphans) = self.orphan_cuts.remove(&root_key) else {
            return;
        };
        let mut waiting = Vec::new();
        for orphan in orphans {
            let key = (database_id, tenant_id, orphan.cut.aggregate.clone());
            let ready = self
                .definitions
                .get(&key)
                .is_some_and(|def| def.incarnation >= orphan.cut.incarnation)
                && self.loaded.contains(&key);
            if ready {
                self.apply_cut(
                    database_id,
                    tenant_id,
                    root,
                    &orphan.cut,
                    &orphan.partitions,
                );
            } else {
                waiting.push(orphan);
            }
        }
        if !waiting.is_empty() {
            self.orphan_cuts.insert(root_key.clone(), waiting);
        }
        self.dirty_images.insert(root_key);
    }

    /// Drop the orphan cuts of `name` up to `incarnation`: that aggregate
    /// was dropped.
    pub(super) fn drop_orphan_cuts(&mut self, key: &AggKey, incarnation: nodedb_types::Hlc) {
        for (root, orphans) in self.orphan_cuts.iter_mut() {
            if root.0 == key.0 && root.1 == key.1 {
                orphans.retain(|o| o.cut.aggregate != key.2 || o.cut.incarnation > incarnation);
            }
        }
        self.orphan_cuts.retain(|_, orphans| !orphans.is_empty());
    }

    /// Merge the parked delta of `key` into its buckets.
    fn merge_parked(&mut self, key: &AggKey) {
        let Some(def) = self.definitions.get(key) else {
            return;
        };
        let Some(state) = self.cuts.get_mut(key) else {
            return;
        };
        if state.parked.is_empty() && state.parked_rows == 0 {
            return;
        }
        let parked = std::mem::take(&mut state.parked);
        let rows = std::mem::take(&mut state.parked_rows);
        let max_ts = std::mem::replace(&mut state.parked_max_ts, i64::MIN);
        let layout = ColumnLayout::of(def);
        refresh::merge_delta(
            self.materialized.entry(key.clone()).or_default(),
            &parked,
            &layout,
        );
        let last = self.watermarks.get(key).map_or(0, |wm| wm.last_refresh_ms);
        self.watermarks
            .entry(key.clone())
            .or_default()
            .advance(max_ts, rows, last);
    }

    /// Add a removed partition's delta to `key`'s parked delta.
    pub(super) fn park_delta(&mut self, key: &AggKey, delta: &Buckets, rows: u64, max_ts: i64) {
        let Some(def) = self.definitions.get(key) else {
            return;
        };
        let layout = ColumnLayout::of(def);
        let state = self.cuts.entry(key.clone()).or_default();
        refresh::merge_delta(&mut state.parked, delta, &layout);
        state.parked_rows += rows;
        state.parked_max_ts = state.parked_max_ts.max(max_ts);
    }

    /// Whether a pending cut of `key` folds `partition`.
    pub(super) fn cut_folds(&self, key: &AggKey, partition: &str) -> bool {
        self.cuts
            .get(key)
            .is_some_and(|state| state.pending.contains(partition))
    }

    /// Every loaded, non-stale aggregate rooted at `root` folded every
    /// partition its pending cuts name that is still present: complete
    /// those cuts.
    pub(super) fn complete_cuts(&mut self, database_id: u64, tenant_id: u64, root: &str) {
        let mut completed = false;
        for name in self.chain(database_id, tenant_id, root) {
            let key = (database_id, tenant_id, name);
            let folding = self
                .definitions
                .get(&key)
                .is_some_and(|def| !def.stale && self.loaded.contains(&key));
            if !folding {
                continue;
            }
            if let Some(state) = self.cuts.get_mut(&key)
                && !state.pending_ids.is_empty()
            {
                state.complete_pending();
                completed = true;
            }
        }
        if completed {
            self.dirty_images
                .insert((database_id, tenant_id, root.to_string()));
        }
    }

    /// Where cut `refresh_id` of `name` stands on this core.
    pub fn cut_status(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        refresh_id: u64,
    ) -> CutStatus {
        let key = (database_id, tenant_id, name.to_string());
        if self.definitions.get(&key).is_some_and(|def| def.stale) {
            return CutStatus::Stale;
        }
        let done = self
            .cuts
            .get(&key)
            .is_some_and(|state| state.completed.contains(&refresh_id));
        if done {
            CutStatus::Complete
        } else {
            CutStatus::Waiting
        }
    }

    /// Partitions the pending cuts of `name` still fold.
    pub fn pending_cut_partitions(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> HashSet<String> {
        self.cuts
            .get(&(database_id, tenant_id, name.to_string()))
            .map(|state| state.pending.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::fold::{FoldScope, FoldTarget};
    use crate::engine::timeseries::continuous_agg::refresh::RefreshSource;
    use crate::engine::timeseries::continuous_agg::test_support::{
        T0, ad_hoc, exact_def, exact_rows, int_drain, loaded_manager, materialized,
    };

    fn manual(name: &str) -> crate::engine::timeseries::continuous_agg::ContinuousAggregateDef {
        let mut def = exact_def(name, "metrics", "1m");
        def.refresh_policy = RefreshPolicy::Manual;
        def
    }

    fn cut(name: &str, refresh_id: u64) -> RefreshCut {
        RefreshCut {
            aggregate: name.into(),
            incarnation: exact_def(name, "metrics", "1m").incarnation,
            refresh_id,
            periodic: false,
        }
    }

    fn fold(mgr: &mut ContinuousAggregateManager, partition: &str, rows: &[(i64, i64)]) {
        let drain = int_drain(rows);
        mgr.fold_into_chain(
            FoldTarget {
                database_id: 0,
                tenant_id: 1,
                root: "metrics",
                partition,
            },
            &RefreshSource::of_drain(&drain),
            FoldScope::Tick,
            T0,
        );
    }

    /// Two replicas flush at different moments, so their partition names
    /// and splits differ. The same cut still gives the same buckets.
    #[test]
    fn same_cut_on_two_replicas_gives_same_rows() {
        let rows = exact_rows();
        let (before, after) = rows.split_at(5);

        let mut a = loaded_manager(vec![manual("m")]);
        a.on_flush(0, 1, "metrics", "a1", &int_drain(&before[..2]), T0);
        a.on_flush(0, 1, "metrics", "a2", &int_drain(&before[2..]), T0);
        assert_eq!(
            a.apply_cut(0, 1, "metrics", &cut("m", 7), &["a1".into(), "a2".into()]),
            CutOutcome::Applied
        );
        a.on_flush(0, 1, "metrics", "a3", &int_drain(after), T0);
        fold(&mut a, "a1", &before[..2]);
        fold(&mut a, "a2", &before[2..]);
        fold(&mut a, "a3", after);
        a.complete_cuts(0, 1, "metrics");

        let mut b = loaded_manager(vec![manual("m")]);
        b.on_flush(0, 1, "metrics", "b1", &int_drain(before), T0);
        b.apply_cut(0, 1, "metrics", &cut("m", 7), &["b1".into()]);
        fold(&mut b, "b1", before);
        b.complete_cuts(0, 1, "metrics");

        assert_eq!(materialized(&a, "m"), ad_hoc(before, 60_000));
        assert_eq!(materialized(&a, "m"), materialized(&b, "m"));
        assert_eq!(a.cut_status(0, 1, "m", 7), CutStatus::Complete);
        assert_eq!(b.cut_status(0, 1, "m", 7), CutStatus::Complete);
    }

    #[test]
    fn a_cut_applies_once() {
        let mut mgr = loaded_manager(vec![manual("m")]);
        let live = vec!["p1".to_string()];
        assert_eq!(
            mgr.apply_cut(0, 1, "metrics", &cut("m", 1), &live),
            CutOutcome::Applied
        );
        assert_eq!(
            mgr.apply_cut(0, 1, "metrics", &cut("m", 1), &live),
            CutOutcome::Duplicate
        );
        assert_eq!(mgr.cut_status(0, 1, "m", 1), CutStatus::Waiting);
    }

    #[test]
    fn periodic_slots_apply_in_order_only() {
        let mut mgr = loaded_manager(vec![manual("m")]);
        let slot = |n| RefreshCut {
            periodic: true,
            ..cut("m", n)
        };
        assert_eq!(
            mgr.apply_cut(0, 1, "metrics", &slot(5), &[]),
            CutOutcome::Applied
        );
        assert_eq!(
            mgr.apply_cut(0, 1, "metrics", &slot(4), &[]),
            CutOutcome::Duplicate
        );
        assert_eq!(
            mgr.apply_cut(0, 1, "metrics", &slot(5), &[]),
            CutOutcome::Duplicate
        );
        assert_eq!(mgr.cut_status(0, 1, "m", 5), CutStatus::Complete);
    }

    /// A partition retention removes after the last cut parks its rows.
    /// They reach the buckets at the next cut, not before.
    #[test]
    fn retention_after_a_cut_parks_until_the_next_cut() {
        let rows = exact_rows();
        let mut mgr = loaded_manager(vec![manual("m")]);
        let drain = int_drain(&rows);
        mgr.fold_into_chain(
            FoldTarget {
                database_id: 0,
                tenant_id: 1,
                root: "metrics",
                partition: "old",
            },
            &RefreshSource::of_drain(&drain),
            FoldScope::Expiring,
            T0,
        );
        assert!(materialized(&mgr, "m").is_empty(), "parked rows stay out");
        mgr.apply_cut(0, 1, "metrics", &cut("m", 1), &[]);
        assert_eq!(materialized(&mgr, "m"), ad_hoc(&rows, 60_000));
        assert_eq!(mgr.cut_status(0, 1, "m", 1), CutStatus::Complete);
    }

    #[test]
    fn a_cut_before_registration_waits_for_its_aggregate() {
        let rows = exact_rows();
        let mut mgr = loaded_manager(vec![]);
        assert_eq!(
            mgr.apply_cut(0, 1, "metrics", &cut("m", 3), &["p1".into()]),
            CutOutcome::Orphaned
        );
        mgr.register(1, manual("m"));
        mgr.install_image(0, 1, "metrics", Default::default());
        assert_eq!(
            mgr.pending_cut_partitions(0, 1, "m"),
            HashSet::from(["p1".into()])
        );
        fold(&mut mgr, "p1", &rows);
        mgr.complete_cuts(0, 1, "metrics");
        assert_eq!(mgr.cut_status(0, 1, "m", 3), CutStatus::Complete);
        assert_eq!(materialized(&mgr, "m"), ad_hoc(&rows, 60_000));
    }

    #[test]
    fn a_cut_for_a_dropped_incarnation_is_superseded() {
        let mut recreated = manual("m");
        recreated.incarnation = nodedb_types::Hlc::new(99, 0);
        let mut mgr = loaded_manager(vec![recreated]);
        assert_eq!(
            mgr.apply_cut(0, 1, "metrics", &cut("m", 1), &["p1".into()]),
            CutOutcome::Superseded
        );
        assert!(mgr.pending_cut_partitions(0, 1, "m").is_empty());
    }
}
