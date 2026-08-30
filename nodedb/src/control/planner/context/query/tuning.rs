// SPDX-License-Identifier: BUSL-1.1

//! Per-request tuning knobs on [`QueryContext`] — vector dim caps, forced
//! shuffle join/aggregate overrides, and cost-model thresholds.

use super::context::QueryContext;

/// Default Gather-vs-shuffle aggregate threshold, in distinct-group units.
///
/// A GROUP BY whose estimated group cardinality exceeds this many distinct
/// groups is auto-shuffled (the coordinator Gather-merge of that many partial
/// rows is the bottleneck); below it, the aggregate stays on the cheaper Gather
/// path. Used when no `SharedState` tuning is available (legacy `new()` /
/// `with_catalog()` fixtures) and as the effective value when the session var
/// `nodedb.shuffle_agg_threshold` is unset.
pub const DEFAULT_SHUFFLE_AGG_THRESHOLD: usize = 10_000;

impl QueryContext {
    /// Update the per-tenant vector dimension cap for the next plan call.
    ///
    /// Called by connection handlers after resolving the tenant's quota from
    /// `TenantIsolation`. Using an atomic allows `&self` (no exclusive borrow
    /// needed since handlers do not pipeline concurrent plan calls on one
    /// connection). Relaxed ordering is sufficient: this value is written
    /// before the planning call begins and read only within that same call.
    pub fn set_max_vector_dim(&self, dim: u32) {
        self.max_vector_dim
            .store(dim, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the force-shuffle-join override for the next plan call.
    ///
    /// Called by connection handlers after reading the session var
    /// `nodedb.force_shuffle_join` (and `nodedb.shuffle_num_parts`). `num_parts
    /// == 0` means "unset — the emit defaults to the cluster data-node count".
    /// Relaxed ordering suffices: written before planning begins, read only
    /// within that same call (same contract as `set_max_vector_dim`).
    pub fn set_force_shuffle_join(&self, force: bool, num_parts: u32) {
        self.force_shuffle_join
            .store(force, std::sync::atomic::Ordering::Relaxed);
        self.shuffle_num_parts
            .store(num_parts, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the force-shuffle-aggregate override for the next plan call.
    ///
    /// Called by connection handlers after reading the session var
    /// `nodedb.force_shuffle_agg` (and `nodedb.shuffle_agg_num_parts`).
    /// `num_parts == 0` means "unset — the emit defaults to the cluster
    /// data-node count". Relaxed ordering suffices: written before planning
    /// begins, read only within that same call (same contract as
    /// `set_force_shuffle_join`).
    pub fn set_force_shuffle_agg(&self, force: bool, num_parts: u32) {
        self.force_shuffle_agg
            .store(force, std::sync::atomic::Ordering::Relaxed);
        self.shuffle_agg_num_parts
            .store(num_parts, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the broadcast-vs-shuffle cost threshold (bytes) for the next plan
    /// call.
    ///
    /// Called by connection handlers with the effective value — the session
    /// override `nodedb.broadcast_threshold_bytes` when set, otherwise the
    /// node's configured `[tuning.cluster_transport] broadcast_threshold_bytes`.
    /// Passing the resolved value (rather than only the override) means a
    /// session that sets and later unsets the knob correctly reverts to the
    /// tuning default. Relaxed ordering suffices: written before planning
    /// begins, read only within that same call (same contract as
    /// `set_max_vector_dim`).
    pub fn set_broadcast_threshold_bytes(&self, bytes: usize) {
        self.broadcast_threshold_bytes
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the Gather-vs-shuffle aggregate cost threshold (distinct-group count)
    /// for the next plan call.
    ///
    /// Called by connection handlers with the effective value — the session
    /// override `nodedb.shuffle_agg_threshold` when set, otherwise
    /// [`DEFAULT_SHUFFLE_AGG_THRESHOLD`]. Passing the resolved value (rather than
    /// only the override) means a session that sets and later unsets the knob
    /// correctly reverts to the default. Relaxed ordering suffices: written
    /// before planning begins, read only within that same call (same contract as
    /// `set_broadcast_threshold_bytes`).
    pub fn set_shuffle_agg_threshold(&self, groups: usize) {
        self.shuffle_agg_threshold
            .store(groups, std::sync::atomic::Ordering::Relaxed);
    }

    /// The node's default broadcast threshold, used when no `SharedState` tuning
    /// is available (legacy `new()` / `with_catalog()` fixtures). Mirrors
    /// `ClusterTransportTuning::default().broadcast_threshold_bytes`.
    pub fn default_broadcast_threshold(&self) -> usize {
        self.broadcast_threshold_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Override the default rounding mode for `ROUND()`.
    ///
    /// No-op: rounding mode is handled at execution time, not planning.
    pub fn set_rounding_mode(&self, _mode: &str) {}
}
