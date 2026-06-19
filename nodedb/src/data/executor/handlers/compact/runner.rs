// SPDX-License-Identifier: BUSL-1.1

//! Top-level compaction orchestration.
//!
//! `run_compaction` drives all five compaction phases (vector, CSR, dangling
//! edges, timeseries L1, FTS LSM) and aggregates the results into a
//! `CompactionStats`. `execute_compact` is the entry point dispatched from
//! the Control Plane via `PhysicalPlan::Compact`.

use tracing::info;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::DatabaseId;

use super::budget::BudgetGate;
use super::stats::CompactionStats;

impl CoreLoop {
    /// Execute an on-demand compaction request.
    ///
    /// Forces compaction of all vector collections (regardless of tombstone
    /// ratio), CSR compaction, and dangling edge sweep. Returns a summary
    /// payload with compaction statistics.
    pub(in crate::data::executor) fn execute_compact(&mut self, task: &ExecutionTask) -> Response {
        let result = self.run_compaction(true);
        let payload = match zerompk::to_msgpack_vec(&result) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "failed to encode compaction stats");
                Vec::new()
            }
        };
        self.response_with_payload(task, payload)
    }

    /// Run all maintenance/compaction tasks.
    ///
    /// Called periodically from the runtime event loop (idle maintenance)
    /// and on-demand via `PhysicalPlan::Compact`.
    ///
    /// When `force` is false (periodic), only compacts collections whose
    /// tombstone ratio exceeds the threshold. When `force` is true
    /// (on-demand), compacts everything.
    ///
    /// Per-database CPU budget is enforced when a budget tracker is installed.
    /// Collections whose owning database has exhausted its per-minute budget
    /// are deferred; this only affects periodic maintenance, not forced compaction.
    pub fn run_compaction(&mut self, force: bool) -> CompactionStats {
        let mut stats = CompactionStats::default();

        // 1. Vector compaction: remove tombstoned nodes from HNSW indexes.
        // Collect keys first to avoid borrow conflict on `self`.
        let vector_keys: Vec<_> = self.vector_collections.keys().cloned().collect();
        for key in vector_keys {
            let db = key.0;

            // Budget gate. The lease, if any, MUST be bound to a `let` so it
            // lives across `collection.compact()` below — its `Drop` impl is
            // what records actual elapsed CPU into the per-db window.
            let _lease = match self.acquire_maintenance_lease(db, force) {
                BudgetGate::Granted(lease) => lease,
                BudgetGate::Deferred => {
                    stats.vectors_deferred += 1;
                    tracing::debug!(
                        core = self.core_id,
                        db = db.as_u64(),
                        collection = &key.2,
                        "vector compaction deferred: database over maintenance budget"
                    );
                    continue;
                }
            };

            let collection = match self.vector_collections.get_mut(&key) {
                Some(c) => c,
                None => continue,
            };

            let total_tombstones: usize = collection
                .sealed_segments()
                .iter()
                .map(|seg| seg.index.tombstone_count())
                .sum();
            let total_nodes: usize = collection
                .sealed_segments()
                .iter()
                .map(|seg| seg.index.len())
                .sum();

            if total_tombstones == 0 {
                continue;
            }

            let ratio = if total_nodes > 0 {
                total_tombstones as f64 / total_nodes as f64
            } else {
                0.0
            };

            if !force && ratio < self.compaction_tombstone_threshold {
                continue;
            }

            let removed = collection.compact();
            if removed > 0 {
                info!(
                    core = self.core_id,
                    collection = &key.2,
                    removed,
                    ratio = format!("{ratio:.2}"),
                    "vector compaction: tombstones removed"
                );
                stats.vectors_compacted += removed;
                stats.collections_compacted += 1;
            }
            // `_lease` drops here, recording elapsed wall-clock into the budget window.
        }

        // 2. CSR compaction: merge write buffers into dense arrays.
        // CSR is not per-database keyed; use DEFAULT as the budget scope.
        let csr_db = DatabaseId::DEFAULT;
        match self.acquire_maintenance_lease(csr_db, force) {
            BudgetGate::Granted(_lease) => {
                match self.csr.compact_all() {
                    Ok(()) => stats.csr_compacted = true,
                    Err(e) => tracing::warn!(
                        error = %e,
                        "CSR compaction rejected by memory governor; skipping"
                    ),
                }
                // _lease drops here, recording elapsed wall-clock into the budget window.
            }
            BudgetGate::Deferred => {
                stats.csr_deferred = true;
                tracing::debug!(
                    core = self.core_id,
                    db = csr_db.as_u64(),
                    "CSR compaction deferred: database over maintenance budget"
                );
            }
        }

        // 3. Dangling edge sweep — budget-gated against DEFAULT (sweep is
        // process-wide; per-tenant attribution happens inside the sweep loop).
        let edges_db = DatabaseId::DEFAULT;
        match self.acquire_maintenance_lease(edges_db, force) {
            BudgetGate::Granted(_lease) => {
                stats.edges_swept = self.sweep_dangling_edges();
                // _lease drops here, recording elapsed wall-clock into the budget window.
            }
            BudgetGate::Deferred => {
                stats.edges_deferred = true;
                tracing::debug!(
                    core = self.core_id,
                    db = edges_db.as_u64(),
                    "edge sweep deferred: database over maintenance budget"
                );
            }
        }

        // 4. L1 segment compaction: per-(tenant, collection) → per-database gated.
        let (merged, deferred) = self.run_segment_compaction(force);
        stats.segments_merged = merged;
        stats.segments_deferred = deferred;

        // 5. FTS LSM level compaction: merge L0→L1→… segments per collection.
        let fts_outcome = self.run_fts_compaction(force);
        stats.fts_compacted = fts_outcome.merged;
        stats.fts_deferred = fts_outcome.deferred;
        stats.fts_enumeration_failed = fts_outcome.enumeration_failed;

        if stats.vectors_compacted > 0
            || stats.edges_swept > 0
            || stats.segments_merged > 0
            || stats.fts_compacted > 0
            || stats.vectors_deferred > 0
            || stats.csr_deferred
            || stats.edges_deferred
            || stats.segments_deferred > 0
            || stats.fts_deferred > 0
            || stats.fts_enumeration_failed
        {
            info!(
                core = self.core_id,
                vectors_compacted = stats.vectors_compacted,
                collections_compacted = stats.collections_compacted,
                edges_swept = stats.edges_swept,
                segments_merged = stats.segments_merged,
                fts_compacted = stats.fts_compacted,
                vectors_deferred = stats.vectors_deferred,
                csr_deferred = stats.csr_deferred,
                edges_deferred = stats.edges_deferred,
                segments_deferred = stats.segments_deferred,
                fts_deferred = stats.fts_deferred,
                fts_enumeration_failed = stats.fts_enumeration_failed,
                "compaction cycle complete"
            );
        }

        stats
    }
}
