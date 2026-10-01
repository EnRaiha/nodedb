// SPDX-License-Identifier: BUSL-1.1

//! Compaction statistics returned by `run_compaction` and reported back to
//! the caller of `PhysicalPlan::Compact`.
//!
//! Each field tracks one observable outcome of a compaction cycle so that
//! operators can distinguish work-done vs deferred-by-budget vs failed
//! without parsing log lines.

/// Statistics from a compaction cycle.
#[derive(
    Debug,
    Clone,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CompactionStats {
    /// Number of tombstoned vectors removed across all collections.
    pub vectors_compacted: usize,
    /// Number of collections that had tombstones compacted.
    pub collections_compacted: usize,
    /// Whether CSR write buffers were compacted.
    pub csr_compacted: bool,
    /// Number of L1 segments selected for merge compaction.
    pub segments_merged: usize,

    /// Vector collections skipped because their database was over its
    /// per-minute maintenance CPU budget.
    pub vectors_deferred: usize,
    /// Whether CSR compaction was skipped due to budget exhaustion.
    pub csr_deferred: bool,
    /// `(tenant, collection)` pairs whose L1 segment compaction was skipped
    /// due to budget exhaustion.
    pub segments_deferred: usize,

    /// Number of FTS LSM segments merged across all collections this cycle.
    pub fts_compacted: u64,
    /// `(tenant, collection)` pairs whose FTS LSM compaction was deferred —
    /// either because the owning database was over its maintenance CPU
    /// budget, or because a transient backend/commit failure aborted the
    /// merge. Both cases are retried on the next maintenance pass.
    pub fts_deferred: u64,
    /// `true` iff the FTS subsystem could not be enumerated this cycle
    /// (e.g. the segments table read txn failed). Distinct from
    /// `fts_deferred`, which counts per-collection deferrals; this flag
    /// signals that the cycle observed *zero* FTS state because the
    /// enumeration itself failed, not because no work was needed.
    pub fts_enumeration_failed: bool,
}
