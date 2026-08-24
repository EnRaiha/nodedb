// SPDX-License-Identifier: BUSL-1.1

//! Applied-index durability and log compaction.
//!
//! Two watermarks are tracked separately and must not be confused:
//! `last_applied` is the DELIVERY watermark, advanced as entries are handed
//! to the state machine; `durable_applied` is the DURABILITY floor, advanced
//! only once those effects are fsynced. Compaction gates on the floor, never
//! on delivery — discarding an entry whose redo record is not yet durable
//! destroys the only source that can rebuild the memory-only engines.

use crate::error::{RaftError, Result};
use crate::node::core::RaftNode;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Advance `last_applied` after the caller has applied entries.
    ///
    /// This is the DELIVERY watermark: it advances as entries are handed to
    /// the state machine, before their effects are necessarily durable. Use
    /// [`Self::save_durable_applied_index`] for the durability floor.
    pub fn advance_applied(&mut self, applied_to: u64) {
        self.volatile.last_applied = applied_to;
    }

    /// Highest log index whose apply is durable on this node.
    pub fn durable_applied_index(&self) -> u64 {
        self.durable_applied
    }

    /// The lowest log index still available in the retained (post-compaction)
    /// log — `snapshot_index + 1`. A committed-entry read below this yields
    /// [`RaftError::LogCompacted`]. Used to arm a Calvin scheduler catch-up from
    /// the earliest replayable index so the drain never faults on a compacted
    /// range.
    pub fn first_available_index(&self) -> u64 {
        self.log.snapshot_index() + 1
    }

    /// Persist `index` as the durable applied floor.
    ///
    /// The caller MUST only pass an index whose state-machine effects are
    /// already durable — for data groups, an index whose redo record the WAL
    /// has fsynced. The next boot resumes delivery at `index + 1`, so an index
    /// saved ahead of durability silently drops the entries in between.
    ///
    /// Monotonic: an `index` at or below the current floor is a no-op, so an
    /// out-of-order or retrying caller can never move the floor backwards and
    /// re-expose an entry to a second apply.
    pub fn save_durable_applied_index(&mut self, index: u64) -> Result<()> {
        if index <= self.durable_applied {
            return Ok(());
        }
        self.log.storage_mut().save_applied_index(index)?;
        self.durable_applied = index;
        Ok(())
    }

    /// Auto-compaction threshold: entries retained past `snapshot_index`
    /// before the log is compacted. `None` disables auto-compaction.
    pub fn log_compaction_threshold(&self) -> Option<u64> {
        self.config.log_compaction_threshold
    }

    /// Compact the log up to `up_to_index` after the DATA-PLANE state
    /// machine has durably applied every entry `<= up_to_index`.
    ///
    /// Resolves the term at `up_to_index` from the in-memory log and
    /// calls [`RaftLog::apply_snapshot`], which discards entries
    /// `<= up_to_index` and persists the new snapshot boundary. The
    /// snapshot bytes themselves are NOT materialized here — the
    /// `SnapshotBuilder` hook rebuilds them on demand from live engine
    /// state when a lagging follower needs an `InstallSnapshot`.
    ///
    /// # Safety / gating
    ///
    /// The CALLER MUST pass an `up_to_index` that the DATA-PLANE state
    /// machine has durably applied. Compacting past a data-plane-unapplied
    /// index would let the `SnapshotBuilder` serialize incomplete state.
    /// The sole caller path (`run_apply_loop` → [`Self::maybe_compact_log`])
    /// guarantees this: it only compacts an index after the SPSC round-trip
    /// that applies that entry to the Data Plane has returned.
    ///
    /// This method additionally clamps to the DURABLE applied index
    /// (returning [`RaftError::CompactionAheadOfApplied`] otherwise).
    /// Deliberately not `volatile.last_applied`: that advances at
    /// commit/enqueue time, so clamping to it would let compaction discard
    /// entries whose redo record is not yet fsynced — losing the only recovery
    /// source for the memory-only engines.
    ///
    /// Returns `Ok(false)` when there is nothing to compact
    /// (`up_to_index <= snapshot_index`). Returns
    /// `Err(RaftError::LogCompacted)` if the term at `up_to_index` is no
    /// longer available (already compacted away).
    pub fn compact_log_up_to(&mut self, up_to_index: u64) -> Result<bool> {
        if up_to_index <= self.log.snapshot_index() {
            return Ok(false);
        }
        if up_to_index > self.durable_applied {
            return Err(RaftError::CompactionAheadOfApplied {
                requested: up_to_index,
                last_applied: self.durable_applied,
            });
        }
        let term = self
            .log
            .term_at(up_to_index)
            .ok_or(RaftError::LogCompacted {
                requested: up_to_index,
                first_available: self.log.snapshot_index() + 1,
            })?;
        self.log.apply_snapshot(up_to_index, term);
        Ok(true)
    }

    /// Check the configured auto-compaction threshold against the
    /// data-plane applied index `applied_index` and compact the log up to
    /// `applied_index` if the retained-entry count has reached the
    /// threshold.
    ///
    /// `applied_index` is the index the DATA-PLANE state machine has
    /// durably applied up to (NOT raft's commit index) — see
    /// [`RaftConfig::log_compaction_threshold`]. No-op when the threshold
    /// is `None` or the retained count is below it.
    ///
    /// Returns `Ok(true)` when a compaction was performed.
    pub fn maybe_compact_log(&mut self, applied_index: u64) -> Result<bool> {
        let Some(threshold) = self.config.log_compaction_threshold else {
            return Ok(false);
        };
        let snapshot_index = self.log.snapshot_index();
        if applied_index <= snapshot_index {
            return Ok(false);
        }
        if applied_index - snapshot_index < threshold {
            return Ok(false);
        }
        // Never compact past an entry whose apply is not yet durable.
        let up_to = applied_index.min(self.durable_applied);
        self.compact_log_up_to(up_to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;
    use crate::test_support::{apply_durably, leader_with_applied_noop, test_config};

    #[test]
    fn threshold_some_compacts_after_enough_applied() {
        // Single-voter group so every propose commits immediately.
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(4);
        let mut node = leader_with_applied_noop(cfg);

        // Propose entries and apply each as the data plane would.
        for _ in 0..8 {
            let idx = node.propose(b"write".to_vec()).unwrap();
            let _ = node.take_ready();
            apply_durably(&mut node, idx);

            // Trigger gated on the data-plane applied watermark (= idx here).
            node.maybe_compact_log(idx).unwrap();
        }

        let snap = node.log_snapshot_index();
        // With threshold 4, the log keeps at most 4 entries past the
        // snapshot boundary; the boundary must have advanced.
        assert!(
            snap > 0,
            "snapshot_index should have advanced past 0, got {snap}"
        );
        assert!(
            node.last_log_index() - snap <= 4,
            "retained entries ({}) must be <= threshold (4)",
            node.last_log_index() - snap
        );

        // Entries at or before the snapshot boundary are discarded.
        assert!(
            node.log.entry_at(snap).is_none(),
            "entry at snapshot boundary must be gone"
        );
        assert!(
            node.log.entries_range(1, snap).is_err(),
            "range into compacted region must fail"
        );
    }

    #[test]
    fn threshold_none_never_compacts() {
        let cfg = test_config(1, vec![]); // log_compaction_threshold: None
        let mut node = leader_with_applied_noop(cfg);

        for _ in 0..12 {
            let idx = node.propose(b"write".to_vec()).unwrap();
            let _ = node.take_ready();
            apply_durably(&mut node, idx);
            // No-op: threshold is None.
            assert!(!node.maybe_compact_log(idx).unwrap());
        }

        assert_eq!(
            node.log_snapshot_index(),
            0,
            "no compaction must occur when threshold is None"
        );
        // Every entry from index 1 is still present.
        assert!(node.log.entry_at(1).is_some());
        assert!(node.log.entries_range(1, node.last_log_index()).is_ok());
    }

    #[test]
    fn compact_log_up_to_rejects_ahead_of_applied() {
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(2);
        let mut node = leader_with_applied_noop(cfg);

        let idx = node.propose(b"write".to_vec()).unwrap();
        let _ = node.take_ready();
        // Deliberately do NOT apply past the noop — the data plane has not
        // applied `idx` yet.
        let err = node.compact_log_up_to(idx).unwrap_err();
        assert!(matches!(err, RaftError::CompactionAheadOfApplied { .. }));
    }

    /// Compaction gates on the DURABLE applied floor, not the delivery
    /// watermark. An entry that has been handed to the state machine but whose
    /// redo is not yet fsynced must NOT be compacted away: the log is the only
    /// thing that can rebuild the memory-only engines for it.
    #[test]
    fn compact_log_up_to_rejects_delivered_but_not_durable() {
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(2);
        let mut node = leader_with_applied_noop(cfg);

        let idx = node.propose(b"write".to_vec()).unwrap();
        let _ = node.take_ready();
        // Delivery watermark advances; the durable floor does not.
        node.advance_applied(idx);

        let err = node.compact_log_up_to(idx).unwrap_err();
        assert!(matches!(err, RaftError::CompactionAheadOfApplied { .. }));

        // Once the apply is durable the same index compacts.
        node.save_durable_applied_index(idx).unwrap();
        assert!(node.compact_log_up_to(idx).unwrap());
    }

    /// The durable floor never moves backwards, however a caller retries.
    #[test]
    fn durable_applied_index_is_monotonic() {
        let mut node = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        assert_eq!(node.durable_applied_index(), 0);

        node.save_durable_applied_index(5).unwrap();
        assert_eq!(node.durable_applied_index(), 5);

        node.save_durable_applied_index(3).unwrap();
        assert_eq!(node.durable_applied_index(), 5);
    }
}
