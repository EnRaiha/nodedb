// SPDX-License-Identifier: BUSL-1.1

//! Audit buffer flush, retention pruning, and merged reads for `SharedState`.
//!
//! The in-memory buffer and the catalog together hold the whole audit log.
//! A flush commits buffered entries to the catalog and only then evicts them.
//! Readers copy the buffer first and read the catalog after.
//! So a reader sees every entry recorded before it started.

use std::collections::BTreeMap;

use tracing::{debug, info, warn};

use super::SharedState;
use crate::control::security::audit::guard::{AuditLogGuard, lock_audit_log};
use crate::control::security::audit::{AuditEntry, AuditEvent, checkpoint_detail};
use crate::control::security::catalog::audit::AuditPrunePlan;

/// Microseconds in one day, for the age-based retention cutoff.
const MICROS_PER_DAY: u64 = 86_400 * 1_000_000;

/// Merge persisted and buffered entries by seq and keep the newest `limit`.
///
/// A seq present in both sources appears once. The result is in ascending seq order.
fn merge_newest(
    persisted: Vec<AuditEntry>,
    buffered: Vec<AuditEntry>,
    limit: usize,
) -> Vec<AuditEntry> {
    let mut by_seq: BTreeMap<u64, AuditEntry> = persisted.into_iter().map(|e| (e.seq, e)).collect();
    by_seq.extend(buffered.into_iter().map(|e| (e.seq, e)));
    let skip = by_seq.len().saturating_sub(limit);
    by_seq.into_values().skip(skip).collect()
}

impl SharedState {
    fn lock_audit(&self) -> AuditLogGuard<'_> {
        lock_audit_log(&self.audit)
    }

    /// Newest `limit` audit entries that pass `filter`, in ascending seq order.
    ///
    /// The result holds every entry recorded before this call started.
    pub fn recent_audit_entries(
        &self,
        limit: usize,
        filter: impl Fn(&AuditEntry) -> bool,
    ) -> crate::Result<Vec<AuditEntry>> {
        let buffered: Vec<AuditEntry> = self
            .lock_audit()
            .all()
            .iter()
            .rev()
            .filter(|entry| filter(entry))
            .take(limit)
            .cloned()
            .collect();
        let persisted = self
            .credentials
            .catalog()
            .load_recent_audit_entries(limit, &filter)?;
        Ok(merge_newest(persisted, buffered, limit))
    }

    /// DML audit entries whose event LSN lies above its core's floor, in ascending seq order.
    ///
    /// `floors[c]` is core `c`'s floor. A core at or past `floors.len()` has floor 0.
    /// The buffer is copied first and the catalog index is read after, as in [`Self::recent_audit_entries`].
    pub fn dml_audits_above(&self, floors: &[u64]) -> crate::Result<Vec<AuditEntry>> {
        let above = |entry: &AuditEntry| {
            entry.event == AuditEvent::DmlAudit
                && crate::event::sink_ledger::audit::dml_event_position(&entry.detail).is_some_and(
                    |(core, lsn)| lsn > floors.get(core as usize).copied().unwrap_or(0),
                )
        };
        let buffered: Vec<AuditEntry> = self
            .lock_audit()
            .all()
            .iter()
            .filter(|entry| above(entry))
            .cloned()
            .collect();
        let persisted = self.credentials.catalog().load_dml_audits_above(floors)?;
        Ok(merge_newest(persisted, buffered, usize::MAX))
    }

    /// The whole audit log in ascending seq order, plus the live tail hash.
    ///
    /// The entries end at the seq the tail hash belongs to.
    /// An entry recorded during the read is left out, so the pair stays consistent.
    pub fn audit_chain_snapshot(&self) -> crate::Result<(Vec<AuditEntry>, String)> {
        let (buffered, last_seq, tail_hash) = {
            let log = self.lock_audit();
            (
                log.all().iter().cloned().collect::<Vec<_>>(),
                log.last_seq(),
                log.last_hash().to_string(),
            )
        };
        let persisted = self
            .credentials
            .catalog()
            .load_recent_audit_entries(usize::MAX, |entry| entry.seq <= last_seq)?;
        Ok((merge_newest(persisted, buffered, usize::MAX), tail_hash))
    }

    /// Flush buffered audit entries to the catalog, then apply retention pruning.
    ///
    /// Entries stay in the buffer until the catalog commit succeeds.
    /// On a catalog error they stay buffered for the next flush.
    /// Flushes are serialized, so two callers never interleave.
    /// The flush executor and explicit maintenance calls run this. A recorder never does.
    /// When it returns, every recorder waiting for backpressure wakes.
    pub fn flush_audit_log(&self) {
        let control = self.lock_audit().flush_control();
        {
            let _serialized = control.serialize();
            if self.persist_audit_buffer() {
                self.prune_audit_catalog();
            }
        }
        control.note_flush_completed();
    }

    /// Commit the buffer to the catalog and evict what was committed.
    ///
    /// Returns `false` when the catalog commit failed.
    fn persist_audit_buffer(&self) -> bool {
        let entries = self.lock_audit().snapshot_for_persistence();
        let Some(through_seq) = entries.last().map(|e| e.seq) else {
            return true;
        };
        if let Err(e) = self.credentials.catalog().append_audit_entries(&entries) {
            warn!(
                error = %e,
                count = entries.len(),
                "failed to persist audit entries, kept in memory for the next flush"
            );
            crate::diag::audit_flush_not_persisted(&e, entries.len(), through_seq);
            return false;
        }
        let (committed_position, wal_sink) = {
            let mut log = self.lock_audit();
            (log.evict_persisted(through_seq), log.wal_sink())
        };
        debug!(count = entries.len(), "flushed audit entries to catalog");
        // The catalog holds every entry through `committed_position`, so the WAL drops them.
        // A truncation error leaves the records in place, and the next flush covers them.
        if let (Some(position), Some(sink)) = (committed_position, wal_sink)
            && let Err(e) = sink.truncate_through(position)
        {
            warn!(error = %e, position, "failed to truncate the audit WAL");
        }
        true
    }

    /// Apply the age and count retention limits to the persisted audit log.
    fn prune_audit_catalog(&self) {
        if self.audit_retention_days > 0 {
            self.prune_audit_by_age(self.audit_retention_days);
        }
        if self.audit_max_entries > 0 {
            self.prune_audit_by_count(self.audit_max_entries);
        }
    }

    /// Prune persisted entries older than `retention_days`.
    fn prune_audit_by_age(&self, retention_days: u32) {
        let retention_us = u64::from(retention_days).saturating_mul(MICROS_PER_DAY);
        let now_us = crate::control::security::audit::entry::now_us();
        let cutoff = now_us.saturating_sub(retention_us);
        let plan = self.credentials.catalog().plan_audit_prune_before(cutoff);
        self.apply_audit_prune("retention_age", plan);
    }

    /// Prune the oldest persisted entries until at most `max_entries` remain.
    fn prune_audit_by_count(&self, max_entries: u64) {
        let plan = self
            .credentials
            .catalog()
            .plan_audit_prune_to_count(max_entries);
        self.apply_audit_prune("retention_count", plan);
    }

    /// Record the prune's checkpoint, persist it, then delete the planned rows.
    ///
    /// The checkpoint names the hash the first surviving entry links to.
    /// It reaches the catalog before any row is deleted.
    /// So a crash cannot leave a surviving head that the chain verifier cannot anchor.
    fn apply_audit_prune(&self, source: &'static str, plan: crate::Result<Option<AuditPrunePlan>>) {
        let plan = match plan {
            Ok(Some(plan)) => plan,
            Ok(None) => return,
            Err(e) => {
                warn!(error = %e, source, "failed to plan audit pruning");
                return;
            }
        };
        let detail = checkpoint_detail(
            plan.pruned_count,
            plan.through_seq.saturating_add(1),
            &plan.anchor_hash,
        );
        let (durable, _, _) =
            self.audit_record_buffered(AuditEvent::AuditCheckpoint, None, None, source, &detail);
        if let Err(e) = durable {
            warn!(error = %e, source, "audit checkpoint WAL write failed");
        }
        if !self.persist_audit_buffer() {
            return;
        }
        match self
            .credentials
            .catalog()
            .delete_audit_through(plan.through_seq)
        {
            Ok(pruned) => info!(
                pruned,
                through_seq = plan.through_seq,
                source,
                "pruned audit entries"
            ),
            Err(e) => warn!(error = %e, source, "failed to prune audit entries"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::audit::verify_chain;
    use crate::wal::WalManager;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create audit store test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("audit-store.wal"))
                .expect("open audit store test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct audit store state");
        (state, directory)
    }

    fn record(state: &SharedState, prefix: &str, count: usize) {
        for i in 0..count {
            state.audit_record(
                AuditEvent::AdminAction,
                None,
                "test",
                &format!("{prefix}-{i}"),
            );
        }
    }

    fn chain_verdict(state: &SharedState) -> Option<u64> {
        let (entries, tail) = state.audit_chain_snapshot().expect("read audit chain");
        verify_chain(&entries, 1, u64::MAX, &tail).broken_at
    }

    #[tokio::test]
    async fn chain_stays_valid_across_flushes() {
        let (state, _dir) = test_state();
        record(&state, "before", 5);
        assert_eq!(chain_verdict(&state), None);
        state.flush_audit_log();
        assert_eq!(
            chain_verdict(&state),
            None,
            "a flush must not break the chain"
        );
        record(&state, "after", 3);
        assert_eq!(
            chain_verdict(&state),
            None,
            "the buffer must link to the catalog"
        );
        state.flush_audit_log();
        assert_eq!(chain_verdict(&state), None);
    }

    #[tokio::test]
    async fn count_prune_keeps_the_chain_anchored() {
        let (state, _dir) = test_state();
        record(&state, "old", 10);
        state.flush_audit_log();
        state.prune_audit_by_count(4);
        let catalog = state.credentials.catalog();
        assert_eq!(catalog.audit_entry_count().expect("count"), 4);
        assert_eq!(
            chain_verdict(&state),
            None,
            "a pruned head must stay anchored"
        );

        // The table is within the cap, so a second pass records no new checkpoint.
        state.prune_audit_by_count(4);
        assert_eq!(catalog.audit_entry_count().expect("count"), 4);

        record(&state, "new", 3);
        state.flush_audit_log();
        state.prune_audit_by_count(4);
        assert_eq!(chain_verdict(&state), None);
    }

    #[tokio::test]
    async fn age_prune_keeps_the_chain_anchored() {
        let (state, _dir) = test_state();
        record(&state, "old", 6);
        state.flush_audit_log();
        // A zero-day window makes every persisted entry older than the cutoff.
        state.prune_audit_by_age(0);
        assert_eq!(chain_verdict(&state), None);
        record(&state, "new", 2);
        assert_eq!(chain_verdict(&state), None);
    }

    #[tokio::test]
    async fn tampered_persisted_row_is_reported() {
        let (state, _dir) = test_state();
        record(&state, "row", 5);
        state.flush_audit_log();
        let catalog = state.credentials.catalog();
        let mut rows = catalog
            .load_recent_audit_entries(usize::MAX, |_| true)
            .expect("read rows");
        let target = rows.len() / 2;
        let tampered_seq = rows[target].seq;
        rows[target].detail = "tampered".to_string();
        catalog
            .append_audit_entries(&rows[target..=target])
            .expect("overwrite row");
        assert_eq!(
            chain_verdict(&state),
            Some(tampered_seq + 1),
            "the link after the altered row must fail"
        );
    }

    /// Run a flush executor on its own thread until `stop` is set, as the flush task does.
    fn spawn_executor(
        state: &Arc<SharedState>,
        stop: &Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        let state = Arc::clone(state);
        let stop = Arc::clone(stop);
        let control = state.lock_audit().flush_control();
        control.attach_executor();
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                if control.take_flush_request() {
                    state.flush_audit_log();
                } else {
                    std::thread::yield_now();
                }
            }
            control.detach_executor();
        })
    }

    #[tokio::test]
    async fn burst_past_the_bound_loses_nothing() {
        let (state, _dir) = test_state();
        state.lock_audit().set_flush_bound(4);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = spawn_executor(&state, &stop);

        let first_seq = state.lock_audit().last_seq() + 1;
        record(&state, "central", 50);
        for i in 0..50 {
            state.lock_audit().record(
                AuditEvent::AdminAction,
                None,
                "test",
                &format!("direct-{i}"),
            );
        }
        let last_seq = state.lock_audit().last_seq();
        assert!(
            state
                .credentials
                .catalog()
                .audit_entry_count()
                .expect("count")
                > 0,
            "backpressure must have the executor flush during the burst"
        );

        stop.store(true, std::sync::atomic::Ordering::Release);
        executor.join().expect("executor thread");

        let in_burst = |e: &AuditEntry| (first_seq..=last_seq).contains(&e.seq);
        let seqs: Vec<u64> = state
            .recent_audit_entries(usize::MAX, in_burst)
            .expect("read burst")
            .iter()
            .map(|e| e.seq)
            .collect();
        assert_eq!(seqs, (first_seq..=last_seq).collect::<Vec<_>>());

        state.flush_audit_log();
        let persisted = state
            .credentials
            .catalog()
            .load_recent_audit_entries(usize::MAX, in_burst)
            .expect("read persisted burst");
        assert_eq!(
            persisted.len(),
            seqs.len(),
            "every burst entry must reach the catalog"
        );
        assert_eq!(chain_verdict(&state), None);
    }

    fn event_key(lsn: u64) -> crate::event::sink_ledger::SinkEventKey {
        crate::event::sink_ledger::SinkEventKey {
            core: 0,
            lsn,
            occurrence: 0,
            delete: false,
            collection: "orders".into(),
            row_kind: 0,
            row: format!("o-{lsn}"),
        }
    }

    fn record_dml(state: &SharedState, lsn: u64) {
        let detail = format!(
            "INSERT orders:o-{lsn} lsn={lsn}{}",
            crate::event::sink_ledger::audit::detail_suffix(&event_key(lsn))
        );
        state.audit_record(AuditEvent::DmlAudit, None, "alice", &detail);
    }

    #[tokio::test]
    async fn redelivery_above_the_floor_stays_deduplicated() {
        use crate::event::sink_ledger::AuditedKeys;

        let (state, _dir) = test_state();
        // Old rows and some rows above the floor reach the catalog.
        for lsn in 1..=500 {
            record_dml(&state, lsn);
        }
        state.flush_audit_log();
        // Newer rows stay in the buffer.
        for lsn in 501..=510 {
            record_dml(&state, lsn);
        }

        let floor = 495;
        let above = state.dml_audits_above(&[floor]).expect("read DML audits");
        assert_eq!(above.len(), 15, "only rows above the floor are read");

        let audited = AuditedKeys::from_entries(above.iter(), |_| floor);
        assert!(
            audited.contains(&event_key(498)),
            "a persisted row above the floor dedups"
        );
        assert!(
            audited.contains(&event_key(505)),
            "a buffered row above the floor dedups"
        );
        assert!(
            !audited.contains(&event_key(400)),
            "a row at or below the floor is not loaded"
        );
    }

    fn wal_seqs(state: &SharedState) -> Vec<u64> {
        state
            .wal
            .recover_audit_entries()
            .expect("recover audit WAL")
            .iter()
            .map(|(_, bytes)| {
                zerompk::from_msgpack::<AuditEntry>(bytes)
                    .expect("decode audit WAL record")
                    .seq
            })
            .collect()
    }

    #[tokio::test]
    async fn flush_truncates_the_wal_and_a_crash_still_restores_everything() {
        let (state, _dir) = test_state();
        record(&state, "old", 5);
        state.flush_audit_log();
        assert!(
            wal_seqs(&state).is_empty(),
            "the WAL must drop every record the catalog holds"
        );

        record(&state, "new", 3);
        let buffered: Vec<u64> = state.lock_audit().all().iter().map(|e| e.seq).collect();
        assert_eq!(
            wal_seqs(&state),
            buffered,
            "the WAL must hold only unflushed records"
        );

        // A crash loses the buffer. The catalog and the WAL survive it.
        let all_seqs: Vec<u64> = state
            .recent_audit_entries(usize::MAX, |_| true)
            .expect("read audit")
            .iter()
            .map(|e| e.seq)
            .collect();
        let catalog = state.credentials.catalog();
        crate::control::security::audit::restore_audit_log(catalog, &state.wal)
            .expect("boot restore");
        let restored: Vec<u64> = catalog
            .load_recent_audit_entries(usize::MAX, |_| true)
            .expect("read catalog")
            .iter()
            .map(|e| e.seq)
            .collect();
        assert_eq!(
            restored, all_seqs,
            "every entry must survive a crash after truncation"
        );
        assert!(
            wal_seqs(&state).is_empty(),
            "boot restore drops what it committed"
        );
    }

    /// A flush that cannot start, as when it waits on a transaction the recorder holds.
    /// The recorder must return after the bounded wait, and every entry must stay buffered.
    #[tokio::test]
    async fn stuck_executor_never_blocks_a_recorder_past_the_wait() {
        let (state, _dir) = test_state();
        let control = {
            let mut log = state.lock_audit();
            log.set_flush_bound(1);
            log.flush_control()
        };
        control.attach_executor();
        control.set_backpressure_wait(std::time::Duration::from_millis(20));

        let first_seq = state.lock_audit().last_seq() + 1;
        let started = std::time::Instant::now();
        record(&state, "stuck", 5);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "each wait must end at its bound"
        );
        let last_seq = state.lock_audit().last_seq();
        let buffered = state
            .lock_audit()
            .all()
            .iter()
            .filter(|e| (first_seq..=last_seq).contains(&e.seq))
            .count();
        assert_eq!(
            buffered, 5,
            "a timed-out wait must keep every entry buffered"
        );
        control.detach_executor();
    }

    /// The flush path records its checkpoint without waiting, so a prune past the bound cannot stall.
    #[tokio::test]
    async fn prune_past_the_bound_does_not_wait_on_itself() {
        let (state, _dir) = test_state();
        record(&state, "old", 6);
        state.flush_audit_log();
        let control = {
            let mut log = state.lock_audit();
            log.set_flush_bound(1);
            log.flush_control()
        };
        control.attach_executor();
        control.set_backpressure_wait(std::time::Duration::from_secs(30));
        let started = std::time::Instant::now();
        state.prune_audit_by_count(2);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "the checkpoint record must not wait for a flush"
        );
        control.detach_executor();
        assert_eq!(chain_verdict(&state), None);
    }
}
