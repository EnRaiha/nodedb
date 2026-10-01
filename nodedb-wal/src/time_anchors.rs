// SPDX-License-Identifier: Apache-2.0

//! The live LSN ↔ commit-time map of one WAL.
//!
//! Two paths feed it, and both go through [`TimeAnchors::record`]:
//!
//! - the writer, after the fsync that made an anchored batch durable;
//! - replay, for each `TimeAnchor` record read back at boot.
//!
//! Anchor times come from the node's HLC. The HLC never runs backwards within
//! a process, and replay folds each persisted anchor into it, so anchors stay
//! strictly increasing across restarts too.

use std::sync::{Arc, Mutex, MutexGuard};

use nodedb_types::hlc::{Hlc, HlcClock};
use nodedb_types::temporal::{LsnTimeAnchor, LsnTimeError, LsnTimeMap};

use crate::error::Result;
use crate::record::{RecordType, TimeAnchorPayload, WalRecord};

#[derive(Debug)]
struct State {
    map: LsnTimeMap,
    /// Last time handed to the writer. Kept apart from the map because a
    /// stamped anchor enters the map only after its fsync.
    last_stamp_ns: u64,
}

/// Commit-time anchors of one WAL, plus the clock that stamps them.
#[derive(Debug)]
pub struct TimeAnchors {
    clock: Arc<HlcClock>,
    state: Mutex<State>,
}

impl TimeAnchors {
    pub fn new(clock: Arc<HlcClock>) -> Self {
        Self::with_map(clock, LsnTimeMap::new())
    }

    /// Anchors held in `map`, whose capacity bounds memory.
    pub fn with_map(clock: Arc<HlcClock>, map: LsnTimeMap) -> Self {
        Self {
            clock,
            state: Mutex::new(State {
                map,
                last_stamp_ns: 0,
            }),
        }
    }

    /// The clock anchors are stamped from.
    pub fn clock(&self) -> &Arc<HlcClock> {
        &self.clock
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Time for the anchor of the batch being committed now. Strictly above
    /// every earlier stamp and every recorded anchor.
    pub(crate) fn stamp(&self) -> u64 {
        let mut state = self.lock();
        let floor = state
            .map
            .last()
            .map_or(0, |a| a.hlc_wall_ns)
            .max(state.last_stamp_ns)
            .saturating_add(1);
        let ns = self.clock.now().wall_ns.max(floor);
        state.last_stamp_ns = ns;
        ns
    }

    /// Record that every record at or below `lsn` committed by `hlc_wall_ns`.
    /// Returns `Ok(false)` for an anchor the map already covers.
    pub fn record(&self, lsn: u64, hlc_wall_ns: u64) -> std::result::Result<bool, LsnTimeError> {
        self.lock().map.push(LsnTimeAnchor::new(lsn, hlc_wall_ns))
    }

    /// Record every `TimeAnchor` in replayed `records` and fold its time into
    /// the clock.
    ///
    /// A replayed anchor that does not advance the map's time is skipped with a
    /// warning. Its records fall to the next anchor, which is later: lookups
    /// get coarser there but never return an LSN committed after the target.
    pub fn absorb_replayed(&self, records: &[WalRecord]) -> Result<()> {
        for record in records {
            if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::TimeAnchor) {
                continue;
            }
            let payload = TimeAnchorPayload::from_bytes(&record.payload)?;
            self.clock.update(Hlc::new(payload.hlc_wall_ns, 0));
            if let Err(error) = self.record(record.header.lsn, payload.hlc_wall_ns) {
                tracing::warn!(
                    lsn = record.header.lsn,
                    hlc_wall_ns = payload.hlc_wall_ns,
                    %error,
                    "replayed WAL time anchor skipped"
                );
            }
        }
        Ok(())
    }

    /// Anchor every record up to `last_recovered_lsn` at the time of this call.
    ///
    /// A batch whose sync never finished has no anchor on disk, but its
    /// records are durable by the time boot reads them back. On an empty log,
    /// `last_recovered_lsn` is 0 and the anchor says nothing had committed by
    /// the time the log opened.
    pub fn cover_recovered(&self, last_recovered_lsn: u64) {
        let covered = self
            .lock()
            .map
            .last()
            .is_some_and(|a| a.lsn >= last_recovered_lsn);
        if covered {
            return;
        }
        let ns = self.stamp();
        if let Err(error) = self.record(last_recovered_lsn, ns) {
            tracing::warn!(
                lsn = last_recovered_lsn,
                %error,
                "recovered WAL tail left without a time anchor"
            );
        }
    }

    /// The highest LSN committed at or before `target_ns`.
    pub fn lsn_at_or_before(&self, target_ns: u64) -> std::result::Result<u64, LsnTimeError> {
        self.lock().map.lsn_at_or_before(target_ns)
    }

    /// The highest LSN committed at or before the end of millisecond `target_ms`.
    pub fn lsn_at_or_before_ms(&self, target_ms: i64) -> std::result::Result<u64, LsnTimeError> {
        self.lock().map.lsn_at_or_before_ms(target_ms)
    }

    /// Commit time of the batch holding `lsn`, or `None` if no anchor covers it.
    pub fn commit_ns_of(&self, lsn: u64) -> Option<u64> {
        self.lock().map.commit_ns_of(lsn)
    }

    /// Copy of the retained anchors, oldest first.
    pub fn anchors(&self) -> Vec<LsnTimeAnchor> {
        self.lock().map.anchors().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::WalRecordArgs;

    fn anchor_record(lsn: u64, ns: u64) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TimeAnchor as u32,
            lsn,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: TimeAnchorPayload::new(ns).to_bytes().to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap()
    }

    #[test]
    fn stamps_strictly_increase_past_recorded_anchors() {
        let anchors = TimeAnchors::new(Arc::new(HlcClock::new()));
        let far_future = u64::MAX / 2;
        anchors.record(1, far_future).unwrap();
        let a = anchors.stamp();
        let b = anchors.stamp();
        assert!(a > far_future);
        assert!(b > a);
    }

    #[test]
    fn replay_folds_anchor_time_into_the_clock() {
        let clock = Arc::new(HlcClock::new());
        let anchors = TimeAnchors::new(Arc::clone(&clock));
        let far_future = u64::MAX / 2;
        anchors
            .absorb_replayed(&[
                anchor_record(3, far_future - 10),
                anchor_record(7, far_future),
            ])
            .unwrap();
        assert!(clock.peek().wall_ns >= far_future);
        assert_eq!(anchors.lsn_at_or_before(far_future - 1).unwrap(), 3);
        assert_eq!(anchors.lsn_at_or_before(far_future).unwrap(), 7);
        assert!(anchors.stamp() > far_future);
    }

    #[test]
    fn empty_log_resolves_to_lsn_zero_after_open() {
        let anchors = TimeAnchors::new(Arc::new(HlcClock::new()));
        anchors.cover_recovered(0);
        let opened = anchors.anchors()[0];
        assert_eq!(opened.lsn, 0);
        assert_eq!(anchors.lsn_at_or_before(opened.hlc_wall_ns).unwrap(), 0);
        assert_eq!(
            anchors.lsn_at_or_before(opened.hlc_wall_ns - 1).unwrap(),
            0,
            "nothing had committed before the log opened"
        );
    }

    #[test]
    fn recovered_tail_is_covered_once() {
        let anchors = TimeAnchors::new(Arc::new(HlcClock::new()));
        anchors.absorb_replayed(&[anchor_record(4, 1_000)]).unwrap();
        anchors.cover_recovered(9);
        anchors.cover_recovered(9);
        let held = anchors.anchors();
        assert_eq!(held.len(), 2);
        assert_eq!(held[1].lsn, 9);
        assert!(held[1].hlc_wall_ns > 1_000);
    }
}
