// SPDX-License-Identifier: BUSL-1.1

//! State the tick phases carry from one pass to the next, and the handle of
//! the metadata apply lane.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_raft::LogEntry;
use tokio::sync::mpsc;

use super::leader_balance::{PeerHealth, PeerSample};

/// Samples, records and in-flight markers of the leader-balance and
/// leader-probe phases, and the metadata lane's sender. Every mutex is a
/// leaf lock: no other lock is taken while one is held.
#[derive(Debug, Default)]
pub struct TickState {
    /// `(group_id, peer) -> health record` of each preferred leader the
    /// balance sampled within the last few passes.
    peer_health: Mutex<HashMap<(u64, u64), PeerHealth>>,
    /// Groups with a leader probe in flight.
    probes_in_flight: Mutex<HashSet<u64>>,
    /// Groups whose mount is opening their disk.
    mounts_in_flight: Mutex<HashSet<u64>>,
    /// Sender of the metadata apply lane while the loop runs.
    metadata_tx: Mutex<Option<mpsc::Sender<Vec<LogEntry>>>>,
    /// Highest metadata index handed to the lane.
    metadata_sent_through: AtomicU64,
    /// `group_id -> (through, seq)` for a data group whose conf changes up
    /// to index `through` are applied in memory and wait for the routing
    /// save of request `seq`.
    conf_saves: Mutex<HashMap<u64, (u64, u64)>>,
}

impl TickState {
    pub fn new() -> Self {
        Self::default()
    }

    /// The `(through, seq)` conf-change save `group_id` waits for, if any.
    pub(super) fn conf_save(&self, group_id: u64) -> Option<(u64, u64)> {
        self.conf_saves
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&group_id)
            .copied()
    }

    /// Record that `group_id`'s conf changes up to `through` wait for the
    /// routing save of request `seq`.
    pub(super) fn set_conf_save(&self, group_id: u64, through: u64, seq: u64) {
        self.conf_saves
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(group_id, (through, seq));
    }

    /// Forget `group_id`'s conf-change save once its batch went on.
    pub(super) fn clear_conf_save(&self, group_id: u64) {
        self.conf_saves
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&group_id);
    }

    /// Record the balance pass `pass` sample of `peer` in `group_id`, and
    /// return the peer's updated health record.
    pub(super) fn observe_peer_health(
        &self,
        group_id: u64,
        peer: u64,
        sample: PeerSample,
        pass: u64,
    ) -> PeerHealth {
        let mut records = self.peer_health.lock().unwrap_or_else(|p| p.into_inner());
        let previous = records.get(&(group_id, peer)).copied();
        let next = PeerHealth::next(previous, sample, pass);
        records.insert((group_id, peer), next);
        next
    }

    /// Forget every record too old to count at balance pass `pass`. The map
    /// stays bounded by the groups this node led recently and their voters.
    pub(super) fn forget_stale_peer_health(&self, pass: u64) {
        self.peer_health
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|_, record| record.is_current(pass));
    }

    /// Mark a probe of `group_id` in flight. Returns `false` when one is.
    pub(super) fn begin_probe(&self, group_id: u64) -> bool {
        self.probes_in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(group_id)
    }

    /// Clear the in-flight marker of `group_id`'s probe.
    pub(super) fn end_probe(&self, group_id: u64) {
        self.probes_in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&group_id);
    }

    /// Mark a mount of `group_id` in flight. Returns `false` when one is.
    pub(super) fn begin_mount(&self, group_id: u64) -> bool {
        self.mounts_in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(group_id)
    }

    /// Clear the in-flight marker of `group_id`'s mount.
    pub(super) fn end_mount(&self, group_id: u64) {
        self.mounts_in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&group_id);
    }

    /// Install the sender of the metadata lane.
    pub(super) fn open_metadata_lane(&self, tx: mpsc::Sender<Vec<LogEntry>>) {
        *self.metadata_tx.lock().unwrap_or_else(|p| p.into_inner()) = Some(tx);
    }

    /// Drop the sender of the metadata lane. The lane ends once it applied
    /// or gave up every batch it holds.
    pub(super) fn close_metadata_lane(&self) {
        self.metadata_tx
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
    }

    /// Hand the entries of `batch` above the highest index handed so far to
    /// the metadata lane. Returns the entries the lane did not take: the
    /// lane is closed or full. The caller queues them again.
    pub(super) fn send_to_metadata_lane(&self, batch: &[LogEntry]) -> Vec<LogEntry> {
        let sent = self.metadata_sent_through.load(Ordering::Acquire);
        let fresh: Vec<LogEntry> = batch
            .iter()
            .filter(|entry| entry.index > sent)
            .cloned()
            .collect();
        let Some(through) = fresh.last().map(|entry| entry.index) else {
            return Vec::new();
        };
        let guard = self.metadata_tx.lock().unwrap_or_else(|p| p.into_inner());
        let Some(tx) = guard.as_ref() else {
            return fresh;
        };
        match tx.try_send(fresh) {
            Ok(()) => {
                self.metadata_sent_through.store(through, Ordering::Release);
                Vec::new()
            }
            Err(mpsc::error::TrySendError::Full(fresh))
            | Err(mpsc::error::TrySendError::Closed(fresh)) => fresh,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::leader_balance::{PEER_HEALTH_GAP_PASSES, PREFERRED_HEALTHY_PASSES};
    use super::*;

    fn entry(index: u64) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            data: Vec::new(),
        }
    }

    fn healthy(term: u64, acks: u64) -> PeerSample {
        PeerSample {
            term,
            acks,
            caught_up: true,
        }
    }

    #[test]
    fn a_peer_record_carries_from_pass_to_pass() {
        let state = TickState::new();
        let first = state.observe_peer_health(1, 2, healthy(1, 5), 0);
        assert!(!first.earned_leadership(), "no earlier pass");
        let mut last = first;
        for pass in 1..=u64::from(PREFERRED_HEALTHY_PASSES) {
            last = state.observe_peer_health(1, 2, healthy(1, 5 + pass), pass);
        }
        assert!(last.earned_leadership());
        // Another group's record is separate.
        assert!(
            !state
                .observe_peer_health(3, 2, healthy(1, 100), 5)
                .earned_leadership()
        );
    }

    #[test]
    fn stale_peer_records_are_forgotten() {
        let state = TickState::new();
        state.observe_peer_health(1, 2, healthy(1, 5), 0);
        state.forget_stale_peer_health(PEER_HEALTH_GAP_PASSES);
        assert_eq!(state.peer_health.lock().unwrap().len(), 1);
        state.forget_stale_peer_health(PEER_HEALTH_GAP_PASSES + 1);
        assert!(state.peer_health.lock().unwrap().is_empty());
    }

    #[test]
    fn one_probe_per_group_at_a_time() {
        let state = TickState::new();
        assert!(state.begin_probe(4));
        assert!(!state.begin_probe(4));
        state.end_probe(4);
        assert!(state.begin_probe(4));
    }

    #[test]
    fn the_lane_takes_each_index_once_and_returns_what_it_cannot_take() {
        let state = TickState::new();
        assert_eq!(state.send_to_metadata_lane(&[entry(1)]).len(), 1, "closed");
        let (tx, mut rx) = mpsc::channel(1);
        state.open_metadata_lane(tx);
        assert!(
            state
                .send_to_metadata_lane(&[entry(1), entry(2)])
                .is_empty()
        );
        // A repeat of handed entries sends nothing; the lane is full.
        assert!(state.send_to_metadata_lane(&[entry(2)]).is_empty());
        let back = state.send_to_metadata_lane(&[entry(2), entry(3)]);
        assert_eq!(back.iter().map(|e| e.index).collect::<Vec<_>>(), vec![3]);
        let got = rx.try_recv().expect("the first batch");
        assert_eq!(got.iter().map(|e| e.index).collect::<Vec<_>>(), vec![1, 2]);
        assert!(state.send_to_metadata_lane(&[entry(3)]).is_empty());
        state.close_metadata_lane();
        assert_eq!(state.send_to_metadata_lane(&[entry(4)]).len(), 1);
    }
}
