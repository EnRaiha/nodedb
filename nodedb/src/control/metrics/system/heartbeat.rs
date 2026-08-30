// SPDX-License-Identifier: BUSL-1.1

//! Per-core Data Plane liveness counters.
//!
//! One `AtomicU64` per core, bumped at the top of every event-loop iteration
//! before any work runs. A counter that stops advancing means that core is no
//! longer completing iterations — the only cross-plane evidence of a stalled
//! core, since the Data Plane is `!Send` and shares nothing else.
//!
//! Sized once at boot from the configured core count. Cores are dense
//! `0..num_cores`, so a core id indexes its own slot directly and no core
//! ever contends with another on the same cache line's counter.

use std::sync::atomic::{AtomicU64, Ordering};

/// Liveness counters for every Data Plane core on this node.
#[derive(Debug, Default)]
pub struct CoreHeartbeats {
    beats: Box<[AtomicU64]>,
}

impl CoreHeartbeats {
    /// Allocate zeroed counters for `num_cores` cores.
    pub fn new(num_cores: usize) -> Self {
        Self {
            beats: (0..num_cores).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Number of cores tracked.
    pub fn len(&self) -> usize {
        self.beats.len()
    }

    pub fn is_empty(&self) -> bool {
        self.beats.is_empty()
    }

    /// This core's counter, or `None` when the array was sized without a Data
    /// Plane (a metrics instance built outside boot).
    ///
    /// A core resolves its slot once before entering its loop, so the steady
    /// state costs one relaxed increment and no bounds check.
    pub fn slot(&self, core_id: usize) -> Option<&AtomicU64> {
        self.beats.get(core_id)
    }

    /// Read every counter into `out`, replacing its contents.
    ///
    /// Reuses the caller's buffer so the sampling loop allocates once for the
    /// life of the process. Relaxed loads: the monitor compares a counter
    /// against its own earlier value and orders nothing else against it.
    pub fn sample_into(&self, out: &mut Vec<u64>) {
        out.clear();
        out.extend(self.beats.iter().map(|beat| beat.load(Ordering::Relaxed)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_dense_and_bounded_by_core_count() {
        let beats = CoreHeartbeats::new(3);
        assert_eq!(beats.len(), 3);
        assert!(beats.slot(2).is_some());
        assert!(beats.slot(3).is_none());
    }

    #[test]
    fn sampling_reads_back_what_cores_incremented() {
        let beats = CoreHeartbeats::new(2);
        if let Some(slot) = beats.slot(1) {
            slot.fetch_add(5, Ordering::Relaxed);
        }
        let mut sample = vec![99u64; 7];
        beats.sample_into(&mut sample);
        assert_eq!(sample, vec![0, 5]);
    }
}
