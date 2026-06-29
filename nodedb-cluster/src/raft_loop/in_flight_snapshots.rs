// SPDX-License-Identifier: BUSL-1.1

//! Tracks in-flight `InstallSnapshot` transfers per Raft group.
//!
//! Log compaction advances a group's snapshot boundary. Doing so while a
//! snapshot transfer to a catching-up peer is in flight would move the
//! boundary mid-transfer and corrupt the peer's catch-up. The tick loop
//! marks a group as having an active send for the lifetime of each transfer
//! task (via [`InFlightSnapshots::begin`] + the returned guard), and
//! `maybe_compact_group` skips compaction for any group reporting an active
//! transfer, retrying on the next applied entry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Per-group count of active `InstallSnapshot` send tasks.
///
/// The `counts` mutex is a LEAF lock: its methods never acquire any other
/// lock while holding it.
#[derive(Debug, Default)]
pub struct InFlightSnapshots {
    counts: Mutex<HashMap<u64, u32>>,
}

impl InFlightSnapshots {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the start of a snapshot transfer for `group_id`, returning a guard
    /// that decrements the count when dropped (including on error/shutdown).
    pub fn begin(self: &Arc<Self>, group_id: u64) -> InFlightSnapshotGuard {
        {
            let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *counts.entry(group_id).or_insert(0) += 1;
        }
        InFlightSnapshotGuard {
            inner: Arc::clone(self),
            group_id,
        }
    }

    /// Whether a snapshot transfer is currently in flight for `group_id`.
    pub fn is_active(&self, group_id: u64) -> bool {
        let counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        counts.get(&group_id).copied().unwrap_or(0) > 0
    }
}

/// Drop guard that decrements the active-send count for its group, removing
/// the entry when it reaches zero.
#[derive(Debug)]
pub struct InFlightSnapshotGuard {
    inner: Arc<InFlightSnapshots>,
    group_id: u64,
}

impl Drop for InFlightSnapshotGuard {
    fn drop(&mut self) {
        let mut counts = self.inner.counts.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(count) = counts.get_mut(&self.group_id) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.group_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inactive_when_empty() {
        let tracker = Arc::new(InFlightSnapshots::new());
        assert!(!tracker.is_active(1));
    }

    #[test]
    fn begin_marks_active_and_drop_clears() {
        let tracker = Arc::new(InFlightSnapshots::new());
        {
            let _guard = tracker.begin(7);
            assert!(tracker.is_active(7));
            assert!(!tracker.is_active(8));
        }
        assert!(!tracker.is_active(7));
    }

    #[test]
    fn nested_sends_require_all_guards_dropped() {
        let tracker = Arc::new(InFlightSnapshots::new());
        let g1 = tracker.begin(3);
        let g2 = tracker.begin(3);
        assert!(tracker.is_active(3));
        drop(g1);
        assert!(tracker.is_active(3));
        drop(g2);
        assert!(!tracker.is_active(3));
    }
}
