// SPDX-License-Identifier: BUSL-1.1

//! Whether each running Calvin scheduler on this node reached its rebuild
//! target.
//!
//! A scheduler starts with a rebuild target: the highest epoch its recovery
//! found applied for the vShard. It is caught up once its fully-applied
//! watermark reaches that target. A scheduler with no target is caught up
//! from the start. Startup holds the client gateway until every running
//! scheduler is caught up.
//!
//! Each scheduler holds a [`CaughtUpHandle`] for its vShard. The handle
//! removes its entry when the scheduler is dropped, so the registry lists
//! only running schedulers. A restarted scheduler replaces its predecessor's
//! entry, and the predecessor's drop leaves the new entry in place.
//!
//! Each entry also carries the scheduler's fully-applied watermark. Startup
//! reads it as progress: a lagging scheduler whose watermark rises is still
//! catching up, and one whose watermark stays put is stuck.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

type Entries = Arc<Mutex<BTreeMap<u32, Arc<Entry>>>>;

/// One running scheduler's state in the registry.
#[derive(Debug, Default)]
struct Entry {
    caught_up: AtomicBool,
    /// The highest fully-applied watermark reported plus one. Zero means none
    /// reported yet.
    watermark_mark: AtomicU64,
}

/// A running scheduler that has not reached its rebuild target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaggingScheduler {
    pub vshard_id: u32,
    /// The highest fully-applied watermark it reported, or `None` before its
    /// first.
    pub watermark: Option<u64>,
}

/// Caught-up state of every running scheduler on this node, by vShard.
#[derive(Debug, Default)]
pub struct CaughtUpRegistry {
    entries: Entries,
}

impl CaughtUpRegistry {
    /// Register the scheduler starting for `vshard_id`, not caught up yet.
    pub fn register(&self, vshard_id: u32) -> CaughtUpHandle {
        let entry = Arc::new(Entry::default());
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, Arc::clone(&entry));
        CaughtUpHandle {
            vshard_id,
            entry,
            entries: Arc::clone(&self.entries),
        }
    }

    /// The vShards whose running scheduler is not caught up, in order.
    pub fn lagging(&self) -> Vec<u32> {
        self.lagging_schedulers()
            .into_iter()
            .map(|lagging| lagging.vshard_id)
            .collect()
    }

    /// The running schedulers that are not caught up, by vShard, with the
    /// watermark each reported.
    pub fn lagging_schedulers(&self) -> Vec<LaggingScheduler> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(_, entry)| !entry.caught_up.load(Ordering::Acquire))
            .map(|(vshard_id, entry)| LaggingScheduler {
                vshard_id: *vshard_id,
                watermark: entry.watermark_mark.load(Ordering::Acquire).checked_sub(1),
            })
            .collect()
    }
}

/// One scheduler's entry in the [`CaughtUpRegistry`].
#[derive(Debug)]
pub struct CaughtUpHandle {
    vshard_id: u32,
    entry: Arc<Entry>,
    entries: Entries,
}

impl CaughtUpHandle {
    /// Whether this scheduler reported caught up.
    pub fn is_caught_up(&self) -> bool {
        self.entry.caught_up.load(Ordering::Acquire)
    }

    /// Report the scheduler's fully-applied watermark. A lower watermark than
    /// one already reported changes nothing.
    pub fn report_watermark(&self, watermark: u64) {
        self.entry
            .watermark_mark
            .fetch_max(watermark.saturating_add(1), Ordering::Release);
    }

    /// Report this scheduler caught up. The state never returns to lagging:
    /// the fully-applied watermark only rises.
    pub fn mark_caught_up(&self) {
        self.entry.caught_up.store(true, Ordering::Release);
    }
}

impl Drop for CaughtUpHandle {
    fn drop(&mut self) {
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if entries
            .get(&self.vshard_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
        {
            entries.remove(&self.vshard_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registered_scheduler_lags_until_it_reports() {
        let registry = CaughtUpRegistry::default();
        let first = registry.register(3);
        let second = registry.register(1);
        assert_eq!(registry.lagging(), vec![1, 3]);

        second.mark_caught_up();
        assert!(second.is_caught_up());
        assert_eq!(registry.lagging(), vec![3]);

        first.mark_caught_up();
        assert!(registry.lagging().is_empty());
    }

    #[test]
    fn a_lagging_scheduler_reports_its_highest_watermark() {
        let registry = CaughtUpRegistry::default();
        let handle = registry.register(4);
        assert_eq!(
            registry.lagging_schedulers(),
            vec![LaggingScheduler {
                vshard_id: 4,
                watermark: None,
            }]
        );

        handle.report_watermark(0);
        handle.report_watermark(7);
        handle.report_watermark(3);
        assert_eq!(registry.lagging_schedulers()[0].watermark, Some(7));

        handle.mark_caught_up();
        assert!(registry.lagging_schedulers().is_empty());
    }

    #[test]
    fn a_dropped_scheduler_leaves_the_registry() {
        let registry = CaughtUpRegistry::default();
        let handle = registry.register(5);
        assert_eq!(registry.lagging(), vec![5]);
        drop(handle);
        assert!(registry.lagging().is_empty());
    }

    #[test]
    fn a_predecessor_drop_keeps_its_successor_entry() {
        let registry = CaughtUpRegistry::default();
        let predecessor = registry.register(7);
        let successor = registry.register(7);
        drop(predecessor);
        assert_eq!(registry.lagging(), vec![7], "the successor still lags");
        successor.mark_caught_up();
        assert!(registry.lagging().is_empty());
    }
}
