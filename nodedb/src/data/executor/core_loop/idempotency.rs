// SPDX-License-Identifier: BUSL-1.1

//! The outcomes of recent idempotency keys on a core.

use std::collections::{HashMap, VecDeque};

/// The most keys a core remembers. The oldest key goes first.
const CAPACITY: usize = 16_384;

/// Whether each recent idempotency key succeeded, with its arrival order for
/// eviction.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct IdempotencyCache {
    outcomes: HashMap<u64, bool>,
    order: VecDeque<u64>,
}

impl IdempotencyCache {
    /// Whether `key` succeeded, when the cache holds it.
    pub(in crate::data::executor) fn outcome(&self, key: u64) -> Option<bool> {
        self.outcomes.get(&key).copied()
    }

    /// Remember that `key` succeeded or failed. A full cache drops its oldest
    /// key first.
    pub(in crate::data::executor) fn record(&mut self, key: u64, succeeded: bool) {
        if self.outcomes.len() >= CAPACITY
            && let Some(oldest) = self.order.pop_front()
        {
            self.outcomes.remove(&oldest);
        }
        self.outcomes.insert(key, succeeded);
        self.order.push_back(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_cache_drops_its_oldest_key() {
        let mut cache = IdempotencyCache::default();
        for key in 0..CAPACITY as u64 {
            cache.record(key, true);
        }
        cache.record(u64::MAX, false);
        assert_eq!(cache.outcome(0), None);
        assert_eq!(cache.outcome(1), Some(true));
        assert_eq!(cache.outcome(u64::MAX), Some(false));
    }
}
