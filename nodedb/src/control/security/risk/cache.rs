// SPDX-License-Identifier: BUSL-1.1

//! [`KnownIpCache`] — the bounded per-user known-IP set backing the `new_ip`
//! signal.
//!
//! Both dimensions are bounded: at most [`MAX_IPS_PER_USER`] addresses are
//! kept per user, and at most `max_users` users are tracked at all. Neither
//! is allowed to grow with traffic — this map is touched on every scored
//! request, and an unbounded map on that path is a memory-exhaustion vector
//! an unauthenticated-user-id spray could drive.

use std::collections::{HashMap, VecDeque};

/// Addresses retained per user before the oldest is evicted.
pub const MAX_IPS_PER_USER: usize = 50;

/// Bounded `user -> recently seen IPs` map with FIFO eviction on both axes.
#[derive(Debug, Default)]
pub struct KnownIpCache {
    ips: HashMap<String, Vec<String>>,
    /// Insertion order of the keys in `ips`, used to evict the oldest user
    /// once `max_users` is exceeded.
    order: VecDeque<String>,
}

impl KnownIpCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `ip` has already been seen for `user_id`.
    pub fn contains(&self, user_id: &str, ip: &str) -> bool {
        self.ips
            .get(user_id)
            .is_some_and(|ips| ips.iter().any(|known| known == ip))
    }

    /// Record `ip` as known for `user_id`, evicting the oldest entry on
    /// either axis if that would exceed a bound.
    pub fn record(&mut self, user_id: &str, ip: &str, max_users: usize) {
        if let Some(ips) = self.ips.get_mut(user_id) {
            if ips.iter().any(|known| known == ip) {
                return;
            }
            if ips.len() >= MAX_IPS_PER_USER {
                ips.remove(0);
            }
            ips.push(ip.to_string());
            return;
        }

        // A `max_users` of 0 disables tracking entirely rather than
        // admitting an unbounded map; every request then reads as a new IP.
        if max_users == 0 {
            return;
        }
        while self.order.len() >= max_users {
            match self.order.pop_front() {
                Some(evicted) => {
                    self.ips.remove(&evicted);
                }
                None => break,
            }
        }
        self.ips.insert(user_id.to_string(), vec![ip.to_string()]);
        self.order.push_back(user_id.to_string());
    }

    /// Number of tracked users. Test/observability accessor.
    pub fn tracked_users(&self) -> usize {
        self.ips.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_recalls_an_ip() {
        let mut cache = KnownIpCache::new();
        assert!(!cache.contains("u1", "10.0.0.1"));
        cache.record("u1", "10.0.0.1", 8);
        assert!(cache.contains("u1", "10.0.0.1"));
        assert!(!cache.contains("u1", "10.0.0.2"));
    }

    #[test]
    fn per_user_ip_list_is_bounded() {
        let mut cache = KnownIpCache::new();
        for i in 0..(MAX_IPS_PER_USER + 10) {
            cache.record("u1", &format!("10.0.0.{i}"), 8);
        }
        // The oldest addresses were evicted; the newest are retained.
        assert!(!cache.contains("u1", "10.0.0.0"));
        assert!(cache.contains("u1", &format!("10.0.0.{}", MAX_IPS_PER_USER + 9)));
    }

    #[test]
    fn user_count_is_bounded() {
        let mut cache = KnownIpCache::new();
        for i in 0..100 {
            cache.record(&format!("u{i}"), "10.0.0.1", 8);
        }
        assert_eq!(cache.tracked_users(), 8);
        assert!(!cache.contains("u0", "10.0.0.1"));
        assert!(cache.contains("u99", "10.0.0.1"));
    }

    #[test]
    fn zero_max_users_tracks_nothing() {
        let mut cache = KnownIpCache::new();
        cache.record("u1", "10.0.0.1", 0);
        assert_eq!(cache.tracked_users(), 0);
        assert!(!cache.contains("u1", "10.0.0.1"));
    }
}
