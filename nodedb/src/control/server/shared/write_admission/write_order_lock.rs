// SPDX-License-Identifier: BUSL-1.1

//! Per-key FIFO write-ordering lock for the single-node (no-Calvin) fast path.
//!
//! When a write's vShard has NO deterministic Calvin scheduler registered, the
//! gate has no lock table to fence against — yet two concurrent same-key
//! autocommit writes must still serialize so that WAL-LSN order equals
//! Data-Plane apply order per key. [`KeyedWriteOrderLock`] closes that gap: it
//! hands out ONE `tokio::sync::Mutex<()>` per [`LockKey`]. `tokio::sync::Mutex`
//! is FIFO-fair, so N concurrent same-key waiters are admitted in arrival order
//! (acquire-order == LSN-order == enqueue-order). Distinct keys map to distinct
//! mutexes and therefore never contend.
//!
//! The per-key mutex map is guarded by a set of sharded `std::sync::Mutex`es
//! (sharded by key hash to cut contention). Each entry is a `Weak` handle, so
//! when a key's last `OwnedMutexGuard` (and thus its last `Arc`) drops, the
//! entry becomes reclaimable and is reaped on a later touch of the same shard —
//! the map never grows unbounded. A std shard mutex is held only for the O(1)
//! get-or-insert (plus an amortized reap), NEVER across the `.await`.
//!
//! [`LockKey`]: crate::control::cluster::calvin::scheduler::lock_manager::LockKey

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::control::cluster::calvin::scheduler::lock_manager::LockKey;

/// Number of independent map shards. A power of two so `hash % SHARDS` spreads
/// evenly; sized to keep std-mutex contention negligible under many-core write
/// fan-in while staying a trivial fixed allocation.
const SHARDS: usize = 64;

/// The smallest map size at which a shard reaps dead `Weak` entries. Below this
/// the map is tiny and reaping is not worth the scan.
const MIN_REAP: usize = 16;

/// One shard of the key → per-key-mutex map, plus the reap trigger.
struct Shard {
    /// Live and recently-dead per-key async mutexes, held as `Weak` so an idle
    /// key's entry is reclaimable once its last guard drops.
    map: HashMap<LockKey, Weak<AsyncMutex<()>>>,
    /// Reap dead entries once `map.len()` reaches this many entries. Reset to
    /// `2 × live` after each reap, giving amortized O(1) insertion.
    reap_at: usize,
}

impl Shard {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            reap_at: MIN_REAP,
        }
    }
}

/// A keyed, FIFO-fair async lock: one `tokio::sync::Mutex<()>` per lock key,
/// created on first touch and reaped when idle.
pub struct KeyedWriteOrderLock {
    shards: [Mutex<Shard>; SHARDS],
}

impl KeyedWriteOrderLock {
    /// Create an empty keyed lock. No per-key mutexes exist until first touch.
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(Shard::new())),
        }
    }

    /// Acquire the ordering lock for `key`, awaiting FIFO-fairly behind any
    /// concurrent same-key holder/waiter. The returned `OwnedMutexGuard` keeps
    /// the per-key mutex alive for as long as it is held; drop it to release.
    ///
    /// The internal shard mutex is taken and released synchronously inside
    /// [`Self::mutex_for`] — it is never held across the `.await` below.
    pub async fn lock_owned(&self, key: LockKey) -> OwnedMutexGuard<()> {
        self.mutex_for(key).lock_owned().await
    }

    /// Shard index for `key`.
    fn shard_index(key: &LockKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & (SHARDS - 1)
    }

    /// Get-or-create the `Arc<Mutex<()>>` for `key`. Holds the shard's std mutex
    /// only for this O(1) lookup + insert (plus an amortized dead-entry reap);
    /// returns before any `.await`. A warm key allocates nothing beyond the
    /// `Arc` clone (an atomic increment) — no heap allocation.
    fn mutex_for(&self, key: LockKey) -> Arc<AsyncMutex<()>> {
        let shard = &self.shards[Self::shard_index(&key)];
        let mut shard = shard.lock().unwrap_or_else(|p| p.into_inner());

        // Warm key: an existing, still-live mutex — clone its `Arc` and return.
        if let Some(existing) = shard.map.get(&key).and_then(Weak::upgrade) {
            return existing;
        }

        // Miss (absent, or a dead `Weak` we are about to overwrite). Reap dead
        // entries when the map has grown past its trigger so it never leaks the
        // graves of one-shot keys, then insert the freshly-created mutex.
        if shard.map.len() >= shard.reap_at {
            shard.map.retain(|_, weak| weak.strong_count() > 0);
            shard.reap_at = shard.map.len().saturating_mul(2).max(MIN_REAP);
        }
        let created = Arc::new(AsyncMutex::new(()));
        shard.map.insert(key, Arc::downgrade(&created));
        created
    }
}

impl Default for KeyedWriteOrderLock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn key(k: &[u8]) -> LockKey {
        LockKey::Kv {
            collection: Arc::from("c"),
            key: Arc::from(k),
        }
    }

    /// A warm key returns the SAME underlying mutex on every touch (no new
    /// allocation) and drops its map entry once the last guard is released.
    #[tokio::test]
    async fn warm_key_reuses_mutex_and_reaps_when_idle() {
        let lock = KeyedWriteOrderLock::new();
        let k = key(b"a");

        {
            let _g = lock.lock_owned(k.clone()).await;
            // While held, the entry is live and upgradeable.
            let idx = KeyedWriteOrderLock::shard_index(&k);
            let shard = lock.shards[idx].lock().expect("shard");
            assert!(
                shard.map.get(&k).and_then(Weak::upgrade).is_some(),
                "held key must have a live entry"
            );
        }
        // After the guard drops the last strong ref is gone → the Weak is dead.
        let idx = KeyedWriteOrderLock::shard_index(&k);
        let dead = {
            let shard = lock.shards[idx].lock().expect("shard");
            shard.map.get(&k).and_then(Weak::upgrade).is_none()
        };
        assert!(dead, "idle key's mutex must be released");
    }

    /// Concurrent same-key acquisitions are mutually exclusive AND admitted in
    /// arrival (FIFO) order. Deterministic under the current-thread test runtime:
    /// each spawned waiter is polled to its park point before the next spawns.
    #[tokio::test]
    async fn same_key_serializes_fifo() {
        let lock = Arc::new(KeyedWriteOrderLock::new());
        let k = key(b"K");
        let order = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));

        // Holder acquires first and parks the two waiters behind it.
        let held = lock.lock_owned(k.clone()).await;

        let mut handles = Vec::new();
        for id in [1u32, 2u32] {
            let lock = Arc::clone(&lock);
            let order = Arc::clone(&order);
            let k = k.clone();
            let h = tokio::spawn(async move {
                let _g = lock.lock_owned(k).await;
                order.lock().expect("order").push(id);
            });
            handles.push(h);
            // Let the just-spawned waiter run until it parks on the held mutex,
            // fixing its position in the mutex's FIFO wait queue.
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }

        // No waiter may proceed while the holder is live.
        assert!(
            order.lock().expect("order").is_empty(),
            "same-key waiters must block behind the holder"
        );

        // Release; waiters drain in the order they parked.
        drop(held);
        for h in handles {
            h.await.expect("waiter");
        }
        assert_eq!(
            *order.lock().expect("order"),
            vec![1, 2],
            "same-key waiters must acquire in FIFO arrival order"
        );
    }

    /// Distinct keys never block each other — both guards are held at once.
    #[tokio::test]
    async fn distinct_keys_do_not_block() {
        let lock = KeyedWriteOrderLock::new();
        let g1 = lock.lock_owned(key(b"one")).await;
        let g2 = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            lock.lock_owned(key(b"two")),
        )
        .await
        .expect("a distinct key must not block on a held key");
        // Both alive simultaneously — proof of non-contention.
        drop(g2);
        drop(g1);
    }
}
