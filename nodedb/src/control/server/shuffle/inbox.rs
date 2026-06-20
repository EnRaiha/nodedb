// SPDX-License-Identifier: BUSL-1.1

//! Cross-node streaming-shuffle receiver registry + per-part inbox (E1).
//!
//! # Plane discipline
//!
//! This registry is **Send + Sync** and lives in the Control Plane's
//! `SharedState`. Its inbox buffer is consumed (in a later unit, E3) by the
//! `!Send` Data Plane, which cannot await Tokio futures. The inbox therefore
//! uses **std primitives only** — a bounded [`std::sync::Mutex`]-guarded
//! [`VecDeque`] plus a [`std::sync::Condvar`] — never `tokio::sync::mpsc`.
//! Producers block on the condvar when the buffer is full (bounded
//! back-pressure, which propagates to QUIC flow control through the transport
//! read-loop), and the Data Plane drains via [`ShuffleInbox::pop`] /
//! [`ShuffleInbox::try_drain`].
//!
//! # Build barrier
//!
//! Each inbox tracks how many distinct producers (`producer_count`) are
//! expected to push to this `(shuffle_id, part, side)`. The build side of the
//! join is complete only once an `End` frame has been received from **all** of
//! them — see [`ShuffleInbox::record_end`] / [`ShuffleInbox::barrier_complete`].

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use nodedb_cluster::TypedClusterError;

/// Key for one shuffle receiver inbox: `(shuffle_id, part, side)`.
///
/// `side` is `0` for the build side and `1` for the probe side of a hash join.
pub type ShuffleKey = (u64, u32, u8);

/// A bounded receiver inbox for one `(shuffle_id, part, side)`.
///
/// Holds the chunk payloads pushed by all producers for this part, plus the
/// per-part build barrier state.
pub struct ShuffleInbox {
    /// Bounded buffer of chunk payloads (each a standalone msgpack row array).
    /// Guarded by a std `Mutex`; producers wait on `not_full` when at capacity,
    /// consumers are notified via `not_empty`.
    buffer: Mutex<VecDeque<Vec<u8>>>,
    /// Maximum number of buffered payloads before producers block.
    capacity: usize,
    /// Signalled when a payload is popped (a producer may resume pushing).
    not_full: Condvar,
    /// Signalled when a payload is pushed (a waiting consumer may resume).
    not_empty: Condvar,
    /// Number of producers expected to push to this part. The barrier fires
    /// once `ends_received == producer_count`.
    producer_count: usize,
    /// Count of `End` frames received so far (one per finished producer).
    ends_received: AtomicUsize,
    /// First terminal error reported by any producer, if any.
    error: Mutex<Option<TypedClusterError>>,
}

impl ShuffleInbox {
    /// Create an empty inbox expecting `producer_count` producers, buffering at
    /// most `capacity` payloads before back-pressuring producers.
    ///
    /// `capacity` is clamped to at least 1 so a zero never deadlocks `push`.
    pub fn new(producer_count: usize, capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
            producer_count: producer_count.max(1),
            ends_received: AtomicUsize::new(0),
            error: Mutex::new(None),
        }
    }

    /// Number of producers expected for this part.
    pub fn producer_count(&self) -> usize {
        self.producer_count
    }

    /// Push one chunk payload, blocking while the buffer is at capacity.
    ///
    /// Bounded back-pressure: the calling task (the transport read-loop) blocks
    /// on the `not_full` condvar until a consumer pops, then deposits the
    /// payload and wakes a waiting consumer. Never allocates beyond `capacity`.
    pub fn push(&self, payload: Vec<u8>) {
        let mut buf = self.buffer.lock().unwrap_or_else(|p| p.into_inner());
        while buf.len() >= self.capacity {
            buf = self.not_full.wait(buf).unwrap_or_else(|p| p.into_inner());
        }
        buf.push_back(payload);
        drop(buf);
        self.not_empty.notify_one();
    }

    /// Pop the oldest buffered payload, or `None` if the buffer is empty.
    ///
    /// Non-blocking — the E3 Data Plane consumer polls this. Wakes one producer
    /// blocked on a full buffer.
    pub fn pop(&self) -> Option<Vec<u8>> {
        let mut buf = self.buffer.lock().unwrap_or_else(|p| p.into_inner());
        let item = buf.pop_front();
        drop(buf);
        if item.is_some() {
            self.not_full.notify_one();
        }
        item
    }

    /// Drain and return all currently-buffered payloads in FIFO order.
    ///
    /// Wakes all producers blocked on a full buffer.
    pub fn try_drain(&self) -> Vec<Vec<u8>> {
        let mut buf = self.buffer.lock().unwrap_or_else(|p| p.into_inner());
        let drained: Vec<Vec<u8>> = buf.drain(..).collect();
        drop(buf);
        if !drained.is_empty() {
            self.not_full.notify_all();
        }
        drained
    }

    /// Number of payloads currently buffered (not yet drained).
    pub fn buffered_len(&self) -> usize {
        self.buffer.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Record one producer's `End` frame.
    ///
    /// Returns `true` when this `End` completes the barrier (i.e.
    /// `ends_received == producer_count`), meaning every expected producer has
    /// finished and the build side for this part is complete.
    pub fn record_end(&self) -> bool {
        let prev = self.ends_received.fetch_add(1, Ordering::AcqRel);
        prev + 1 >= self.producer_count
    }

    /// Number of `End` frames received so far.
    pub fn ends_received(&self) -> usize {
        self.ends_received.load(Ordering::Acquire)
    }

    /// `true` once an `End` has been received from every expected producer.
    pub fn barrier_complete(&self) -> bool {
        self.ends_received.load(Ordering::Acquire) >= self.producer_count
    }

    /// Capture a terminal error reported by a producer (first writer wins).
    pub fn set_error(&self, error: TypedClusterError) {
        let mut slot = self.error.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    /// Take the captured terminal error, if any, leaving `None` behind.
    pub fn take_error(&self) -> Option<TypedClusterError> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

/// Registry of [`ShuffleInbox`]es keyed by `(shuffle_id, part, side)`.
///
/// Owned by `SharedState` (`Send + Sync`). The transport read-loop creates and
/// feeds inboxes through the [`nodedb_cluster::ShuffleReceiver`] hook; the E3
/// Data Plane drains them.
pub struct ShuffleReceiverRegistry {
    inboxes: Mutex<HashMap<ShuffleKey, Arc<ShuffleInbox>>>,
}

impl Default for ShuffleReceiverRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ShuffleReceiverRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            inboxes: Mutex::new(HashMap::new()),
        }
    }

    /// Get the inbox for `(shuffle_id, part, side)`, lazily creating it on the
    /// first frame with the carried `producer_count` and buffer `capacity`.
    ///
    /// Idempotent: subsequent frames for the same key reuse the existing inbox
    /// (the `producer_count` / `capacity` of the first creator win).
    pub fn get_or_create(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        producer_count: usize,
        capacity: usize,
    ) -> Arc<ShuffleInbox> {
        let key = (shuffle_id, part, side);
        let mut map = self.inboxes.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(
            map.entry(key)
                .or_insert_with(|| Arc::new(ShuffleInbox::new(producer_count, capacity))),
        )
    }

    /// Look up an existing inbox without creating one.
    pub fn get(&self, key: ShuffleKey) -> Option<Arc<ShuffleInbox>> {
        self.inboxes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .map(Arc::clone)
    }

    /// Remove every inbox belonging to `shuffle_id` (all parts and sides).
    ///
    /// Called when a shuffle completes or is cancelled so its buffers are
    /// released.
    pub fn unregister_shuffle(&self, shuffle_id: u64) {
        self.inboxes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(sid, _, _), _| *sid != shuffle_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_fires_only_after_all_producers_end() {
        let inbox = ShuffleInbox::new(2, 8);
        assert!(!inbox.barrier_complete());
        // First End: not complete.
        assert!(!inbox.record_end());
        assert!(!inbox.barrier_complete());
        assert_eq!(inbox.ends_received(), 1);
        // Second End: barrier complete.
        assert!(inbox.record_end());
        assert!(inbox.barrier_complete());
        assert_eq!(inbox.ends_received(), 2);
    }

    #[test]
    fn single_producer_barrier_fires_on_first_end() {
        let inbox = ShuffleInbox::new(1, 8);
        assert!(!inbox.barrier_complete());
        assert!(inbox.record_end());
        assert!(inbox.barrier_complete());
    }

    #[test]
    fn push_pop_is_fifo() {
        let inbox = ShuffleInbox::new(1, 8);
        inbox.push(vec![1]);
        inbox.push(vec![2]);
        inbox.push(vec![3]);
        assert_eq!(inbox.buffered_len(), 3);
        assert_eq!(inbox.pop(), Some(vec![1]));
        assert_eq!(inbox.pop(), Some(vec![2]));
        assert_eq!(inbox.pop(), Some(vec![3]));
        assert_eq!(inbox.pop(), None);
    }

    #[test]
    fn try_drain_returns_all_in_order() {
        let inbox = ShuffleInbox::new(1, 8);
        inbox.push(vec![10]);
        inbox.push(vec![20]);
        let drained = inbox.try_drain();
        assert_eq!(drained, vec![vec![10], vec![20]]);
        assert_eq!(inbox.buffered_len(), 0);
        assert!(inbox.try_drain().is_empty());
    }

    #[test]
    fn bounded_push_blocks_until_pop() {
        // capacity 1: a second push from another thread must block until the
        // first payload is popped, proving the bound is enforced.
        let inbox = Arc::new(ShuffleInbox::new(1, 1));
        inbox.push(vec![1]);
        assert_eq!(inbox.buffered_len(), 1);

        let producer = {
            let inbox = Arc::clone(&inbox);
            std::thread::spawn(move || {
                inbox.push(vec![2]); // blocks until main pops
            })
        };

        // Give the producer a chance to block on the full buffer.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(inbox.buffered_len(), 1, "buffer must stay at capacity");

        // Pop frees a slot; the blocked producer can now push.
        assert_eq!(inbox.pop(), Some(vec![1]));
        producer.join().expect("producer thread");
        assert_eq!(inbox.pop(), Some(vec![2]));
    }

    #[test]
    fn error_capture_first_writer_wins() {
        let inbox = ShuffleInbox::new(1, 8);
        assert!(inbox.take_error().is_none());
        inbox.set_error(TypedClusterError::Internal {
            code: 1,
            message: "first".into(),
        });
        inbox.set_error(TypedClusterError::Internal {
            code: 2,
            message: "second".into(),
        });
        match inbox.take_error() {
            Some(TypedClusterError::Internal { code, .. }) => assert_eq!(code, 1),
            other => panic!("expected first Internal error, got {other:?}"),
        }
        // Taken — now empty.
        assert!(inbox.take_error().is_none());
    }

    #[test]
    fn registry_get_or_create_is_idempotent() {
        let reg = ShuffleReceiverRegistry::new();
        let a = reg.get_or_create(7, 0, 0, 2, 16);
        let b = reg.get_or_create(7, 0, 0, 99, 99);
        assert!(Arc::ptr_eq(&a, &b), "same key must reuse the same inbox");
        // First creator's producer_count wins.
        assert_eq!(a.producer_count(), 2);
        // A different key gets a distinct inbox.
        let c = reg.get_or_create(7, 1, 0, 1, 16);
        assert!(!Arc::ptr_eq(&a, &c));
    }

    #[test]
    fn registry_get_returns_none_for_missing() {
        let reg = ShuffleReceiverRegistry::new();
        assert!(reg.get((1, 0, 0)).is_none());
        reg.get_or_create(1, 0, 0, 1, 8);
        assert!(reg.get((1, 0, 0)).is_some());
    }

    #[test]
    fn unregister_shuffle_removes_only_matching_id() {
        let reg = ShuffleReceiverRegistry::new();
        reg.get_or_create(1, 0, 0, 1, 8);
        reg.get_or_create(1, 1, 1, 1, 8);
        reg.get_or_create(2, 0, 0, 1, 8);
        reg.unregister_shuffle(1);
        assert!(reg.get((1, 0, 0)).is_none());
        assert!(reg.get((1, 1, 1)).is_none());
        assert!(reg.get((2, 0, 0)).is_some());
    }
}
