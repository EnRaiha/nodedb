// SPDX-License-Identifier: BUSL-1.1

//! How far each Event Plane consumer has delivered its core's events.
//!
//! A core numbers the events it emits onto its ring. A consumer delivers an
//! event once its trigger actions and committed message are durably held.
//! A Raft group snapshot reads every core's emitted counter under the
//! group's apply fence, then waits here until every consumer delivered
//! through it. The lane state it then captures holds every event of the
//! entries at or below the cut.
//!
//! An event the ring dropped is delivered only by WAL catch-up. A consumer
//! therefore owes every event from the first dropped number until its
//! catch-up completes. At boot it owes every event, including the ones the
//! previous process emitted, until the boot catch-up completes.

use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;

/// What one consumer delivered.
#[derive(Debug, Clone, Copy)]
struct CoreDelivery {
    /// Every event numbered at or below this was taken off the ring.
    taken: u64,
    /// The lowest event number a catch-up still owes. Zero owes every event.
    owes_from: Option<u64>,
}

impl CoreDelivery {
    /// A consumer that has not run its boot catch-up.
    const BOOT: Self = Self {
        taken: 0,
        owes_from: Some(0),
    };

    fn delivered_through(&self, emitted: u64) -> bool {
        self.taken >= emitted && self.owes_from.is_none_or(|from| from > emitted)
    }
}

/// The delivery of every consumer, in core order.
#[derive(Debug)]
pub struct DeliveredEvents {
    cores: Mutex<Vec<CoreDelivery>>,
    advanced: Notify,
}

impl DeliveredEvents {
    pub fn new(num_cores: usize) -> Self {
        Self {
            cores: Mutex::new(vec![CoreDelivery::BOOT; num_cores]),
            advanced: Notify::new(),
        }
    }

    fn update(&self, core: usize, change: impl FnOnce(&mut CoreDelivery)) {
        {
            let mut cores = self.cores.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(slot) = cores.get_mut(core) {
                change(slot);
            }
        }
        self.advanced.notify_waiters();
    }

    /// Record that `core` delivered every event it took, through `sequence`.
    pub fn note_taken(&self, core: usize, sequence: u64) {
        self.update(core, |slot| slot.taken = slot.taken.max(sequence));
    }

    /// Record that `core`'s ring dropped events numbered from `from`. A
    /// catch-up delivers them.
    pub fn note_dropped(&self, core: usize, from: u64) {
        self.update(core, |slot| {
            slot.owes_from = Some(slot.owes_from.map_or(from, |owed| owed.min(from)));
        });
    }

    /// Record that `core`'s catch-up delivered every event it owed.
    pub fn note_caught_up(&self, core: usize) {
        self.update(core, |slot| slot.owes_from = None);
    }

    /// Whether every consumer delivered through `emitted`, one emitted
    /// counter per core in core order.
    pub fn delivered_through(&self, emitted: &[u64]) -> bool {
        let cores = self.cores.lock().unwrap_or_else(|p| p.into_inner());
        emitted.iter().enumerate().all(|(core, emitted)| {
            cores
                .get(core)
                .is_some_and(|slot| slot.delivered_through(*emitted))
        })
    }

    /// Wait until every consumer delivered through `emitted`, for at most
    /// `limit`. Returns whether they did.
    pub async fn wait_through(&self, emitted: &[u64], limit: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            let advanced = self.advanced.notified();
            if self.delivered_through(emitted) {
                return true;
            }
            if tokio::time::timeout_at(deadline, advanced).await.is_err() {
                return self.delivered_through(emitted);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_consumer_owes_every_event_until_its_boot_catch_up() {
        let delivered = DeliveredEvents::new(1);
        delivered.note_taken(0, 4);
        assert!(!delivered.delivered_through(&[0]));
        delivered.note_caught_up(0);
        assert!(delivered.delivered_through(&[4]));
        assert!(!delivered.delivered_through(&[5]));
    }

    #[test]
    fn a_drop_owes_its_events_until_the_catch_up() {
        let delivered = DeliveredEvents::new(1);
        delivered.note_caught_up(0);
        delivered.note_taken(0, 3);
        delivered.note_dropped(0, 4);
        delivered.note_taken(0, 9);
        assert!(delivered.delivered_through(&[3]));
        assert!(!delivered.delivered_through(&[4]));
        delivered.note_caught_up(0);
        assert!(delivered.delivered_through(&[9]));
    }

    #[tokio::test]
    async fn a_wait_ends_once_every_core_passed_its_counter() {
        let delivered = DeliveredEvents::new(2);
        delivered.note_caught_up(0);
        delivered.note_caught_up(1);
        delivered.note_taken(0, 9);
        assert!(
            !delivered
                .wait_through(&[5, 5], Duration::from_millis(10))
                .await
        );
        delivered.note_taken(1, 5);
        assert!(
            delivered
                .wait_through(&[5, 5], Duration::from_millis(10))
                .await
        );
    }
}
