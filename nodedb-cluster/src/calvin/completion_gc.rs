// SPDX-License-Identifier: BUSL-1.1

//! Eviction of completion entries no waiter will collect.
//!
//! Every sequencer replica applies every `CompletionAck`, so every replica
//! builds a completion entry for every transaction, and the entry holds each
//! participant's apply result. Only the replica on the coordinator's node
//! gains a waiter. A terminal entry keeps its outcome so a waiter that
//! registers after the last ack still receives it. A coordinator registers
//! within its statement deadline, so a terminal entry that gained no waiter
//! within the eviction window never will, and is removed with its results.
//!
//! An entry becomes terminal when every expected participant acked, or when
//! an OLLP mismatch or a routing failure is recorded for it. A terminal
//! entry with no waiter joins a queue stamped with the instant it did. Each
//! later registry change sweeps the queue front, so the queue holds at most
//! the entries of one window.

use std::time::{Duration, Instant};

use super::completion::{Inner, TxnId};

/// The eviction window when the host sets none. Longer than any statement
/// deadline, so a coordinator's waiter always registers inside it.
pub const DEFAULT_WAITERLESS_TTL: Duration = Duration::from_secs(120);

impl Inner {
    /// Queue `txn`'s entry when it just became a waiterless terminal entry,
    /// then evict every queued entry whose window passed.
    pub(crate) fn settle_waiterless(&mut self, txn: TxnId) {
        // no-determinism: node-local eviction of finished entries; never in the log.
        let now = Instant::now();
        self.park_waiterless(txn, now);
        self.sweep_waiterless(now);
    }

    /// Queue `txn`'s entry for eviction when it is terminal, has no waiter,
    /// and is not queued yet.
    pub(crate) fn park_waiterless(&mut self, txn: TxnId, now: Instant) {
        let Some(entry) = self.completions.get_mut(&txn) else {
            return;
        };
        if entry.parked || entry.has_waiter() || !entry.is_terminal() {
            return;
        }
        entry.parked = true;
        self.waiterless.push_back((now, txn));
    }

    /// Remove every queued entry that stayed waiterless for the whole
    /// window.
    pub(crate) fn sweep_waiterless(&mut self, now: Instant) {
        let ttl = self.waiterless_ttl.unwrap_or(DEFAULT_WAITERLESS_TTL);
        while let Some(&(since, txn)) = self.waiterless.front() {
            if now.saturating_duration_since(since) < ttl {
                break;
            }
            self.waiterless.pop_front();
            if self
                .completions
                .get(&txn)
                .is_some_and(|entry| !entry.has_waiter())
            {
                self.completions.remove(&txn);
            }
        }
    }
}
