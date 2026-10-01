// SPDX-License-Identifier: BUSL-1.1

//! Per-peer monotonic sequence counters and a [`REPLAY_WINDOW`]-entry
//! sliding-window replay detector.
//!
//! # Outbound: [`PeerSeqSender`]
//!
//! Each local-node-to-peer direction has a distinct counter. Sent frames
//! carry strictly-increasing sequence numbers. Each boot of a node sends in
//! its own range, above its durable boot epoch shifted by
//! [`BOOT_EPOCH_SHIFT`], so a restarted node sends above every number an
//! earlier boot sent. `0` is reserved as a sentinel meaning "never sent".
//!
//! # Inbound: [`PeerSeqWindow`]
//!
//! A [`REPLAY_WINDOW`]-bit bitmap anchored at `last_accepted_seq`. Frame
//! with sequence `n` is:
//! - accepted and window advanced if `n > last_accepted_seq`
//! - accepted and bit set if `last_accepted_seq - (REPLAY_WINDOW - 1) <= n <
//!   last_accepted_seq` and the bit was previously unset
//! - rejected as replay if the bit was already set, or if `n` is older
//!   than the window
//!
//! The window model is IPsec AH/ESP's (RFC 4303 §3.4.3), with a wider
//! window. A sender numbers every frame from one counter, to every peer,
//! and runs many RPCs at once, each on its own QUIC stream. Frames reach a
//! peer out of order by up to the number the sender numbered while they
//! were in flight, across all its peers. A 64-entry window rejects a burst
//! of concurrent RPCs as stale; [`REPLAY_WINDOW`] covers every frame the
//! sender's concurrent streams hold at once.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{ClusterError, Result};

/// Size of the inbound replay-detection window, in sequence numbers. It is
/// 16 times a connection's default limit of concurrent streams, so frames a
/// sender numbered for other peers in the same moment fit as well.
pub const REPLAY_WINDOW: u64 = 4096;

/// 64-bit words in one peer's window bitmap.
const WINDOW_WORDS: usize = (REPLAY_WINDOW / 64) as usize;

/// Bits of a sequence number below the boot epoch. A boot owns `2^44`
/// (about 1.8 × 10^13) sequence numbers: at 100,000 frames a second that
/// lasts over five years of one boot's sending. Above the shift, `2^20`
/// (about a million) boots fit.
pub const BOOT_EPOCH_SHIFT: u32 = 44;

/// Outbound monotonic counter for this `AuthContext`. One counter total
/// — not one per target — because the receiver's replay window is keyed
/// by the *sender's* `local_node_id`. If this sender used a per-target
/// counter, two distinct targets' traffic would share the same window on
/// any node that receives from both: seq=1 from target=A and seq=1 from
/// target=B collide in the receiver's `window[sender_id]`. A single
/// counter makes every outbound seq globally unique per sender.
#[derive(Debug)]
pub struct PeerSeqSender {
    counter: AtomicU64,
}

impl Default for PeerSeqSender {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerSeqSender {
    /// A counter whose first sequence number is 1. A node moves it into its
    /// boot's own range with [`Self::enter_boot_epoch`] before it sends.
    pub fn new() -> Self {
        Self::starting_after(0)
    }

    /// A counter whose first sequence number is `floor + 1`.
    pub fn starting_after(floor: u64) -> Self {
        Self {
            counter: AtomicU64::new(floor),
        }
    }

    /// Move the counter into boot `epoch`'s range: its next sequence number
    /// is above `epoch << BOOT_EPOCH_SHIFT`. Never moves it back.
    ///
    /// A peer keeps its window for this node across this node's restart. A
    /// restarted node that counted from 1 again would send only sequence
    /// numbers the peer rejects as stale. The boot epoch rises durably at
    /// every boot (see `ClusterCatalog::advance_boot_epoch`), so each boot's
    /// range starts above every number an earlier boot sent: a boot sends
    /// fewer than `1 << BOOT_EPOCH_SHIFT` frames. No clock is read.
    pub fn enter_boot_epoch(&self, epoch: u64) {
        let floor = epoch.saturating_mul(1u64 << BOOT_EPOCH_SHIFT);
        self.counter.fetch_max(floor, Ordering::AcqRel);
    }

    /// Reserve and return the next outbound sequence number. Starts one
    /// above the counter's floor and is strictly increasing across all
    /// targets for this sender.
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Current counter value (0 if no frames have been sent). Test-only.
    #[cfg(test)]
    pub fn peek(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }
}

/// Per-peer inbound sliding-window replay detector. One window per
/// (local_node, remote_peer) pair.
#[derive(Default, Debug)]
pub struct PeerSeqWindow {
    windows: RwLock<HashMap<u64, WindowState>>,
}

/// Sliding-window state for one peer.
#[derive(Debug, Clone)]
struct WindowState {
    /// Highest accepted sequence seen from this peer. 0 if none yet.
    high: u64,
    /// Ring bitmap of accepted sequences in `[high - (REPLAY_WINDOW - 1),
    /// high]`. Sequence `s` sits at bit `s % REPLAY_WINDOW`.
    seen: [u64; WINDOW_WORDS],
}

impl Default for WindowState {
    fn default() -> Self {
        Self {
            high: 0,
            seen: [0; WINDOW_WORDS],
        }
    }
}

impl WindowState {
    /// The word and bit of sequence `seq` in the ring.
    fn slot(seq: u64) -> (usize, u64) {
        let pos = seq % REPLAY_WINDOW;
        ((pos / 64) as usize, 1u64 << (pos % 64))
    }

    fn is_set(&self, seq: u64) -> bool {
        let (word, bit) = Self::slot(seq);
        self.seen[word] & bit != 0
    }

    fn set(&mut self, seq: u64) {
        let (word, bit) = Self::slot(seq);
        self.seen[word] |= bit;
    }

    fn clear(&mut self, seq: u64) {
        let (word, bit) = Self::slot(seq);
        self.seen[word] &= !bit;
    }

    /// Move the window's top to `seq`, above `high`: every slot the window
    /// gains is cleared, as no sequence there was accepted yet.
    fn advance(&mut self, seq: u64) {
        let delta = seq - self.high;
        if delta >= REPLAY_WINDOW {
            self.seen = [0; WINDOW_WORDS];
        } else {
            for gained in self.high + 1..=seq {
                self.clear(gained);
            }
        }
        self.high = seq;
    }
}

impl PeerSeqWindow {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept `seq` from `peer_id`, rejecting replays and out-of-window
    /// stale frames. Returns `Err(ClusterError::Codec)` on rejection.
    ///
    /// Sequence `0` is always rejected — a well-formed sender starts at
    /// 1, so `0` means "nothing sent", which is not a valid inbound frame.
    pub fn accept(&self, peer_id: u64, seq: u64) -> Result<()> {
        if seq == 0 {
            return Err(ClusterError::Codec {
                detail: format!("peer {peer_id} sent reserved sequence 0"),
            });
        }

        let mut guard = self.windows.write().unwrap_or_else(|p| p.into_inner());
        let state = guard.entry(peer_id).or_default();

        if seq > state.high {
            // Frame advances the window.
            state.advance(seq);
            state.set(seq);
            return Ok(());
        }

        // Frame is `state.high - seq` positions back in the window.
        let offset = state.high - seq;
        if offset >= REPLAY_WINDOW {
            return Err(ClusterError::Codec {
                detail: format!(
                    "peer {peer_id} sent stale sequence {seq}, window high is {}",
                    state.high
                ),
            });
        }
        if state.is_set(seq) {
            return Err(ClusterError::Codec {
                detail: format!(
                    "peer {peer_id} replayed sequence {seq} (window high {})",
                    state.high
                ),
            });
        }
        state.set(seq);
        Ok(())
    }

    #[cfg(test)]
    pub fn highest(&self, peer_id: u64) -> u64 {
        let guard = self.windows.read().unwrap_or_else(|p| p.into_inner());
        guard.get(&peer_id).map(|w| w.high).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_counter_starts_above_its_floor() {
        let s = PeerSeqSender::starting_after(0);
        assert_eq!(s.next(), 1);
        assert_eq!(s.next(), 2);
        assert_eq!(s.next(), 3);
    }

    /// A restarted sender enters the next boot epoch and starts above
    /// everything the previous boot sent, so the peer's window accepts its
    /// first frame.
    #[test]
    fn a_restarted_sender_is_accepted_by_the_peers_window() {
        let window = PeerSeqWindow::new();
        let before = PeerSeqSender::new();
        before.enter_boot_epoch(1);
        for _ in 0..100 {
            window.accept(9, before.next()).unwrap();
        }
        let after = PeerSeqSender::new();
        after.enter_boot_epoch(2);
        window.accept(9, after.next()).unwrap();
    }

    /// The start depends on the boot epoch alone. A restart whose wall clock
    /// reads earlier than the previous boot's still starts above it: no
    /// clock goes into the sequence number.
    #[test]
    fn a_restart_with_an_earlier_clock_is_still_accepted() {
        let window = PeerSeqWindow::new();
        // The previous boot ran far into its range, as a long uptime does.
        let before = PeerSeqSender::starting_after((7u64 << BOOT_EPOCH_SHIFT) + 1_000_000);
        for _ in 0..10 {
            window.accept(9, before.next()).unwrap();
        }
        // The next boot comes up with its clock set back. Its boot epoch
        // is the durable counter plus one.
        let after = PeerSeqSender::new();
        after.enter_boot_epoch(8);
        window.accept(9, after.next()).unwrap();
    }

    #[test]
    fn entering_a_boot_epoch_never_moves_the_counter_back() {
        let s = PeerSeqSender::new();
        s.enter_boot_epoch(3);
        let first = s.next();
        assert_eq!(first, (3u64 << BOOT_EPOCH_SHIFT) + 1);
        s.enter_boot_epoch(2);
        assert_eq!(s.next(), first + 1);
    }

    #[test]
    fn outbound_counter_is_single_across_all_targets() {
        // The outbound counter is intentionally shared across targets: the
        // receiver's replay window is keyed by the sender's local_node_id,
        // so per-target counters would collide in the same window. A
        // single monotonic counter guarantees every emitted seq is unique
        // from the receiver's point of view regardless of which target
        // the sender was aiming at.
        let s = PeerSeqSender::starting_after(0);
        assert_eq!(s.next(), 1);
        assert_eq!(s.next(), 2);
        assert_eq!(s.next(), 3);
        assert_eq!(s.next(), 4);
    }

    #[test]
    fn window_accepts_monotonic_sequence() {
        let w = PeerSeqWindow::new();
        for seq in 1..=10 {
            w.accept(7, seq).unwrap();
        }
        assert_eq!(w.highest(7), 10);
    }

    #[test]
    fn window_rejects_immediate_replay() {
        let w = PeerSeqWindow::new();
        w.accept(1, 1).unwrap();
        let err = w.accept(1, 1).unwrap_err();
        assert!(err.to_string().contains("replayed"));
    }

    #[test]
    fn window_rejects_zero_sequence() {
        let w = PeerSeqWindow::new();
        let err = w.accept(1, 0).unwrap_err();
        assert!(err.to_string().contains("reserved sequence 0"));
    }

    #[test]
    fn window_accepts_in_order_gap_within_window() {
        let w = PeerSeqWindow::new();
        // 1 ... 5 arrive out of order but within window.
        w.accept(1, 5).unwrap();
        w.accept(1, 3).unwrap();
        w.accept(1, 1).unwrap();
        w.accept(1, 2).unwrap();
        w.accept(1, 4).unwrap();
        assert_eq!(w.highest(1), 5);
    }

    #[test]
    fn window_rejects_replay_within_window() {
        let w = PeerSeqWindow::new();
        w.accept(1, 5).unwrap();
        w.accept(1, 3).unwrap();
        let err = w.accept(1, 3).unwrap_err();
        assert!(err.to_string().contains("replayed"));
    }

    #[test]
    fn window_rejects_stale_outside_window() {
        let w = PeerSeqWindow::new();
        let high = 10_000;
        w.accept(1, high).unwrap();
        // The window is [high - REPLAY_WINDOW + 1, high].
        let stale = high - REPLAY_WINDOW;
        let err = w.accept(1, stale).unwrap_err();
        assert!(err.to_string().contains(&format!("stale sequence {stale}")));
        // The window's lowest sequence is acceptable.
        w.accept(1, stale + 1).unwrap();
    }

    /// A burst of concurrent RPCs arrives out of order by more than 64
    /// frames. Every frame is accepted once.
    #[test]
    fn a_reordered_burst_wider_than_64_frames_is_accepted() {
        let w = PeerSeqWindow::new();
        let burst: Vec<u64> = (1..=600).collect();
        for &seq in burst.iter().rev() {
            w.accept(3, seq).unwrap();
        }
        for &seq in &burst {
            assert!(w.accept(3, seq).is_err(), "sequence {seq} replays");
        }
    }

    /// A slot the window moves past is free again for the sequence that
    /// lands on it one window later.
    #[test]
    fn a_ring_slot_is_reused_after_the_window_moves_past_it() {
        let w = PeerSeqWindow::new();
        w.accept(1, 5).unwrap();
        w.accept(1, 5 + REPLAY_WINDOW).unwrap();
        w.accept(1, 4 + REPLAY_WINDOW).unwrap();
        assert!(w.accept(1, 5 + REPLAY_WINDOW).is_err());
    }

    #[test]
    fn window_advances_beyond_window_clears_mask() {
        let w = PeerSeqWindow::new();
        w.accept(1, 1).unwrap();
        w.accept(1, 2).unwrap();
        w.accept(1, 100 + REPLAY_WINDOW).unwrap();
        // Sequences 1, 2 are now outside the window anchored at 100 and
        // must be rejected on replay (not accepted as fresh within mask).
        let err = w.accept(1, 1).unwrap_err();
        assert!(err.to_string().contains("stale sequence 1"));
    }

    #[test]
    fn windows_are_independent_per_peer() {
        let w = PeerSeqWindow::new();
        w.accept(1, 10).unwrap();
        w.accept(2, 10).unwrap();
        w.accept(1, 9).unwrap();
        w.accept(2, 9).unwrap();
        // Independent — neither is a replay.
        assert_eq!(w.highest(1), 10);
        assert_eq!(w.highest(2), 10);
    }
}
