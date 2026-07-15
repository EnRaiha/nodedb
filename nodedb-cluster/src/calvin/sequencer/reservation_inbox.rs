// SPDX-License-Identifier: BUSL-1.1

//! Dedicated inbox for hot-key read-reservation requests.
//!
//! Mirrors [`super::inbox::Inbox`] but carries [`ReservationRequest`]s from the
//! Control Plane to the [`super::service::SequencerService`] tick loop. Keeping
//! reservations on their own channel means no `MultiRaft` handle ever leaks to
//! Control-Plane code: the CP submits a request and (for a mint) awaits the
//! minted owner id on a oneshot, while only the sequencer leader touches Raft.
//!
//! Only the sequencer leader services these requests (see
//! [`super::service::SequencerService::process_reservations`]); a follower
//! drains and discards them, which drops each `Reserve`'s `reply` sender and
//! makes the CP awaiter observe a closed channel — correct degradation to plain
//! OCC.

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::warn;

use crate::calvin::sequencer::error::SequencerError;
use crate::calvin::types::{LockKeyWire, ReleaseReason, TxnIdWire};

/// A reservation request submitted by the Control Plane.
///
/// `Reserve` mints (or echoes) a stable reservation id `R = (epoch, position)`
/// and installs a shared lock on `key`; `Release` drops all of an owner's
/// reservations on a vshard.
pub enum ReservationRequest {
    /// Reserve `key` on `vshard` for an interactive txn.
    ///
    /// `owner` is `None` to mint a fresh `R` for a brand-new interactive txn, or
    /// `Some(existing)` to reserve an additional key under a txn's already-minted
    /// `R` (no new mint). `reply` returns the owner id (minted or echoed); a
    /// dropped `reply` sender signals the caller to fall back to plain OCC.
    Reserve {
        key: LockKeyWire,
        vshard: u32,
        owner: Option<TxnIdWire>,
        reply: oneshot::Sender<TxnIdWire>,
    },
    /// Release all of `owner`'s shared reservations on `vshard`.
    Release {
        owner: TxnIdWire,
        vshard: u32,
        reason: ReleaseReason,
    },
}

/// The sending half of the reservation inbox.
///
/// Held by the Control Plane (in a `OnceLock`) and cloned per call as needed.
/// Submission is lock-free via the underlying bounded mpsc channel.
#[derive(Clone)]
pub struct ReservationInbox {
    tx: mpsc::Sender<ReservationRequest>,
}

/// The receiving half of the reservation inbox.
///
/// Held exclusively by [`super::service::SequencerService`]. Not `Clone`.
pub struct ReservationInboxReceiver {
    rx: mpsc::Receiver<ReservationRequest>,
}

impl ReservationInbox {
    /// Submit a reservation request and return the oneshot receiver on which the
    /// minted (or echoed) owner id will arrive.
    ///
    /// `owner` is `None` to mint a fresh `R`, or `Some(existing)` to reserve an
    /// additional key under a txn's existing `R`. Non-blocking: maps a full or
    /// closed channel to [`SequencerError::Overloaded`], mirroring
    /// [`super::inbox::Inbox::submit`]'s overflow behavior.
    pub fn submit_reserve(
        &self,
        key: LockKeyWire,
        vshard: u32,
        owner: Option<TxnIdWire>,
    ) -> Result<oneshot::Receiver<TxnIdWire>, SequencerError> {
        let (reply, reply_rx) = oneshot::channel();
        let request = ReservationRequest::Reserve {
            key,
            vshard,
            owner,
            reply,
        };
        match self.tx.try_send(request) {
            Ok(()) => Ok(reply_rx),
            Err(mpsc::error::TrySendError::Full(_)) => Err(SequencerError::Overloaded),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("reservation inbox channel closed; sequencer service may have exited");
                Err(SequencerError::Overloaded)
            }
        }
    }

    /// Submit a fire-and-forget release for all of `owner`'s reservations on
    /// `vshard`. Non-blocking; maps a full or closed channel to
    /// [`SequencerError::Overloaded`].
    pub fn submit_release(
        &self,
        owner: TxnIdWire,
        vshard: u32,
        reason: ReleaseReason,
    ) -> Result<(), SequencerError> {
        let request = ReservationRequest::Release {
            owner,
            vshard,
            reason,
        };
        match self.tx.try_send(request) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(SequencerError::Overloaded),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("reservation inbox channel closed; sequencer service may have exited");
                Err(SequencerError::Overloaded)
            }
        }
    }
}

impl ReservationInboxReceiver {
    /// Drain every currently-queued request into `out`, returning the number
    /// appended. Non-blocking: stops as soon as the channel is empty. Mirrors
    /// [`super::inbox::InboxReceiver::drain_all_discard`]'s drain style.
    pub fn drain_into(&mut self, out: &mut Vec<ReservationRequest>) -> usize {
        let mut count = 0;
        while let Ok(request) = self.rx.try_recv() {
            out.push(request);
            count += 1;
        }
        count
    }

    /// Drain and discard every currently-queued request, returning the number
    /// discarded. Used by the non-leader path: dropping a `Reserve`'s `reply`
    /// sender makes the CP awaiter observe a closed channel and fall back to OCC.
    pub fn drain_all_discard(&mut self) -> usize {
        let mut count = 0;
        while self.rx.try_recv().is_ok() {
            count += 1;
        }
        count
    }
}

/// Build a linked (`ReservationInbox`, `ReservationInboxReceiver`) pair backed by
/// a bounded mpsc channel of the given `capacity`.
pub fn new_reservation_inbox(capacity: usize) -> (ReservationInbox, ReservationInboxReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    (ReservationInbox { tx }, ReservationInboxReceiver { rx })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_key() -> LockKeyWire {
        LockKeyWire::Kv {
            collection: "sessions".to_owned(),
            key: b"hot".to_vec(),
        }
    }

    #[test]
    fn submit_reserve_enqueues_and_returns_receiver() {
        let (inbox, mut rx) = new_reservation_inbox(4);
        let _reply_rx = inbox
            .submit_reserve(sample_key(), 7, None)
            .expect("submit should succeed");
        let mut out = Vec::new();
        let n = rx.drain_into(&mut out);
        assert_eq!(n, 1);
        assert!(matches!(
            out[0],
            ReservationRequest::Reserve {
                vshard: 7,
                owner: None,
                ..
            }
        ));
    }

    #[test]
    fn submit_reserve_returns_overloaded_when_full() {
        let (inbox, _rx) = new_reservation_inbox(1);
        let _rx = inbox
            .submit_reserve(sample_key(), 1, None)
            .expect("first submit fills channel");
        let err = inbox
            .submit_reserve(sample_key(), 1, None)
            .expect_err("second submit should be overloaded");
        assert_eq!(err, SequencerError::Overloaded);
    }

    #[test]
    fn submit_release_is_fire_and_forget() {
        let (inbox, mut rx) = new_reservation_inbox(4);
        inbox
            .submit_release(
                TxnIdWire {
                    epoch: 3,
                    position: 9,
                },
                2,
                ReleaseReason::Commit,
            )
            .expect("release should enqueue");
        let mut out = Vec::new();
        assert_eq!(rx.drain_into(&mut out), 1);
        assert!(matches!(
            out[0],
            ReservationRequest::Release {
                vshard: 2,
                reason: ReleaseReason::Commit,
                ..
            }
        ));
    }

    #[test]
    fn drain_all_discard_drops_reply_senders() {
        let (inbox, mut rx) = new_reservation_inbox(4);
        let reply_rx = inbox.submit_reserve(sample_key(), 1, None).expect("submit");
        let discarded = rx.drain_all_discard();
        assert_eq!(discarded, 1);
        // The dropped reply sender makes the awaiter observe a closed channel.
        assert!(reply_rx.blocking_recv().is_err());
    }

    #[test]
    fn submit_reserve_echoes_existing_owner() {
        let (inbox, mut rx) = new_reservation_inbox(4);
        let existing = TxnIdWire {
            epoch: 5,
            position: 1 << 31,
        };
        inbox
            .submit_reserve(sample_key(), 4, Some(existing))
            .expect("submit");
        let mut out = Vec::new();
        rx.drain_into(&mut out);
        match &out[0] {
            ReservationRequest::Reserve { owner, .. } => {
                assert_eq!(*owner, Some(existing));
            }
            _ => panic!("expected Reserve"),
        }
    }
}
