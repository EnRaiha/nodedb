// SPDX-License-Identifier: BUSL-1.1

//! The probe round in flight, polled beside the detector's inbound arm.
//!
//! A probe round waits for acks, and only the run loop's inbound arm reads
//! them off the transport. The loop therefore never awaits a round inline.
//! It parks the round here and polls it as one `select!` arm, so inbound
//! datagrams keep flowing while the round waits.

use std::future::Future;
use std::pin::Pin;

type RoundFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// At most one probe round in flight.
#[derive(Default)]
pub(super) struct RoundSlot<'a> {
    round: Option<RoundFuture<'a>>,
}

impl<'a> RoundSlot<'a> {
    /// Whether no round is in flight.
    pub(super) fn is_idle(&self) -> bool {
        self.round.is_none()
    }

    /// Park `round` as the round in flight. The caller starts a round only
    /// while the slot is idle.
    pub(super) fn start(&mut self, round: impl Future<Output = ()> + Send + 'a) {
        self.round = Some(Box::pin(round));
    }

    /// Resolves once the round in flight completes, and leaves the slot idle.
    /// Pends forever while no round is in flight.
    ///
    /// Cancel-safe: dropping this future leaves the round parked, and the
    /// next call resumes it where it stopped.
    pub(super) async fn finished(&mut self) {
        match self.round.as_mut() {
            Some(round) => {
                round.as_mut().await;
                self.round = None;
            }
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_parked_round_resumes_after_its_poll_is_dropped() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let mut slot = RoundSlot::default();
        slot.start(async move {
            let _ = rx.await;
        });
        assert!(!slot.is_idle());

        // The first poll is dropped while the round still waits.
        tokio::select! {
            biased;
            () = slot.finished() => panic!("the round cannot finish before its signal"),
            () = std::future::ready(()) => {}
        }
        assert!(!slot.is_idle(), "a dropped poll keeps the round parked");

        let _ = tx.send(());
        slot.finished().await;
        assert!(slot.is_idle());
    }
}
