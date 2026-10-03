// SPDX-License-Identifier: BUSL-1.1

//! Lease durations derived from the Raft timing.
//!
//! - **Lease:** the minimum election timeout. A leader that loses its
//!   quorum is replaced no sooner than that, the same bound Raft leader
//!   leases rely on.
//! - **Skew margin:** the heartbeat interval. The holder ends its lease this
//!   much before the leader does, so a holder clock that runs slow by up to
//!   the heartbeat-to-election ratio never outlives the leader's view.
//! - **Renewal:** every heartbeat interval. A holder that covers a change
//!   renews within one heartbeat, so an acknowledgement waits about one
//!   heartbeat when every node is healthy.
//! - **Reply margin:** the heartbeat interval. The leader answers a renewal
//!   or a barrier this much before the holder's read timeout ends. The reply
//!   then reaches the holder before the holder gives up on it.

use std::time::{Duration, Instant};

/// Lease durations of one cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTiming {
    /// How long a granted lease runs on the leader's clock.
    pub lease: Duration,
    /// How much earlier a holder ends its lease than the leader.
    pub skew_margin: Duration,
    /// How often a holder renews.
    pub renew_every: Duration,
    /// How much earlier the leader's reply deadline ends than the holder's
    /// read timeout.
    pub reply_margin: Duration,
}

impl LeaseTiming {
    /// Derive the timing from the Raft election timeout and heartbeat.
    pub fn from_raft(
        election_timeout_min: Duration,
        heartbeat_interval: Duration,
    ) -> crate::Result<Self> {
        if heartbeat_interval.is_zero() || heartbeat_interval >= election_timeout_min {
            return Err(crate::Error::Config {
                detail: format!(
                    "authorization lease: heartbeat interval {heartbeat_interval:?} must be \
                     non-zero and below the minimum election timeout {election_timeout_min:?}"
                ),
            });
        }
        Ok(Self {
            lease: election_timeout_min,
            skew_margin: heartbeat_interval,
            renew_every: heartbeat_interval,
            reply_margin: heartbeat_interval,
        })
    }

    /// The longest the leader spends confirming its leadership and loading
    /// floors for one reply. It ends a reply margin inside the lease.
    /// [`Self::from_raft`] keeps it non-zero: the heartbeat is below the lease.
    pub fn leader_budget(&self) -> Duration {
        self.lease.saturating_sub(self.reply_margin)
    }

    /// How long a planning path waits for the next renewal once it found
    /// the lease lapsed: one renewal interval for the next round to start,
    /// plus the reply margin for it to finish.
    pub fn lapse_grace(&self) -> Duration {
        self.renew_every + self.reply_margin
    }

    /// The holder's read timeout for an RPC whose leader handler first waits
    /// up to `wait`, then spends up to [`Self::leader_budget`].
    ///
    /// It exceeds the handler's own deadline by the reply margin. A slow
    /// reply then still arrives, and a read timeout means the leader is
    /// unreachable rather than busy.
    pub fn rpc_read_timeout(&self, wait: Duration) -> Duration {
        wait + self.leader_budget() + self.reply_margin
    }

    /// When a lease the leader granted for `granted`, requested at `sent_at`
    /// on the holder's clock, ends on the holder's clock.
    ///
    /// The leader starts its lease no earlier than it received the request,
    /// which is after `sent_at`. Ending at `sent_at + granted - skew_margin`
    /// therefore ends first on any holder clock that runs no slower than the
    /// margin allows.
    pub fn holder_expiry(&self, sent_at: Instant, granted: Duration) -> Instant {
        sent_at + granted.saturating_sub(self.skew_margin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timing_follows_the_raft_configuration() {
        let timing = LeaseTiming::from_raft(Duration::from_millis(150), Duration::from_millis(50))
            .expect("timing");
        assert_eq!(timing.lease, Duration::from_millis(150));
        assert_eq!(timing.skew_margin, Duration::from_millis(50));
        assert_eq!(timing.renew_every, Duration::from_millis(50));
        assert_eq!(timing.reply_margin, Duration::from_millis(50));
        assert_eq!(timing.lapse_grace(), Duration::from_millis(100));
    }

    /// The leader's reply deadline ends strictly inside the holder's read
    /// timeout, for a renewal and for a barrier that waits first.
    #[test]
    fn the_leader_answers_before_the_holder_times_out() {
        let timing = LeaseTiming::from_raft(Duration::from_millis(500), Duration::from_millis(50))
            .expect("timing");
        assert_eq!(timing.leader_budget(), Duration::from_millis(450));
        assert_eq!(timing.rpc_read_timeout(Duration::ZERO), timing.lease);
        for wait in [Duration::ZERO, Duration::from_secs(3)] {
            let leader_done = wait + timing.leader_budget();
            let holder_gives_up = timing.rpc_read_timeout(wait);
            assert!(leader_done < holder_gives_up);
            assert_eq!(holder_gives_up - leader_done, timing.reply_margin);
        }
    }

    #[test]
    fn a_heartbeat_at_or_above_the_election_timeout_is_refused() {
        assert!(
            LeaseTiming::from_raft(Duration::from_millis(50), Duration::from_millis(50)).is_err()
        );
        assert!(LeaseTiming::from_raft(Duration::from_millis(50), Duration::ZERO).is_err());
    }

    /// The holder ends its lease a skew margin before the leader does, even
    /// when the leader granted at the very moment the request left.
    #[test]
    fn the_holder_expires_early_by_the_skew_margin() {
        let timing = LeaseTiming::from_raft(Duration::from_millis(150), Duration::from_millis(50))
            .expect("timing");
        let sent_at = Instant::now();
        let holder_end = timing.holder_expiry(sent_at, timing.lease);
        let leader_end = sent_at + timing.lease;
        assert_eq!(leader_end - holder_end, timing.skew_margin);
        // A holder clock slow by a third of the lease still ends first: 100ms
        // of holder time is at most 133ms of real time, inside 150ms.
        let slow_real_elapsed = (holder_end - sent_at).mul_f64(4.0 / 3.0);
        assert!(sent_at + slow_real_elapsed <= leader_end);
    }
}
