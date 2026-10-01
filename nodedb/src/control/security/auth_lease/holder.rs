// SPDX-License-Identifier: BUSL-1.1

//! The holder side of this node's authorization lease.
//!
//! A statement is planned against local authorization state only while the
//! lease is valid. The lease ends on this node's clock before it ends on the
//! leader's (see [`super::timing`]), so once the leader treats it as expired
//! no statement here can still plan under it.
//!
//! A node that leads the metadata group as its only voter also plans under a
//! pinned lease, which has no expiry (see [`super::status`]).

use std::sync::Mutex;
use std::time::Instant;

use nodedb_cluster::GroupCoverage;

/// How this node's last renewal round ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewAttempt {
    /// No round ran yet.
    NotAttempted,
    /// This node's coverage was not computed, so nothing was sent.
    CoverageFailed { error: String },
    /// This node knows no metadata leader, so nothing was sent.
    NoLeader,
    /// This node leads the metadata group but runs no lease service.
    NoLeaderService,
    /// The renewal did not reach `leader_id`.
    NotDelivered { leader_id: u64, error: String },
    /// `leader_id` answered with a message that is not a renewal reply.
    UnexpectedReply { leader_id: u64 },
    /// `leader_id` withheld the lease: `coverage` misses one of its floors.
    Withheld {
        leader_id: u64,
        coverage: Vec<GroupCoverage>,
    },
    /// `leader_id` no longer leads the metadata group.
    NotLeader {
        leader_id: u64,
        leader_hint: Option<u64>,
    },
    /// `leader_id` granted the lease.
    Granted { leader_id: u64 },
}

impl std::fmt::Display for RenewAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAttempted => write!(f, "no renewal round ran"),
            Self::CoverageFailed { error } => {
                write!(f, "coverage could not be computed: {error}")
            }
            Self::NoLeader => write!(f, "no metadata leader is known"),
            Self::NoLeaderService => {
                write!(
                    f,
                    "this node leads the metadata group but runs no lease service"
                )
            }
            Self::NotDelivered { leader_id, error } => {
                write!(f, "renewal did not reach leader {leader_id}: {error}")
            }
            Self::UnexpectedReply { leader_id } => {
                write!(
                    f,
                    "leader {leader_id} sent a reply that is not a renewal reply"
                )
            }
            Self::Withheld {
                leader_id,
                coverage,
            } => {
                write!(f, "leader {leader_id} withheld the lease; coverage sent:")?;
                for group in coverage {
                    write!(f, " group {} through {}", group.group_id, group.through)?;
                }
                Ok(())
            }
            Self::NotLeader {
                leader_id,
                leader_hint,
            } => write!(
                f,
                "node {leader_id} no longer leads the metadata group (hint {leader_hint:?})"
            ),
            Self::Granted { leader_id } => write!(f, "leader {leader_id} granted the lease"),
        }
    }
}

/// The end of this node's lease, if it holds one, and how the last renewal
/// round ended.
#[derive(Debug)]
pub struct LeaseHolder {
    valid_until: Mutex<Option<Instant>>,
    last_attempt: Mutex<RenewAttempt>,
}

impl Default for LeaseHolder {
    fn default() -> Self {
        Self {
            valid_until: Mutex::new(None),
            last_attempt: Mutex::new(RenewAttempt::NotAttempted),
        }
    }
}

impl LeaseHolder {
    /// Record how the latest renewal round ended.
    pub fn record_attempt(&self, attempt: RenewAttempt) {
        *self.last_attempt.lock().unwrap_or_else(|p| p.into_inner()) = attempt;
    }

    /// How the latest renewal round ended.
    pub fn last_attempt(&self) -> RenewAttempt {
        self.last_attempt
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Extend the lease to `until`. A grant never shortens a lease already
    /// held: the leader granted each one against the state it covers.
    pub fn install(&self, until: Instant) {
        let mut valid_until = self.valid_until.lock().unwrap_or_else(|p| p.into_inner());
        if valid_until.is_none_or(|current| current < until) {
            *valid_until = Some(until);
        }
    }

    /// Whether the lease is valid at `now`.
    pub fn is_valid_at(&self, now: Instant) -> bool {
        self.valid_until
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some_and(|until| now < until)
    }

    /// When the lease ends, if one was granted.
    pub fn valid_until(&self) -> Option<Instant> {
        *self.valid_until.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::control::security::auth_lease::LeaseTiming;

    #[test]
    fn a_lease_is_valid_until_its_margin_adjusted_end() {
        let timing = LeaseTiming::from_raft(Duration::from_millis(150), Duration::from_millis(50))
            .expect("timing");
        let holder = LeaseHolder::default();
        let sent_at = Instant::now();
        assert!(!holder.is_valid_at(sent_at));

        holder.install(timing.holder_expiry(sent_at, timing.lease));
        assert!(holder.is_valid_at(sent_at + Duration::from_millis(99)));
        // The leader's lease still runs at 100ms, but the holder's has ended.
        assert!(!holder.is_valid_at(sent_at + Duration::from_millis(100)));
        assert!(!holder.is_valid_at(sent_at + timing.lease));
    }

    #[test]
    fn an_older_grant_never_shortens_the_lease() {
        let holder = LeaseHolder::default();
        let now = Instant::now();
        holder.install(now + Duration::from_millis(200));
        holder.install(now + Duration::from_millis(100));
        assert!(holder.is_valid_at(now + Duration::from_millis(150)));
    }
}
