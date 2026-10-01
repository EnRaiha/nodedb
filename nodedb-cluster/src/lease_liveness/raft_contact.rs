// SPDX-License-Identifier: BUSL-1.1

//! When each peer last answered this node as metadata-group leader.
//!
//! Raft keeps a per-peer response count, not a time. [`RaftContactClock`]
//! turns successive count samples into a last-change instant per peer. A
//! change is dated to the sample that saw it, which is never earlier than the
//! response itself. Silence is measured only up to the latest sample, so a
//! response that arrived after it cannot be missed.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::multi_raft::PeerAckSample;

#[derive(Debug, Clone, Copy)]
struct PeerContact {
    acks: u64,
    changed_at: Instant,
}

#[derive(Debug)]
struct TermContact {
    term: u64,
    sampled_at: Instant,
    peers: HashMap<u64, PeerContact>,
}

/// Leader-side record of each peer's last observed Raft response.
#[derive(Debug, Default)]
pub struct RaftContactClock {
    inner: Mutex<Option<TermContact>>,
}

impl RaftContactClock {
    /// Fold in a sample taken at `now`. A new term restarts every peer's
    /// clock at `now`: counts restart with the term, so older history says
    /// nothing about the current one.
    pub fn observe(&self, sample: &PeerAckSample, now: Instant) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let same_term = guard.as_ref().is_some_and(|t| t.term == sample.term);
        if !same_term {
            *guard = Some(TermContact {
                term: sample.term,
                sampled_at: now,
                peers: sample
                    .acks
                    .iter()
                    .map(|&(peer, acks)| {
                        (
                            peer,
                            PeerContact {
                                acks,
                                changed_at: now,
                            },
                        )
                    })
                    .collect(),
            });
            return;
        }
        let Some(term) = guard.as_mut() else {
            return;
        };
        term.sampled_at = now;
        let mut peers = HashMap::with_capacity(sample.acks.len());
        for &(peer, acks) in &sample.acks {
            let contact = match term.peers.get(&peer) {
                Some(prev) if prev.acks == acks => *prev,
                _ => PeerContact {
                    acks,
                    changed_at: now,
                },
            };
            peers.insert(peer, contact);
        }
        term.peers = peers;
    }

    /// Forget every sample. Called when this node stops leading.
    pub fn clear(&self) {
        *self.inner.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }

    /// Whether samples taken in `term` show `peer` silent for at least
    /// `min_silence`. False for an untracked peer or another term.
    pub fn silent_for(&self, peer: u64, term: u64, min_silence: Duration) -> bool {
        let guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let Some(contact) = guard.as_ref().filter(|t| t.term == term) else {
            return false;
        };
        contact.peers.get(&peer).is_some_and(|p| {
            contact.sampled_at.saturating_duration_since(p.changed_at) >= min_silence
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PEER: u64 = 2;
    const TERM: u64 = 4;
    const SILENCE: Duration = Duration::from_secs(15);

    fn sample(term: u64, acks: u64) -> PeerAckSample {
        PeerAckSample {
            term,
            acks: vec![(PEER, acks)],
        }
    }

    #[test]
    fn an_unchanged_count_accumulates_silence() {
        let clock = RaftContactClock::default();
        let start = Instant::now();
        clock.observe(&sample(TERM, 7), start);
        clock.observe(&sample(TERM, 7), start + SILENCE);
        assert!(clock.silent_for(PEER, TERM, SILENCE));
    }

    #[test]
    fn a_new_response_resets_silence() {
        let clock = RaftContactClock::default();
        let start = Instant::now();
        clock.observe(&sample(TERM, 7), start);
        clock.observe(&sample(TERM, 8), start + SILENCE);
        assert!(!clock.silent_for(PEER, TERM, SILENCE));
    }

    #[test]
    fn a_new_term_restarts_the_clock() {
        let clock = RaftContactClock::default();
        let start = Instant::now();
        clock.observe(&sample(TERM, 7), start);
        clock.observe(&sample(TERM + 1, 0), start + SILENCE);
        assert!(!clock.silent_for(PEER, TERM + 1, SILENCE));
        assert!(!clock.silent_for(PEER, TERM, SILENCE));
    }

    #[test]
    fn cleared_or_untracked_is_never_silent() {
        let clock = RaftContactClock::default();
        let start = Instant::now();
        clock.observe(&sample(TERM, 7), start);
        clock.observe(&sample(TERM, 7), start + SILENCE);
        assert!(!clock.silent_for(PEER + 1, TERM, SILENCE));
        clock.clear();
        assert!(!clock.silent_for(PEER, TERM, SILENCE));
    }
}
