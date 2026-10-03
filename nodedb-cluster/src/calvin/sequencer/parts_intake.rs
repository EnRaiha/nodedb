// SPDX-License-Identifier: BUSL-1.1

//! The sequencer leader's bounded queue of streamed parts.
//!
//! A coordinator submits a multi-part transaction's header, then streams its
//! parts here in order. The leader proposes queued parts on its epoch ticks.
//!
//! - A stream opens when the leader proposes its header's epoch batch. An
//!   offer to a stream that is not open is answered `Unknown`.
//! - Each part is checked in order against the header's manifest. A
//!   malformed part closes the stream and is answered `Rejected`. The leader
//!   then abandons the transaction.
//! - The queue holds at most `max_queued_part_bytes` across every stream.
//!   An offer that pushes the total past it is answered `Full`, and the coordinator
//!   retries. An empty queue always takes one part, so a stream never
//!   stalls on the cap.
//! - A part offered twice is taken once. The answer names the next part the
//!   stream owes, so a coordinator resumes where the leader stands.
//! - A stream that sends nothing for `part_stream_stall` before its last
//!   part is closed, and the leader abandons its transaction.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::calvin::TxnId;
use crate::calvin::sequencer::config::SequencerConfig;
use crate::calvin::sequencer::entry_limits::{EntryLimits, PartCursor};
use crate::calvin::types::{MultiPartPlans, PartStreamId, StreamedPart};

/// How the leader answered an offer of parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PartsOfferStatus {
    /// Every offered part the stream owed is queued.
    Accepted = 0,
    /// The queue is full. Retry from `next_index`.
    Full = 1,
    /// This leader holds no such stream: the header is not proposed yet, or
    /// the stream closed.
    Unknown = 2,
    /// A part is malformed. The stream is closed and the transaction aborts.
    Rejected = 3,
}

impl PartsOfferStatus {
    /// The status a wire code names, or `None` for an unknown code.
    pub fn from_wire(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Accepted),
            1 => Some(Self::Full),
            2 => Some(Self::Unknown),
            3 => Some(Self::Rejected),
            _ => None,
        }
    }
}

/// The answer to an offer of parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartsOffer {
    pub status: PartsOfferStatus,
    /// The index of the next part the stream owes.
    pub next_index: u32,
    /// Why a part was rejected.
    pub detail: Option<String>,
}

/// One open stream.
#[derive(Debug)]
struct Stream {
    txn: TxnId,
    /// The leader term the header was proposed in.
    term: u64,
    cursor: PartCursor,
    unproposed: VecDeque<StreamedPart>,
    /// no-determinism: stall detection only, never in the log.
    last_offer: Instant,
}

#[derive(Debug, Default)]
struct IntakeState {
    streams: BTreeMap<PartStreamId, Stream>,
    /// Streams in header order, for fair proposal.
    order: VecDeque<PartStreamId>,
    queued_bytes: usize,
}

impl IntakeState {
    fn remove(&mut self, stream: PartStreamId) -> Option<Stream> {
        let removed = self.streams.remove(&stream)?;
        self.order.retain(|id| *id != stream);
        let bytes: usize = removed.unproposed.iter().map(part_bytes).sum();
        self.queued_bytes = self.queued_bytes.saturating_sub(bytes);
        Some(removed)
    }
}

fn part_bytes(part: &StreamedPart) -> usize {
    part.part.plans.len()
}

/// The leader's queue of streamed parts, shared by the inbox that takes
/// offers and the service that proposes them.
#[derive(Debug)]
pub struct PartsIntake {
    state: Mutex<IntakeState>,
    limits: EntryLimits,
    max_queued_bytes: usize,
    stall: Duration,
}

impl PartsIntake {
    pub fn new(config: &SequencerConfig) -> Self {
        Self {
            state: Mutex::new(IntakeState::default()),
            limits: EntryLimits {
                max_plans_bytes: config.max_plans_bytes_per_txn,
                max_participating_vshards: config.max_participating_vshards_per_txn,
            },
            max_queued_bytes: config.max_queued_part_bytes,
            stall: config.part_stream_stall,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, IntakeState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Queue `parts` of `stream`, in index order, as far as the stream and
    /// the queue take them.
    pub fn offer(&self, stream: PartStreamId, parts: Vec<StreamedPart>) -> PartsOffer {
        let mut guard = self.lock();
        let state = &mut *guard;
        let Some(open) = state.streams.get_mut(&stream) else {
            return PartsOffer {
                status: PartsOfferStatus::Unknown,
                next_index: 0,
                detail: None,
            };
        };
        // no-determinism: stall detection only, never in the log.
        open.last_offer = Instant::now();
        let mut status = PartsOfferStatus::Accepted;
        for part in parts {
            let next = open.cursor.next_index();
            if part.index < next {
                continue;
            }
            if part.index > next {
                break;
            }
            let bytes = part_bytes(&part);
            if state.queued_bytes > 0
                && state.queued_bytes.saturating_add(bytes) > self.max_queued_bytes
            {
                status = PartsOfferStatus::Full;
                break;
            }
            if let Err(error) = open.cursor.admit(&part, self.limits) {
                let next_index = open.cursor.next_index();
                state.remove(stream);
                return PartsOffer {
                    status: PartsOfferStatus::Rejected,
                    next_index,
                    detail: Some(error.to_string()),
                };
            }
            open.unproposed.push_back(part);
            state.queued_bytes += bytes;
        }
        PartsOffer {
            status,
            next_index: open.cursor.next_index(),
            detail: None,
        }
    }

    /// Open `stream` for the header of `txn`, proposed in `term`.
    pub(crate) fn open(
        &self,
        stream: PartStreamId,
        txn: TxnId,
        term: u64,
        manifest: &MultiPartPlans,
    ) {
        let mut state = self.lock();
        state.remove(stream);
        state.order.push_back(stream);
        state.streams.insert(
            stream,
            Stream {
                txn,
                term,
                cursor: PartCursor::new(manifest),
                unproposed: VecDeque::new(),
                // no-determinism: stall detection only, never in the log.
                last_offer: Instant::now(),
            },
        );
    }

    /// Close every stream. Returns how many were open.
    pub(crate) fn clear(&self) -> usize {
        let mut state = self.lock();
        let open = state.streams.len();
        *state = IntakeState::default();
        open
    }

    /// Close every stream opened in a term other than `term`, and every
    /// stream whose coordinator sent nothing for the stall window before its
    /// last part. Returns how many closed.
    pub(crate) fn close_stale(&self, term: Option<u64>, now: Instant) -> usize {
        let mut state = self.lock();
        let stale: Vec<PartStreamId> = state
            .streams
            .iter()
            .filter(|(_, open)| {
                Some(open.term) != term
                    || (!open.cursor.is_complete()
                        && now.saturating_duration_since(open.last_offer) > self.stall)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in &stale {
            state.remove(*id);
        }
        stale.len()
    }

    /// Take the next part to propose: the front part of the first stream,
    /// in header order, that has one.
    pub(crate) fn next_part(&self) -> Option<(PartStreamId, TxnId, StreamedPart)> {
        let mut guard = self.lock();
        let state = &mut *guard;
        let (id, open) = state.order.iter().find_map(|id| {
            let open = state.streams.get(id)?;
            (!open.unproposed.is_empty()).then_some((*id, open.txn))
        })?;
        let part = state.streams.get_mut(&id)?.unproposed.pop_front()?;
        state.queued_bytes = state.queued_bytes.saturating_sub(part_bytes(&part));
        Some((id, open, part))
    }

    /// Put back a part whose proposal failed, at the front of its stream.
    pub(crate) fn return_part(&self, stream: PartStreamId, part: StreamedPart) {
        let mut guard = self.lock();
        let state = &mut *guard;
        if let Some(open) = state.streams.get_mut(&stream) {
            state.queued_bytes += part_bytes(&part);
            open.unproposed.push_front(part);
        }
    }

    /// Close every stream whose parts are all proposed and for which `done`
    /// holds.
    pub(crate) fn close_finished(&self, done: impl Fn(TxnId) -> bool) {
        let mut state = self.lock();
        let finished: Vec<PartStreamId> = state
            .streams
            .iter()
            .filter(|(_, open)| {
                open.cursor.is_complete() && open.unproposed.is_empty() && done(open.txn)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in finished {
            state.remove(id);
        }
    }

    /// Whether an open stream carries the parts of `txn`.
    pub(crate) fn holds(&self, txn: TxnId) -> bool {
        self.lock().streams.values().any(|open| open.txn == txn)
    }

    /// Part bytes queued and not yet proposed.
    pub fn queued_bytes(&self) -> usize {
        self.lock().queued_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::types::{PlanPart, VShardParts};

    const STREAM: PartStreamId = PartStreamId { node: 2, seq: 9 };

    fn manifest(parts: u32) -> MultiPartPlans {
        MultiPartPlans {
            stream: STREAM,
            part_count: parts,
            total_tasks: parts,
            user_write: true,
            client_write: true,
            per_vshard: vec![VShardParts { vshard: 5, parts }],
        }
    }

    fn part(index: u32, bytes: usize) -> StreamedPart {
        StreamedPart {
            index,
            targets: vec![5],
            part: PlanPart {
                first_task: index,
                plans: vec![1; bytes],
                chunk: None,
            },
        }
    }

    fn intake(max_queued_part_bytes: usize) -> PartsIntake {
        PartsIntake::new(&SequencerConfig {
            max_plans_bytes_per_txn: 8,
            max_queued_part_bytes,
            ..SequencerConfig::default()
        })
    }

    #[test]
    fn an_offer_before_the_header_is_unknown() {
        let intake = intake(16);
        let offer = intake.offer(STREAM, vec![part(0, 4)]);
        assert_eq!(offer.status, PartsOfferStatus::Unknown);
    }

    /// The queue stops at its byte cap and answers `Full`. Proposing drains
    /// it, and the stream resumes where it stopped. A resent part is taken
    /// once.
    #[test]
    fn a_full_queue_pushes_back_and_resumes_after_proposals() {
        let intake = intake(16);
        intake.open(STREAM, TxnId::new(1, 0), 3, &manifest(6));
        let offer = intake.offer(STREAM, (0..6).map(|i| part(i, 8)).collect());
        assert_eq!(offer.status, PartsOfferStatus::Full);
        assert_eq!(offer.next_index, 2);
        assert_eq!(intake.queued_bytes(), 16);

        let (_, _, first) = intake.next_part().expect("a queued part");
        assert_eq!(first.index, 0);
        let offer = intake.offer(STREAM, (1..6).map(|i| part(i, 8)).collect());
        assert_eq!(offer.next_index, 3, "part 1 is taken once, part 2 fits");
        assert!(intake.queued_bytes() <= 16);
    }

    #[test]
    fn a_malformed_part_closes_the_stream() {
        let intake = intake(64);
        intake.open(STREAM, TxnId::new(1, 0), 3, &manifest(2));
        let offer = intake.offer(STREAM, vec![part(0, 4), part(1, 9)]);
        assert_eq!(offer.status, PartsOfferStatus::Rejected);
        assert!(offer.detail.is_some());
        assert!(!intake.holds(TxnId::new(1, 0)));
        assert_eq!(intake.queued_bytes(), 0);
    }

    #[test]
    fn a_term_change_or_a_stall_closes_the_stream() {
        let intake = intake(64);
        intake.open(STREAM, TxnId::new(1, 0), 3, &manifest(2));
        assert_eq!(intake.close_stale(Some(3), Instant::now()), 0);
        assert_eq!(intake.close_stale(Some(4), Instant::now()), 1);
        assert!(!intake.holds(TxnId::new(1, 0)));

        intake.open(STREAM, TxnId::new(1, 0), 4, &manifest(2));
        let later = Instant::now() + Duration::from_secs(60);
        assert_eq!(intake.close_stale(Some(4), later), 1, "stalled");
    }
}
