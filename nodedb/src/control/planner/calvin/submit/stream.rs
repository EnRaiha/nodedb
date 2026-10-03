// SPDX-License-Identifier: BUSL-1.1

//! Stream a multi-part transaction's parts to the sequencer leader that took
//! its header.
//!
//! The coordinator sends the parts in order, in batches of at most
//! `MAX_PARTS_BATCH_BYTES`, so no request nears the 64 MiB RPC limit
//! whatever the transaction's size. The leader answers each batch with the
//! next part it owes and a status:
//!
//! - `Accepted`: go on from the next part it owes.
//! - `Full`: its queue is full. Wait, then go on from the next part it owes.
//! - `Unknown`: it holds no such stream. Before the leader took a first
//!   part, the header can still lack a proposal, so the stream waits and
//!   retries unless the caller already holds the assignment. After that,
//!   the stream is lost: the leader changed, or the stream stalled. The
//!   transaction then aborts with `PartsLost` and the caller retries it.
//! - `Rejected`: a part is malformed. The transaction aborts.
//!
//! A `Full` answer comes from a live leader that holds the stream, so it
//! keeps the stream alive. A stream with no answer from such a leader for
//! the network deadline stops as lost.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use nodedb_cluster::calvin::PartsOfferStatus;
use nodedb_cluster::calvin::types::{PartStreamId, StreamedPart};
use nodedb_cluster::{CalvinPartsRequest, CalvinPartsResponse, MAX_PARTS_BATCH_BYTES, RaftRpc};
use tracing::warn;

use crate::Error;
use crate::control::cluster::warm_peers::register_peers_from_topology;
use crate::control::state::SharedState;

/// The first wait after a `Full` or an early `Unknown`.
const FIRST_BACKOFF: Duration = Duration::from_millis(5);
/// The longest wait between two offers.
const MAX_BACKOFF: Duration = Duration::from_millis(200);

/// A multi-part transaction's parts, in index order, and the stream that
/// carries them.
#[derive(Debug)]
pub struct PartStream {
    pub id: PartStreamId,
    pub parts: Vec<StreamedPart>,
}

/// The node a stream goes to.
#[derive(Debug, Clone, Copy)]
pub(crate) enum StreamTarget {
    /// This node leads the sequencer group.
    Local,
    /// The sequencer leader that took the header.
    Remote(u64),
}

/// How a stream ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StreamEnd {
    /// The leader took every part.
    Done,
    /// The leader no longer holds the stream. The transaction aborts with
    /// `PartsLost`.
    Lost,
    /// The leader rejected a part.
    Rejected(String),
}

impl StreamEnd {
    /// A rejected stream as the statement's error. A malformed part is a
    /// coordinator bug, so a retry cannot fix it.
    pub(crate) fn into_result(self) -> crate::Result<()> {
        match self {
            Self::Done | Self::Lost => Ok(()),
            Self::Rejected(detail) => Err(Error::Internal {
                detail: format!("calvin part stream rejected by the sequencer leader: {detail}"),
            }),
        }
    }
}

/// Stream `stream` to `target`. `assigned` says the caller holds the
/// header's assignment, so the leader has opened the stream.
pub(crate) async fn stream_parts(
    state: &SharedState,
    target: StreamTarget,
    stream: &PartStream,
    assigned: bool,
) -> StreamEnd {
    if stream.parts.is_empty() {
        return StreamEnd::Done;
    }
    let deadline = Duration::from_secs(state.tuning.network.default_deadline_secs.max(1));
    // no-determinism: coordinator-side liveness timer, never in the log.
    let mut progress = StreamProgress::new(stream.parts.len(), assigned, deadline, Instant::now());
    let mut backoff = FIRST_BACKOFF;
    loop {
        let batch = batch_from(&stream.parts, progress.next);
        let reply = match offer(state, target, stream.id, batch).await {
            Ok(reply) => reply,
            Err(error) => {
                warn!(%error, "calvin part stream: offer failed; retrying");
                PartsOfferReply {
                    status: PartsOfferStatus::Unknown,
                    next_index: 0,
                    detail: None,
                    transport_error: true,
                }
            }
        };
        let before = progress.next;
        // no-determinism: coordinator-side liveness timer.
        match progress.on_reply(&reply, Instant::now()) {
            Step::End(end) => return end,
            Step::Offer => backoff = FIRST_BACKOFF,
            Step::Wait => {
                if progress.next > before {
                    backoff = FIRST_BACKOFF;
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// What the stream does after a reply.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Offer the next batch at once.
    Offer,
    /// Wait, then offer again.
    Wait,
    /// The stream ended.
    End(StreamEnd),
}

/// Where a stream stands, and when the leader last showed it alive.
#[derive(Debug)]
struct StreamProgress {
    total: usize,
    /// The next part the leader owes.
    next: usize,
    /// Whether the leader took a part: it opened the stream.
    taken_any: bool,
    /// Whether the caller holds the header's assignment.
    assigned: bool,
    deadline: Duration,
    /// The last answer from a leader that holds the stream.
    last_alive: Instant,
}

impl StreamProgress {
    fn new(total: usize, assigned: bool, deadline: Duration, now: Instant) -> Self {
        Self {
            total,
            next: 0,
            taken_any: false,
            assigned,
            deadline,
            last_alive: now,
        }
    }

    /// Fold `reply`, answered at `now`, into the stream.
    ///
    /// An `Accepted` or `Full` answer comes from a leader that holds the
    /// stream, so it counts as liveness: a full queue drains as the leader
    /// proposes. Only an `Unknown` answer, or no answer, runs down the
    /// deadline. A leader that stops proposing closes the stalled stream,
    /// and its next answer is `Unknown`.
    fn on_reply(&mut self, reply: &PartsOfferReply, now: Instant) -> Step {
        let owed = usize::try_from(reply.next_index).unwrap_or(usize::MAX);
        match reply.status {
            PartsOfferStatus::Rejected => {
                return Step::End(StreamEnd::Rejected(
                    reply.detail.clone().unwrap_or_default(),
                ));
            }
            PartsOfferStatus::Unknown
                if !reply.transport_error && (self.assigned || self.taken_any) =>
            {
                return Step::End(StreamEnd::Lost);
            }
            PartsOfferStatus::Accepted | PartsOfferStatus::Full => {
                self.taken_any = true;
                self.last_alive = now;
                self.next = owed.min(self.total);
            }
            PartsOfferStatus::Unknown => {}
        }
        if self.next >= self.total {
            return Step::End(StreamEnd::Done);
        }
        if reply.status == PartsOfferStatus::Accepted {
            return Step::Offer;
        }
        if now.saturating_duration_since(self.last_alive) > self.deadline {
            return Step::End(StreamEnd::Lost);
        }
        Step::Wait
    }
}

/// The parts from `from` on that fit one request, at least one.
fn batch_from(parts: &[StreamedPart], from: usize) -> Vec<StreamedPart> {
    let mut bytes = 0usize;
    let mut batch = Vec::new();
    for part in parts.iter().skip(from) {
        let size = part.part.plans.len();
        if !batch.is_empty() && bytes.saturating_add(size) > MAX_PARTS_BATCH_BYTES {
            break;
        }
        bytes = bytes.saturating_add(size);
        batch.push(part.clone());
    }
    batch
}

/// A leader's answer, or a transport error read as `Unknown`.
struct PartsOfferReply {
    status: PartsOfferStatus,
    next_index: u32,
    detail: Option<String>,
    transport_error: bool,
}

async fn offer(
    state: &SharedState,
    target: StreamTarget,
    stream: PartStreamId,
    batch: Vec<StreamedPart>,
) -> crate::Result<PartsOfferReply> {
    match target {
        StreamTarget::Local => {
            let inbox = state
                .sequencer_inbox
                .get()
                .ok_or(Error::SequencerUnavailable)?;
            let offer = inbox.offer_parts(stream, batch);
            Ok(PartsOfferReply {
                status: offer.status,
                next_index: offer.next_index,
                detail: offer.detail,
                transport_error: false,
            })
        }
        StreamTarget::Remote(leader) => offer_remote(state, leader, stream, batch).await,
    }
}

async fn offer_remote(
    state: &SharedState,
    leader: u64,
    stream: PartStreamId,
    batch: Vec<StreamedPart>,
) -> crate::Result<PartsOfferReply> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or(Error::SequencerUnavailable)?;
    register_peers_from_topology(state, transport, &BTreeSet::from([leader]));
    let parts_bytes = zerompk::to_msgpack_vec(&batch).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("calvin part stream: batch encode: {e}"),
    })?;
    let request = RaftRpc::CalvinPartsRequest(CalvinPartsRequest {
        stream_node: stream.node,
        stream_seq: stream.seq,
        parts_bytes,
    });
    let read_timeout = Duration::from_secs(state.tuning.network.default_deadline_secs.max(1));
    let reply = transport
        .send_rpc_with_read_timeout(leader, request, read_timeout)
        .await
        .map_err(|e| Error::Internal {
            detail: format!("calvin part stream RPC to sequencer leader node {leader}: {e}"),
        })?;
    let RaftRpc::CalvinPartsResponse(CalvinPartsResponse {
        status,
        next_index,
        detail,
    }) = reply
    else {
        return Err(Error::Internal {
            detail: format!("calvin part stream: unexpected reply from node {leader}"),
        });
    };
    let status = PartsOfferStatus::from_wire(status).ok_or_else(|| Error::Internal {
        detail: format!("calvin part stream: unknown status {status} from node {leader}"),
    })?;
    Ok(PartsOfferReply {
        status,
        next_index,
        detail,
        transport_error: false,
    })
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::PlanPart;

    use super::*;

    fn part(index: u32, bytes: usize) -> StreamedPart {
        StreamedPart {
            index,
            targets: vec![1],
            part: PlanPart {
                first_task: index,
                plans: vec![0; bytes],
                chunk: None,
            },
        }
    }

    #[test]
    fn a_batch_stays_under_its_byte_cap_and_holds_at_least_one_part() {
        let parts: Vec<StreamedPart> = (0..40).map(|i| part(i, 1 << 20)).collect();
        let batch = batch_from(&parts, 3);
        assert_eq!(batch.first().map(|p| p.index), Some(3));
        let bytes: usize = batch.iter().map(|p| p.part.plans.len()).sum();
        assert!(bytes <= MAX_PARTS_BATCH_BYTES);
        assert_eq!(batch.len(), MAX_PARTS_BATCH_BYTES >> 20);

        let huge = vec![part(0, MAX_PARTS_BATCH_BYTES + 1)];
        assert_eq!(batch_from(&huge, 0).len(), 1);
    }

    fn reply(status: PartsOfferStatus, next_index: u32, transport_error: bool) -> PartsOfferReply {
        PartsOfferReply {
            status,
            next_index,
            detail: None,
            transport_error,
        }
    }

    /// A leader that holds the stream and answers `Full` past the deadline
    /// keeps it alive: its queue drains as it proposes. Only no answer runs
    /// the deadline down.
    #[test]
    fn full_answers_keep_a_stream_alive_past_the_deadline() {
        let deadline = Duration::from_secs(1);
        // no-determinism: test-only synthetic timeline.
        let start = Instant::now();
        let mut progress = StreamProgress::new(4, true, deadline, start);
        assert_eq!(
            progress.on_reply(&reply(PartsOfferStatus::Accepted, 1, false), start),
            Step::Offer
        );
        for second in 1..10 {
            let now = start + Duration::from_secs(second);
            assert_eq!(
                progress.on_reply(&reply(PartsOfferStatus::Full, 1, false), now),
                Step::Wait,
                "a full queue is no stall"
            );
        }
        let alive = start + Duration::from_secs(9);
        assert_eq!(
            progress.on_reply(
                &reply(PartsOfferStatus::Unknown, 0, true),
                alive + Duration::from_millis(500)
            ),
            Step::Wait,
            "one missed answer within the deadline waits"
        );
        assert_eq!(
            progress.on_reply(
                &reply(PartsOfferStatus::Unknown, 0, true),
                alive + Duration::from_secs(2)
            ),
            Step::End(StreamEnd::Lost),
            "no answer for the deadline loses the stream"
        );
    }
}
