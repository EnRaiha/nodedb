// SPDX-License-Identifier: BUSL-1.1

//! Receiver side of a cross-node streaming shuffle push.
//!
//! A `ShufflePushRequest` opens a producer → receiver stream. The producer
//! writes `ShufflePushChunk` envelopes on the same bidi stream, then exactly
//! one `ShufflePushEnd`. The receiver deposits each chunk and writes no
//! reply.

use std::future::Future;

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, RaftRpc, ShufflePushRequest, TypedClusterError, auth_envelope};
use crate::transport::auth_context::AuthContext;
use crate::transport::rpc_handler::RaftRpcHandler;

use super::frame_io::read_envelope_or_finish;

/// The `(shuffle_id, part, side)` a push stream feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PushKey {
    pub shuffle_id: u64,
    pub part: u32,
    pub side: u8,
}

/// A shuffle push stream broke its frame protocol.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ShufflePushError {
    /// The producer finished its send stream before the `ShufflePushEnd`
    /// frame. The receiver holds a partial partition.
    #[error("shuffle push stream ({shuffle_id},{part},{side}) ended before its end frame")]
    Truncated {
        shuffle_id: u64,
        part: u32,
        side: u8,
    },
}

/// A stream of inbound envelopes. `None` is a clean finish.
pub(super) trait EnvelopeSource {
    fn next_envelope(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;
}

impl EnvelopeSource for quinn::RecvStream {
    fn next_envelope(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send {
        read_envelope_or_finish(self)
    }
}

/// The receiver inbox a push stream feeds.
pub(super) trait ShuffleInbox: Sync {
    fn open(&self, req: ShufflePushRequest) -> impl Future<Output = ()> + Send;
    fn deposit(&self, key: PushKey, payload: Vec<u8>) -> impl Future<Output = Result<()>> + Send;
    fn end(
        &self,
        key: PushKey,
        error: Option<TypedClusterError>,
    ) -> impl Future<Output = ()> + Send;
}

impl<H: RaftRpcHandler> ShuffleInbox for H {
    fn open(&self, req: ShufflePushRequest) -> impl Future<Output = ()> + Send {
        self.on_shuffle_request(req)
    }

    fn deposit(&self, key: PushKey, payload: Vec<u8>) -> impl Future<Output = Result<()>> + Send {
        self.on_shuffle_chunk(key.shuffle_id, key.part, key.side, payload)
    }

    fn end(
        &self,
        key: PushKey,
        error: Option<TypedClusterError>,
    ) -> impl Future<Output = ()> + Send {
        self.on_shuffle_end(key.shuffle_id, key.part, key.side, error)
    }
}

/// Open the inbox for `req`, then deposit every inbound frame until the
/// `ShufflePushEnd` frame.
///
/// A clean finish before the `ShufflePushEnd` frame is a truncated stream
/// and fails with [`ShufflePushError::Truncated`]. Every frame must come from `opener_node_id`. A frame from another node
/// calls `reject_identity`. Any error ends the inbox with that error, so
/// the consumer never waits on a producer that is gone, and the error
/// returns to the caller.
pub(super) async fn drain_shuffle_push<I, S>(
    inbox: &I,
    auth: &AuthContext,
    source: &mut S,
    req: ShufflePushRequest,
    opener_node_id: u64,
    reject_identity: impl Fn(u64) -> Result<()> + Send,
) -> Result<()>
where
    I: ShuffleInbox + ?Sized,
    S: EnvelopeSource + Send,
{
    let key = PushKey {
        shuffle_id: req.shuffle_id,
        part: req.part,
        side: req.side,
    };
    inbox.open(req).await;
    let result = drain_frames(inbox, auth, source, key, opener_node_id, reject_identity).await;
    if let Err(err) = &result {
        inbox.end(key, Some(stream_failure(key, err))).await;
    }
    result
}

/// Deposit frames until the `End` frame. A clean finish before it is
/// [`ShufflePushError::Truncated`].
async fn drain_frames<I, S>(
    inbox: &I,
    auth: &AuthContext,
    source: &mut S,
    key: PushKey,
    opener_node_id: u64,
    reject_identity: impl Fn(u64) -> Result<()> + Send,
) -> Result<()>
where
    I: ShuffleInbox + ?Sized,
    S: EnvelopeSource + Send,
{
    loop {
        let Some(envelope) = source.next_envelope().await? else {
            return Err(ShufflePushError::Truncated {
                shuffle_id: key.shuffle_id,
                part: key.part,
                side: key.side,
            }
            .into());
        };
        let (fields, inner) = auth_envelope::parse_envelope(&envelope, &auth.mac_key)?;
        if fields.from_node_id != opener_node_id {
            reject_identity(fields.from_node_id)?;
        }
        if fields.from_node_id != auth.local_node_id {
            auth.peer_seq_in.accept(fields.from_node_id, fields.seq)?;
        }
        match rpc_codec::decode(inner, &auth.epoch)? {
            RaftRpc::ShufflePushChunk(chunk) => inbox.deposit(key, chunk.payload).await?,
            RaftRpc::ShufflePushEnd(end) => {
                inbox.end(key, end.error).await;
                return Ok(());
            }
            other => {
                return Err(ClusterError::Transport {
                    detail: format!("unexpected frame in shuffle push stream: {other:?}"),
                });
            }
        }
    }
}

/// The inbox error for a push stream that failed with `err`.
fn stream_failure(key: PushKey, err: &ClusterError) -> TypedClusterError {
    TypedClusterError::Internal {
        code: 0,
        message: format!(
            "shuffle push stream ({},{},{}) failed: {err}",
            key.shuffle_id, key.part, key.side
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::rpc_codec::{ShufflePushChunk, ShufflePushEnd};
    use crate::transport::credentials::TransportCredentials;
    use crate::transport::frame_io::encode_rpc_frame;

    const PRODUCER: u64 = 2;
    const RECEIVER: u64 = 1;

    #[derive(Debug, PartialEq)]
    enum Event {
        Open,
        Deposit(Vec<u8>),
        End { failed: bool },
    }

    #[derive(Default)]
    struct Recorder(Mutex<Vec<Event>>);

    impl Recorder {
        fn push(&self, event: Event) {
            self.0.lock().expect("recorder lock").push(event);
        }

        fn events(&self) -> Vec<Event> {
            std::mem::take(&mut *self.0.lock().expect("recorder lock"))
        }
    }

    impl ShuffleInbox for Recorder {
        fn open(&self, _req: ShufflePushRequest) -> impl Future<Output = ()> + Send {
            self.push(Event::Open);
            std::future::ready(())
        }

        fn deposit(
            &self,
            _key: PushKey,
            payload: Vec<u8>,
        ) -> impl Future<Output = Result<()>> + Send {
            self.push(Event::Deposit(payload));
            std::future::ready(Ok(()))
        }

        fn end(
            &self,
            _key: PushKey,
            error: Option<TypedClusterError>,
        ) -> impl Future<Output = ()> + Send {
            self.push(Event::End {
                failed: error.is_some(),
            });
            std::future::ready(())
        }
    }

    struct Scripted(VecDeque<Result<Option<Vec<u8>>>>);

    impl EnvelopeSource for Scripted {
        fn next_envelope(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send {
            std::future::ready(self.0.pop_front().unwrap_or(Ok(None)))
        }
    }

    fn request() -> ShufflePushRequest {
        ShufflePushRequest {
            shuffle_id: 9,
            part: 3,
            side: 1,
            num_parts: 4,
            producer_count: 1,
        }
    }

    fn auths() -> (AuthContext, AuthContext) {
        (
            AuthContext::from_credentials(RECEIVER, &TransportCredentials::Insecure),
            AuthContext::from_credentials(PRODUCER, &TransportCredentials::Insecure),
        )
    }

    fn chunk(producer: &AuthContext, payload: &[u8]) -> Result<Option<Vec<u8>>> {
        let rpc = RaftRpc::ShufflePushChunk(ShufflePushChunk {
            payload: payload.to_vec(),
        });
        encode_rpc_frame(&rpc, producer).map(Some)
    }

    fn no_reject(node: u64) -> Result<()> {
        Err(ClusterError::Transport {
            detail: format!("unexpected identity check for node {node}"),
        })
    }

    /// A read error mid-stream ends the inbox with an error and returns the
    /// error. It is never a clean end of stream.
    #[tokio::test]
    async fn read_error_ends_the_inbox_with_the_error() {
        let (receiver, producer) = auths();
        let mut source = Scripted(VecDeque::from([
            chunk(&producer, b"a"),
            Err(ClusterError::Transport {
                detail: "read envelope header: connection lost".into(),
            }),
        ]));
        let inbox = Recorder::default();
        let result = drain_shuffle_push(
            &inbox,
            &receiver,
            &mut source,
            request(),
            PRODUCER,
            no_reject,
        )
        .await;
        assert!(matches!(result, Err(ClusterError::Transport { .. })));
        assert_eq!(
            inbox.events(),
            vec![
                Event::Open,
                Event::Deposit(b"a".to_vec()),
                Event::End { failed: true }
            ]
        );
    }

    /// The `End` frame ends the inbox once, with the producer's outcome.
    #[tokio::test]
    async fn end_frame_ends_the_inbox_once() {
        let (receiver, producer) = auths();
        let end = encode_rpc_frame(
            &RaftRpc::ShufflePushEnd(ShufflePushEnd { error: None }),
            &producer,
        )
        .map(Some);
        let mut source = Scripted(VecDeque::from([chunk(&producer, b"a"), end]));
        let inbox = Recorder::default();
        drain_shuffle_push(
            &inbox,
            &receiver,
            &mut source,
            request(),
            PRODUCER,
            no_reject,
        )
        .await
        .expect("clean drain");
        assert_eq!(
            inbox.events(),
            vec![
                Event::Open,
                Event::Deposit(b"a".to_vec()),
                Event::End { failed: false }
            ]
        );
    }

    /// A clean finish after the `End` frame is the end of the stream. A
    /// clean finish before it is a truncated stream: the drain fails and
    /// ends the inbox with the error.
    #[tokio::test]
    async fn clean_finish_is_end_of_stream() {
        let (receiver, producer) = auths();
        let end = encode_rpc_frame(
            &RaftRpc::ShufflePushEnd(ShufflePushEnd { error: None }),
            &producer,
        )
        .map(Some);
        let mut source = Scripted(VecDeque::from([chunk(&producer, b"a"), end, Ok(None)]));
        let inbox = Recorder::default();
        drain_shuffle_push(
            &inbox,
            &receiver,
            &mut source,
            request(),
            PRODUCER,
            no_reject,
        )
        .await
        .expect("clean finish after the end frame");
        assert_eq!(
            inbox.events(),
            vec![
                Event::Open,
                Event::Deposit(b"a".to_vec()),
                Event::End { failed: false }
            ]
        );

        let mut source = Scripted(VecDeque::from([chunk(&producer, b"a"), Ok(None)]));
        let inbox = Recorder::default();
        let result = drain_shuffle_push(
            &inbox,
            &receiver,
            &mut source,
            request(),
            PRODUCER,
            no_reject,
        )
        .await;
        assert!(matches!(
            result,
            Err(ClusterError::ShufflePush(ShufflePushError::Truncated {
                shuffle_id: 9,
                part: 3,
                side: 1,
            }))
        ));
        assert_eq!(
            inbox.events(),
            vec![
                Event::Open,
                Event::Deposit(b"a".to_vec()),
                Event::End { failed: true }
            ]
        );
    }

    /// A frame from a node other than the opener fails the drain and ends
    /// the inbox with the error.
    #[tokio::test]
    async fn foreign_frame_fails_the_drain() {
        let (receiver, _) = auths();
        let intruder = AuthContext::from_credentials(5, &TransportCredentials::Insecure);
        let mut source = Scripted(VecDeque::from([chunk(&intruder, b"x")]));
        let inbox = Recorder::default();
        let result = drain_shuffle_push(
            &inbox,
            &receiver,
            &mut source,
            request(),
            PRODUCER,
            |node| {
                Err(ClusterError::Transport {
                    detail: format!("peer identity mismatch for node {node}"),
                })
            },
        )
        .await;
        assert!(matches!(result, Err(ClusterError::Transport { .. })));
        assert_eq!(
            inbox.events(),
            vec![Event::Open, Event::End { failed: true }]
        );
    }
}
