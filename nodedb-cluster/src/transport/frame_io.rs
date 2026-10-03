// SPDX-License-Identifier: BUSL-1.1

//! Authenticated frame I/O on QUIC streams.
//!
//! One outbound RPC frame is `rpc_codec::encode`, then a fresh outbound
//! `seq`, then `auth_envelope::write_envelope`, then one `write_all`.
//! [`write_rpc_frame`] does that sequence. [`read_envelope`] and
//! [`read_envelope_or_finish`] read one envelope back.

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, MAX_RPC_PAYLOAD_SIZE, RaftRpc, auth_envelope};
use crate::transport::auth_context::AuthContext;

/// Envelope pre-header: version(1) + from_node_id(8) + seq(8) + inner_len(4).
const ENV_HDR_LEN: usize = 21;

/// Encode `rpc` into one authenticated envelope from the local node.
///
/// Each call takes a fresh outbound `seq`.
pub(super) fn encode_rpc_frame(rpc: &RaftRpc, auth: &AuthContext) -> Result<Vec<u8>> {
    let inner = rpc_codec::encode(rpc, &auth.epoch)?;
    let seq = auth.peer_seq_out.next();
    let mut envelope = Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + inner.len());
    auth_envelope::write_envelope(
        auth.local_node_id,
        seq,
        &inner,
        &auth.mac_key,
        &mut envelope,
    )?;
    Ok(envelope)
}

/// Encode `rpc` into one authenticated envelope and write it on `send`.
///
/// The write is awaited inline, so QUIC flow control throttles the caller.
/// `what` names the frame in the error detail.
pub(super) async fn write_rpc_frame(
    send: &mut quinn::SendStream,
    auth: &AuthContext,
    rpc: &RaftRpc,
    what: &str,
) -> Result<()> {
    let envelope = encode_rpc_frame(rpc, auth)?;
    send.write_all(&envelope)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("write {what}: {e}"),
        })
}

/// Finish the send half of a response stream.
///
/// `what` names the response in the error detail.
pub(super) fn finish_stream(send: &mut quinn::SendStream, what: &str) -> Result<()> {
    send.finish().map_err(|e| ClusterError::Transport {
        detail: format!("finish {what}: {e}"),
    })
}

/// Write `rpc` as the single response frame on `send`, then finish `send`.
pub(super) async fn reply_and_finish(
    send: &mut quinn::SendStream,
    auth: &AuthContext,
    rpc: &RaftRpc,
    what: &str,
) -> Result<()> {
    write_rpc_frame(send, auth, rpc, what).await?;
    finish_stream(send, what)
}

/// Read a complete authenticated envelope from a QUIC receive stream.
///
/// Reads the fixed envelope pre-header, then the inner frame, then the MAC
/// tag. Returns the full envelope bytes for caller-side parsing. A stream
/// that finishes before the envelope is an error.
pub(crate) async fn read_envelope(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
    read_envelope_or_finish(recv)
        .await?
        .ok_or_else(|| ClusterError::Transport {
            detail: "read envelope header: stream finished before an envelope".into(),
        })
}

/// Read one authenticated envelope, or `None` on a clean finish.
///
/// A clean finish is the peer finishing its send half with 0 bytes of a
/// new envelope read. A finish part way through an envelope, a reset, or a
/// lost connection is an error.
pub(crate) async fn read_envelope_or_finish(
    recv: &mut quinn::RecvStream,
) -> Result<Option<Vec<u8>>> {
    let mut hdr = [0u8; ENV_HDR_LEN];
    if !header_read_outcome(recv.read_exact(&mut hdr).await)? {
        return Ok(None);
    }

    let inner_len = u32::from_le_bytes([hdr[17], hdr[18], hdr[19], hdr[20]]);
    if inner_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!(
                "envelope inner length {inner_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"
            ),
        });
    }

    let total = ENV_HDR_LEN + inner_len as usize + rpc_codec::MAC_LEN;
    let mut buf = vec![0u8; total];
    buf[..ENV_HDR_LEN].copy_from_slice(&hdr);
    if total > ENV_HDR_LEN {
        recv.read_exact(&mut buf[ENV_HDR_LEN..])
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("read envelope payload+mac: {e}"),
            })?;
    }

    Ok(Some(buf))
}

/// Classify the result of reading an envelope header.
///
/// `Ok(true)` is a full header. `Ok(false)` is a clean finish with 0 bytes
/// read. Every other outcome is a transport error.
fn header_read_outcome(read: std::result::Result<(), quinn::ReadExactError>) -> Result<bool> {
    match read {
        Ok(()) => Ok(true),
        Err(quinn::ReadExactError::FinishedEarly(0)) => Ok(false),
        Err(e) => Err(ClusterError::Transport {
            detail: format!("read envelope header: {e}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_codec::{ExecuteStreamEnd, parse_envelope};
    use crate::transport::credentials::TransportCredentials;

    #[test]
    fn only_a_zero_byte_finish_is_a_clean_end() {
        assert!(matches!(header_read_outcome(Ok(())), Ok(true)));
        assert!(matches!(
            header_read_outcome(Err(quinn::ReadExactError::FinishedEarly(0))),
            Ok(false)
        ));
        assert!(matches!(
            header_read_outcome(Err(quinn::ReadExactError::FinishedEarly(5))),
            Err(ClusterError::Transport { .. })
        ));
        assert!(matches!(
            header_read_outcome(Err(quinn::ReadExactError::ReadError(
                quinn::ReadError::ClosedStream
            ))),
            Err(ClusterError::Transport { .. })
        ));
    }

    #[test]
    fn encoded_frame_parses_back_with_fresh_seqs() {
        let auth = AuthContext::from_credentials(7, &TransportCredentials::Insecure);
        let rpc = RaftRpc::ExecuteStreamEnd(ExecuteStreamEnd { error: None });
        let first = encode_rpc_frame(&rpc, &auth).expect("encode first");
        let second = encode_rpc_frame(&rpc, &auth).expect("encode second");

        let (f1, inner1) = parse_envelope(&first, &auth.mac_key).expect("parse first");
        let (f2, _) = parse_envelope(&second, &auth.mac_key).expect("parse second");
        assert_eq!(f1.from_node_id, 7);
        assert!(f2.seq > f1.seq, "each frame takes a fresh seq");
        assert!(matches!(
            rpc_codec::decode(inner1, &auth.epoch).expect("decode"),
            RaftRpc::ExecuteStreamEnd(ExecuteStreamEnd { error: None })
        ));
    }
}
