// SPDX-License-Identifier: BUSL-1.1

//! One-shot RPC dispatch arms extracted from [`super::server::handle_stream`].
//!
//! Each arm in [`try_handle_oneshot_rpc`] corresponds to a single
//! request/response exchange that does NOT need access to the inbound
//! [`quinn::RecvStream`] — it reads its single request frame from the
//! already-decoded [`RaftRpc`] value, calls the handler, encodes the response,
//! and finishes the send stream.
//!
//! Arms that read additional frames from `recv` (the `ExecuteStreamRequest`
//! and `ShufflePushRequest` streaming arms) remain in `server.rs` because
//! their borrow shape differs.

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{
    self, AssignSurrogateResponse, RaftRpc, ShuffleAggregateConsumeResponse,
    ShuffleConsumeResponse, ShuffleProduceResponse, auth_envelope,
};
use crate::transport::auth_context::AuthContext;
use crate::transport::rpc_handler::RaftRpcHandler;

/// Attempt to handle a one-shot (single-request / single-response) RPC.
///
/// Checks whether `request` is one of the known one-shot shuffle variants
/// (ShuffleProduce / ShuffleConsume / ShuffleAggregateConsume). If it is,
/// the handler is called, the response is encoded and written on `send`, the
/// stream is finished, and `Ok(None)` is returned — indicating to the caller
/// that it should return immediately.
///
/// If `request` is not one of these variants it is returned as
/// `Ok(Some(request))` so the caller can continue with the normal dispatch
/// path.
pub(super) async fn try_handle_oneshot_rpc<H: RaftRpcHandler>(
    handler: &H,
    request: RaftRpc,
    send: &mut quinn::SendStream,
    auth: &AuthContext,
) -> Result<Option<RaftRpc>> {
    // 4d. Cross-node shuffle PRODUCER trigger (E4a): a `ShuffleProduceRequest`
    //     is a ONE-SHOT request/response (NOT a stream from the coordinator).
    //     The producer runs a local scan, fans the hash-partitioned rows out
    //     to the part-owners on its OWN outbound `ShufflePush` streams, then
    //     replies with exactly one `ShuffleProduceResponse` carrying terminal
    //     success or a typed error so the coordinator can await completion.
    //     The reply is written on this same bidi stream's send half (mirroring
    //     the one-shot `handle_rpc` reply below), then `finish()`ed.
    if let RaftRpc::ShuffleProduceRequest(req) = request {
        let error = handler.on_shuffle_produce(req).await;
        let resp_rpc = RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse { error });
        let resp_inner = rpc_codec::encode(&resp_rpc)?;
        let resp_seq = auth.peer_seq_out.next();
        let mut resp_envelope =
            Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + resp_inner.len());
        auth_envelope::write_envelope(
            auth.local_node_id,
            resp_seq,
            &resp_inner,
            &auth.mac_key,
            &mut resp_envelope,
        )?;
        send.write_all(&resp_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write shuffle produce response: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish shuffle produce response: {e}"),
        })?;
        return Ok(None);
    }

    // 4e. Cross-node shuffle CONSUMER trigger (E4b): a `ShuffleConsumeRequest`
    //     is a ONE-SHOT request/response. The part-owner waits for both staged
    //     sides of its part to finalize, runs the node-local grace join, and
    //     replies with exactly one `ShuffleConsumeResponse` carrying the join
    //     rows (or a typed error). The reply is written on this same bidi
    //     stream's send half (mirroring the produce arm above), then
    //     `finish()`ed.
    if let RaftRpc::ShuffleConsumeRequest(req) = request {
        let resp: ShuffleConsumeResponse = handler.on_shuffle_consume(req).await;
        let resp_rpc = RaftRpc::ShuffleConsumeResponse(resp);
        let resp_inner = rpc_codec::encode(&resp_rpc)?;
        let resp_seq = auth.peer_seq_out.next();
        let mut resp_envelope =
            Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + resp_inner.len());
        auth_envelope::write_envelope(
            auth.local_node_id,
            resp_seq,
            &resp_inner,
            &auth.mac_key,
            &mut resp_envelope,
        )?;
        send.write_all(&resp_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write shuffle consume response: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish shuffle consume response: {e}"),
        })?;
        return Ok(None);
    }

    // 4f. Cross-node distributed GROUP BY shuffle CONSUMER trigger (E5b): a
    //     `ShuffleAggregateConsumeRequest` is a ONE-SHOT request/response and
    //     the single-sided aggregate sibling of the consume arm above. The
    //     part-owner waits for its part's single staged producer side to
    //     finalize, merges + finalizes the partial states, and replies with
    //     exactly one `ShuffleAggregateConsumeResponse` carrying the aggregate
    //     rows (or a typed error). The reply is written on this same bidi
    //     stream's send half (mirroring the consume arm above), then
    //     `finish()`ed.
    if let RaftRpc::ShuffleAggregateConsumeRequest(req) = request {
        let resp: ShuffleAggregateConsumeResponse = handler.on_shuffle_aggregate(req).await;
        let resp_rpc = RaftRpc::ShuffleAggregateConsumeResponse(resp);
        let resp_inner = rpc_codec::encode(&resp_rpc)?;
        let resp_seq = auth.peer_seq_out.next();
        let mut resp_envelope =
            Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + resp_inner.len());
        auth_envelope::write_envelope(
            auth.local_node_id,
            resp_seq,
            &resp_inner,
            &auth.mac_key,
            &mut resp_envelope,
        )?;
        send.write_all(&resp_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write shuffle aggregate consume response: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish shuffle aggregate consume response: {e}"),
        })?;
        return Ok(None);
    }

    // 4g. Routed-surrogate-exchange (F1b): an `AssignSurrogateRequest` is a
    //     ONE-SHOT request/response. This node is the home vShard's leader; it
    //     assign-or-returns the authoritative surrogate for the `(collection,
    //     pk)` endpoint key and replies with exactly one
    //     `AssignSurrogateResponse` carrying the surrogate (or a typed error).
    //     The reply is written on this same bidi stream's send half (mirroring
    //     the consume arms above), then `finish()`ed.
    if let RaftRpc::AssignSurrogateRequest(req) = request {
        let resp: AssignSurrogateResponse = handler.on_assign_surrogate(req).await;
        let resp_rpc = RaftRpc::AssignSurrogateResponse(resp);
        let resp_inner = rpc_codec::encode(&resp_rpc)?;
        let resp_seq = auth.peer_seq_out.next();
        let mut resp_envelope =
            Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + resp_inner.len());
        auth_envelope::write_envelope(
            auth.local_node_id,
            resp_seq,
            &resp_inner,
            &auth.mac_key,
            &mut resp_envelope,
        )?;
        send.write_all(&resp_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write assign surrogate response: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish assign surrogate response: {e}"),
        })?;
        return Ok(None);
    }

    Ok(Some(request))
}
