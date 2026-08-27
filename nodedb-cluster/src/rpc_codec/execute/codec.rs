// SPDX-License-Identifier: BUSL-1.1

//! rkyv frame encode / decode for the execute RPC family.
//!
//! Discriminants 18 and 19, plus the three streaming discriminants, are
//! permanently assigned to these variants.

use crate::error::{ClusterError, Result};
use crate::rpc_codec::discriminants::*;
use crate::rpc_codec::header::write_frame;
use crate::rpc_codec::raft_rpc::RaftRpc;

use super::types::{ExecuteRequest, ExecuteResponse, ExecuteStreamChunk, ExecuteStreamEnd};

macro_rules! to_bytes {
    ($msg:expr) => {
        rkyv::to_bytes::<rkyv::rancor::Error>($msg)
            .map(|b| b.to_vec())
            .map_err(|e| ClusterError::Codec {
                detail: format!("rkyv serialize: {e}"),
            })
    };
}

macro_rules! from_bytes {
    ($payload:expr, $T:ty, $name:expr) => {{
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity($payload.len());
        aligned.extend_from_slice($payload);
        rkyv::from_bytes::<$T, rkyv::rancor::Error>(&aligned).map_err(|e| ClusterError::Codec {
            detail: format!("rkyv deserialize {}: {e}", $name),
        })
    }};
}

pub(in crate::rpc_codec) fn encode_execute_req(
    msg: &ExecuteRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_EXECUTE_REQ, &to_bytes!(msg)?, out)
}
pub(in crate::rpc_codec) fn encode_execute_resp(
    msg: &ExecuteResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_EXECUTE_RESP, &to_bytes!(msg)?, out)
}

pub(in crate::rpc_codec) fn decode_execute_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteRequest(from_bytes!(
        payload,
        ExecuteRequest,
        "ExecuteRequest"
    )?))
}
pub(in crate::rpc_codec) fn decode_execute_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteResponse(from_bytes!(
        payload,
        ExecuteResponse,
        "ExecuteResponse"
    )?))
}

pub(in crate::rpc_codec) fn encode_execute_stream_req(
    msg: &ExecuteRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_EXECUTE_STREAM_REQ, &to_bytes!(msg)?, out)
}
pub(in crate::rpc_codec) fn encode_execute_stream_chunk(
    msg: &ExecuteStreamChunk,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_EXECUTE_STREAM_CHUNK, &to_bytes!(msg)?, out)
}
pub(in crate::rpc_codec) fn encode_execute_stream_end(
    msg: &ExecuteStreamEnd,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_EXECUTE_STREAM_END, &to_bytes!(msg)?, out)
}

pub(in crate::rpc_codec) fn decode_execute_stream_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteStreamRequest(from_bytes!(
        payload,
        ExecuteRequest,
        "ExecuteStreamRequest"
    )?))
}
pub(in crate::rpc_codec) fn decode_execute_stream_chunk(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteStreamChunk(from_bytes!(
        payload,
        ExecuteStreamChunk,
        "ExecuteStreamChunk"
    )?))
}
pub(in crate::rpc_codec) fn decode_execute_stream_end(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteStreamEnd(from_bytes!(
        payload,
        ExecuteStreamEnd,
        "ExecuteStreamEnd"
    )?))
}
