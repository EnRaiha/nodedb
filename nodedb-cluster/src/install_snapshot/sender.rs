// SPDX-License-Identifier: BUSL-1.1

//! Leader-side chunked `InstallSnapshot` sender.
//!
//! Slices `snapshot_bytes` into chunks of at most `chunk_bytes`, wraps each
//! with [`nodedb_raft::encode_snapshot_chunk`] framing, and fires one
//! `InstallSnapshotRequest` RPC per chunk. When `snapshot_bytes` is empty,
//! exactly one chunk is emitted with `data = vec![]` and `done = true` — this
//! is the bootstrap stub path that keeps `tick.rs` correct even before any
//! engine ships real snapshot data.
//!
//! The caller is responsible for calling this inside a `tokio::spawn` task
//! (as the existing tick loop already does) so the RPC does not block the
//! tick pipeline.

use nodedb_raft::{
    InstallSnapshotRequest, SnapshotEngineId, encode_snapshot_chunk, transport::RaftTransport,
};

use crate::error::ClusterError;
use crate::transport::NexarTransport;

/// Parameters for [`send_chunked`].
pub struct SendChunkedParams<'a> {
    pub peer: u64,
    pub group_id: u64,
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub snapshot_bytes: &'a [u8],
    pub chunk_bytes: u64,
}

/// Leader-side chunked send for a single peer.
///
/// Emits `ceil(snapshot_bytes.len() / chunk_bytes)` RPCs (minimum 1 for an
/// empty snapshot). On RPC failure the function returns the error immediately;
/// the caller should log and not retry — the next Raft tick will re-schedule
/// the snapshot if the peer is still behind.
///
/// Returns the final `InstallSnapshotResponse.term` so the tick loop can
/// detect a higher-term response and step down.
pub async fn send_chunked(
    transport: &NexarTransport,
    params: SendChunkedParams<'_>,
) -> Result<u64, ClusterError> {
    let SendChunkedParams {
        peer,
        group_id,
        term,
        leader_id,
        last_included_index,
        last_included_term,
        snapshot_bytes,
        chunk_bytes,
    } = params;
    // For an empty snapshot we send exactly one stub chunk with done=true.
    if snapshot_bytes.is_empty() {
        let req = InstallSnapshotRequest {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset: 0,
            data: vec![],
            done: true,
            group_id,
            total_size: 0,
        };
        let resp =
            transport
                .install_snapshot(peer, req)
                .await
                .map_err(|e| ClusterError::Transport {
                    detail: format!("install_snapshot peer={peer} group={group_id}: {e}"),
                })?;
        return Ok(resp.term);
    }

    let chunk_size = chunk_bytes.max(1) as usize;
    let total = snapshot_bytes.len() as u64;
    let mut offset = 0usize;
    let mut last_term = term;

    while offset < snapshot_bytes.len() {
        let end = (offset + chunk_size).min(snapshot_bytes.len());
        let chunk_payload = &snapshot_bytes[offset..end];
        let done = end == snapshot_bytes.len();

        // Framing: wrap each chunk with the snapshot frame header (magic +
        // version + engine id + CRC) so the receiver's `handle_rpc` boundary can
        // validate per-chunk integrity before stripping the header and
        // accumulating the raw payload. The DataPlane snapshot payload is a
        // whole-tenant composite (`TenantDataSnapshot` spanning every engine),
        // so it is tagged with the `Composite` engine id rather than any single
        // engine's. `offset`/`total_size` below are payload-space (into
        // `snapshot_bytes`); the receiver reassembles the stripped payloads.
        let framed = encode_snapshot_chunk(SnapshotEngineId::Composite, chunk_payload);

        let req = InstallSnapshotRequest {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset: offset as u64,
            data: framed,
            done,
            group_id,
            total_size: total,
        };
        let resp =
            transport
                .install_snapshot(peer, req)
                .await
                .map_err(|e| ClusterError::Transport {
                    detail: format!(
                        "install_snapshot peer={peer} group={group_id} offset={offset}: {e}"
                    ),
                })?;
        last_term = resp.term;
        offset = end;
    }

    Ok(last_term)
}
