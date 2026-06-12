// SPDX-License-Identifier: BUSL-1.1

//! Shared decode for the Data Plane's [`SyncAckResult`] reply.
//!
//! Every per-engine sync session (vector / fts / spatial / columnar /
//! timeseries) receives a msgpack-encoded [`SyncAckResult`] in the Data Plane
//! response payload and needs identical fallback behaviour when it fails to
//! decode. This module is the single place that decision lives.

use tracing::warn;

use nodedb_types::sync::wire::{AckStatus, SyncAckResult};

/// Decode the [`SyncAckResult`] returned by the Data Plane in a sync ingest
/// response payload.
///
/// A decode failure here is not expected: the dispatch already returned `Ok`,
/// meaning the engine write succeeded — only the small ack envelope failed to
/// parse. We log loudly and synthesise an `Applied` ack at `fallback_seq` so the
/// client still receives a positive acknowledgement for the durable write,
/// rather than a spurious rejection. Routing every engine session through this
/// one path keeps the fallback behaviour identical across engines.
pub(super) fn decode_sync_ack(
    payload_bytes: &[u8],
    op: &str,
    session_id: &str,
    collection: &str,
    fallback_seq: u64,
) -> SyncAckResult {
    match zerompk::from_msgpack::<SyncAckResult>(payload_bytes) {
        Ok(result) => result,
        Err(e) => {
            warn!(
                session = %session_id,
                %collection,
                op,
                error = %e,
                "sync: failed to decode SyncAckResult from Data Plane; using default Applied"
            );
            SyncAckResult {
                status: AckStatus::Applied,
                applied_seq: fallback_seq,
            }
        }
    }
}
