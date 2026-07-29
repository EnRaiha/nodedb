// SPDX-License-Identifier: BUSL-1.1

//! Shared unit-test helpers for the per-engine sync handlers.
//!
//! The mock dispatchers in `vector_handler`, `spatial_handler`, and
//! `fts_handler` implement different traits (so the impls cannot be shared),
//! but they produce their ACK payload identically. That one piece lives here.

/// The msgpack `SyncAckResult` bytes a mock dispatcher returns: an `Applied`
/// ack at `seq` on success, or a propagated `Internal` error on failure.
pub(super) fn mock_applied_ack(result: &crate::Result<()>, seq: u64) -> crate::Result<Vec<u8>> {
    match result {
        Ok(()) => {
            let ack = nodedb_types::sync::wire::SyncAckResult::acked(
                nodedb_types::sync::wire::AckStatus::Applied,
                seq,
            );
            zerompk::to_msgpack_vec(&ack).map_err(|e| crate::Error::Internal {
                detail: e.to_string(),
            })
        }
        Err(e) => Err(crate::Error::Internal {
            detail: e.to_string(),
        }),
    }
}
