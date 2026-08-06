// SPDX-License-Identifier: BUSL-1.1

//! `GET /v1/cluster/debug/quarantined-segments` — list segments quarantined
//! due to repeated CRC failures.

use axum::extract::State;
use axum::response::Response;

use super::super::super::auth::{AppState, ResolvedIdentity};
use super::super::super::peer::PeerAddr;
use super::guard::{ensure_debug_access, ok_json};

/// Individual entry in the quarantine list.
#[derive(serde::Serialize)]
struct QuarantinedSegmentEntry {
    engine: String,
    collection: String,
    segment_id: String,
    quarantined_at_unix_ms: u64,
    last_error_summary: String,
    strikes: u32,
}

/// Response shape for `GET /v1/cluster/debug/quarantined-segments`.
#[derive(serde::Serialize)]
struct QuarantinedSegmentsResponse {
    segments: Vec<QuarantinedSegmentEntry>,
}

/// GET /v1/cluster/debug/quarantined-segments
pub async fn quarantined_segments(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
) -> Response {
    if let Some(resp) = ensure_debug_access(&state, &identity, peer.as_str()) {
        return resp;
    }

    let snapshot = state.shared.quarantine_registry.quarantined_snapshot();
    let mut entries: Vec<QuarantinedSegmentEntry> = snapshot
        .into_iter()
        .map(|s| QuarantinedSegmentEntry {
            engine: s.engine,
            collection: s.collection,
            segment_id: s.segment_id,
            quarantined_at_unix_ms: s.quarantined_at_unix_ms,
            last_error_summary: s.last_error_summary,
            strikes: s.strikes,
        })
        .collect();
    // Deterministic order: engine → collection → segment_id.
    entries.sort_by(|a, b| {
        a.engine
            .cmp(&b.engine)
            .then_with(|| a.collection.cmp(&b.collection))
            .then_with(|| a.segment_id.cmp(&b.segment_id))
    });

    ok_json(&QuarantinedSegmentsResponse { segments: entries })
}
