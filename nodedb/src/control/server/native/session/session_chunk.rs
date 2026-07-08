// SPDX-License-Identifier: BUSL-1.1

//! Response chunking for the native binary protocol.

use nodedb_types::protocol::{MAX_FRAME_SIZE, NativeResponse, ResponseStatus};

use super::codec;

/// Split a large `NativeResponse` into multiple encoded frames that each
/// fit within `MAX_FRAME_SIZE`. Intermediate frames use `Partial` status;
/// the last frame uses the original status.
///
/// Only responses with `rows` data are split. Error or auth responses
/// that somehow exceed the frame limit are returned as-is (best effort).
pub(super) fn chunk_large_response(
    response: NativeResponse,
    format: codec::FrameFormat,
) -> crate::Result<Vec<Vec<u8>>> {
    let rows = match response.rows {
        Some(ref rows) if !rows.is_empty() => rows,
        _ => {
            // No rows to split — send as-is (shouldn't happen, but safe).
            return Ok(vec![codec::encode_response(&response, format)?]);
        }
    };

    // Estimate how many rows per chunk: target ~12 MiB per frame (75% of max)
    // to leave headroom for envelope overhead.
    let target_size = (MAX_FRAME_SIZE as usize) * 3 / 4;
    let total_rows = rows.len();

    // Estimate per-row size from a sample of the first rows.
    let sample_resp = NativeResponse {
        seq: response.seq,
        status: ResponseStatus::Ok,
        columns: response.columns.clone(),
        rows: Some(rows[..total_rows.min(100)].to_vec()),
        rows_affected: None,
        watermark_lsn: response.watermark_lsn,
        error: None,
        auth: None,
        warnings: Vec::new(),
    };
    let sample_bytes = codec::encode_response(&sample_resp, format)?;
    let sample_count = total_rows.min(100);
    let per_row_estimate = sample_bytes.len().checked_div(sample_count).unwrap_or(256);

    let rows_per_chunk = target_size
        .checked_div(per_row_estimate)
        .map(|v| v.max(1))
        .unwrap_or(1000);

    let mut frames = Vec::new();
    let chunks: Vec<_> = rows.chunks(rows_per_chunk).collect();
    let last_idx = chunks.len().saturating_sub(1);

    for (i, chunk) in chunks.iter().enumerate() {
        let is_last = i == last_idx;
        let frame_resp = NativeResponse {
            seq: response.seq,
            status: if is_last {
                response.status
            } else {
                ResponseStatus::Partial
            },
            columns: if i == 0 {
                response.columns.clone()
            } else {
                None
            },
            rows: Some(chunk.to_vec()),
            rows_affected: if is_last {
                response.rows_affected
            } else {
                None
            },
            watermark_lsn: response.watermark_lsn,
            error: if is_last {
                response.error.clone()
            } else {
                None
            },
            auth: None,
            warnings: Vec::new(),
        };
        frames.push(codec::encode_response(&frame_resp, format)?);
    }

    Ok(frames)
}
