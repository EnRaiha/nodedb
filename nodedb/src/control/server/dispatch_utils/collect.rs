// SPDX-License-Identifier: BUSL-1.1

//! Bounded response collection for dispatched requests: draining a streamed
//! response channel with a total-payload byte ceiling.

use crate::bridge::envelope::{Payload, Response};

#[derive(Debug)]
pub(crate) enum DispatchCollectError {
    OverBudget { bytes: usize },
    ChannelClosed,
}

/// Drain a dispatched request's bounded response channel, enforcing a
/// total-payload byte ceiling across streamed partials.
///
/// Returns the final Response (non-streaming: pass-through; streaming:
/// concatenated payload) or an error if the channel closed without a
/// final chunk or if the accumulated payload would exceed the ceiling.
pub(crate) async fn collect_bounded_response(
    rx: &mut tokio::sync::mpsc::Receiver<Response>,
    max_result_bytes: usize,
) -> Result<Response, DispatchCollectError> {
    // Each streamed chunk is its OWN msgpack array (`encode_raw_document_rows`
    // per chunk), so the chunks are accumulated separately and merged into a
    // single msgpack array at the end. Raw byte concatenation would leave every
    // chunk after the first as a trailing array that downstream single-array
    // decoders silently drop — truncating a streamed scan to `stream_chunk_size`
    // rows. The byte budget is enforced on the running total of raw chunk bytes
    // (the memory actually held), which is `>=` the merged-array size.
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut total_bytes: usize = 0;
    let mut final_response_meta: Option<Response> = None;

    loop {
        let Some(resp) = rx.recv().await else { break };
        if resp.partial {
            total_bytes = total_bytes.saturating_add(resp.payload.len());
            if total_bytes > max_result_bytes {
                return Err(DispatchCollectError::OverBudget { bytes: total_bytes });
            }
            chunks.push(resp.payload.to_vec());
        } else if chunks.is_empty() {
            // Non-streaming fast path: a single terminal frame is returned
            // unmodified (writes, point reads, DDL, counts, single-chunk scans).
            return Ok(resp);
        } else {
            total_bytes = total_bytes.saturating_add(resp.payload.len());
            if total_bytes > max_result_bytes {
                return Err(DispatchCollectError::OverBudget { bytes: total_bytes });
            }
            chunks.push(resp.payload.to_vec());
            final_response_meta = Some(resp);
            break;
        }
    }

    match final_response_meta {
        Some(meta) => Ok(Response {
            payload: Payload::from_vec(
                crate::control::server::payload_merge::merge_msgpack_arrays(&chunks),
            ),
            ..meta
        }),
        None => Err(DispatchCollectError::ChannelClosed),
    }
}

#[cfg(test)]
mod collect_budget_tests {
    use super::*;
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::{Lsn, RequestId};
    use tokio::sync::mpsc;

    use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};

    /// A standalone msgpack array of `n` one-byte elements — the shape a streamed
    /// scan chunk has (`encode_raw_document_rows` per chunk).
    fn array_payload(n: usize) -> Vec<u8> {
        let rows: Vec<Vec<u8>> = (0..n).map(|i| vec![(i % 128) as u8]).collect();
        encode_msgpack_array(&rows)
    }

    fn partial_rows(n: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(array_payload(n)),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn final_rows(n: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(array_payload(n)),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    /// Raw (non-array) payload, sized in bytes, for the budget-ceiling tests.
    fn partial_bytes(bytes: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(vec![0u8; bytes]),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn final_bytes(bytes: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(vec![0u8; bytes]),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    #[tokio::test]
    async fn non_streaming_single_response_passes_through() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(final_bytes(100)).await.unwrap();
        drop(tx);
        // Single terminal frame returns unmodified — no merge, exact bytes.
        let resp = collect_bounded_response(&mut rx, 1024).await.unwrap();
        assert_eq!(resp.payload.len(), 100);
    }

    #[tokio::test]
    async fn streaming_merges_all_chunk_arrays() {
        // Three standalone array chunks must merge into ONE array with every
        // element — the regression: raw concatenation kept only the first array.
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial_rows(1000)).await.unwrap();
        tx.send(partial_rows(1000)).await.unwrap();
        tx.send(final_rows(500)).await.unwrap();
        drop(tx);
        let resp = collect_bounded_response(&mut rx, 1 << 20).await.unwrap();
        let elements = extract_msgpack_elements(resp.payload.as_ref());
        assert_eq!(
            elements.len(),
            2500,
            "streamed chunks must merge into one array of all rows, not just the first chunk"
        );
    }

    #[tokio::test]
    async fn streaming_over_budget_on_partial_aborts() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial_bytes(600)).await.unwrap();
        tx.send(partial_bytes(600)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1000).await.unwrap_err();
        match err {
            DispatchCollectError::OverBudget { bytes } => assert!(bytes > 1000),
            DispatchCollectError::ChannelClosed => panic!("expected OverBudget, got ChannelClosed"),
        }
    }

    #[tokio::test]
    async fn streaming_over_budget_on_final_chunk_aborts() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial_bytes(500)).await.unwrap();
        tx.send(final_bytes(600)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1000).await.unwrap_err();
        assert!(matches!(err, DispatchCollectError::OverBudget { .. }));
    }

    #[tokio::test]
    async fn channel_closed_without_final_is_explicit_error() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial_bytes(10)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1024).await.unwrap_err();
        assert!(matches!(err, DispatchCollectError::ChannelClosed));
    }
}
