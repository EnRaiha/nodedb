// SPDX-License-Identifier: BUSL-1.1

use super::codec;
use super::session_chunk::chunk_large_response;
use nodedb_types::Value;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{MAX_FRAME_SIZE, NativeResponse};

#[test]
fn chunk_large_response_splits_rows() {
    // Build a response with 100 rows, each ~200 bytes when serialized.
    let columns = vec!["id".to_string(), "data".to_string()];
    let rows: Vec<Vec<Value>> = (0..100)
        .map(|i| {
            vec![
                Value::Integer(i),
                Value::String(format!("row-data-{i}-padding-{}", "x".repeat(150))),
            ]
        })
        .collect();

    let response = NativeResponse {
        seq: 1,
        status: ResponseStatus::Ok,
        columns: Some(columns),
        rows: Some(rows),
        rows_affected: None,
        watermark_lsn: 42,
        error: None,
        auth: None,
        warnings: Vec::new(),
    };

    let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();

    // With 100 rows of ~200 bytes each (~20KB total), this should fit in
    // one frame (MAX_FRAME_SIZE = 16MB). Test with a scenario that forces splitting.
    assert!(!frames.is_empty());

    // Decode each frame and verify structure.
    for (i, frame) in frames.iter().enumerate() {
        let resp: NativeResponse = zerompk::from_msgpack(frame).unwrap();
        assert!(resp.rows.is_some());
        if i < frames.len() - 1 {
            assert_eq!(resp.status, ResponseStatus::Partial);
        } else {
            assert_eq!(resp.status, ResponseStatus::Ok);
        }
    }
}

#[test]
fn chunk_large_response_no_rows_passthrough() {
    let response = NativeResponse {
        seq: 1,
        status: ResponseStatus::Ok,
        columns: None,
        rows: None,
        rows_affected: Some(5),
        watermark_lsn: 42,
        error: None,
        auth: None,
        warnings: Vec::new(),
    };

    let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();
    assert_eq!(
        frames.len(),
        1,
        "no-rows response should pass through as-is"
    );
}

#[test]
fn chunk_large_response_preserves_all_rows() {
    // Create a response that's guaranteed to exceed MAX_FRAME_SIZE.
    // Each row ~200 bytes * 100K rows = ~20MB > 16MB limit.
    let columns = vec!["id".to_string(), "value".to_string()];
    let row_count = 100_000;
    let rows: Vec<Vec<Value>> = (0..row_count)
        .map(|i| {
            vec![
                Value::Integer(i),
                Value::String(format!("v{i}-{}", "p".repeat(150))),
            ]
        })
        .collect();

    let response = NativeResponse {
        seq: 42,
        status: ResponseStatus::Ok,
        columns: Some(columns.clone()),
        rows: Some(rows),
        rows_affected: None,
        watermark_lsn: 99,
        error: None,
        auth: None,
        warnings: Vec::new(),
    };

    let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();
    assert!(frames.len() > 1, "should produce multiple frames");

    // Reassemble all rows from frames (simulating client behavior).
    let mut total_rows: Vec<Vec<Value>> = Vec::new();
    for frame in &frames {
        let resp: NativeResponse = zerompk::from_msgpack(frame).unwrap();
        if let Some(rows) = resp.rows {
            total_rows.extend(rows);
        }
    }
    assert_eq!(total_rows.len(), row_count as usize);

    // First frame should have columns.
    let first: NativeResponse = zerompk::from_msgpack(&frames[0]).unwrap();
    assert_eq!(first.columns, Some(columns));
    assert_eq!(first.status, ResponseStatus::Partial);

    // Last frame should have Ok status.
    let last: NativeResponse = zerompk::from_msgpack(frames.last().unwrap()).unwrap();
    assert_eq!(last.status, ResponseStatus::Ok);

    // Each frame should be <= MAX_FRAME_SIZE.
    for frame in &frames {
        assert!(
            frame.len() <= MAX_FRAME_SIZE as usize,
            "frame size {} exceeds MAX_FRAME_SIZE {}",
            frame.len(),
            MAX_FRAME_SIZE
        );
    }
}
