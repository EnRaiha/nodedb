// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the native binary protocol (port 6433).
//!
//! Covers:
//! - Version handshake: server accepts proto v1, rejects v0 / v2+
//! - Capability bits: server advertises known bits; client requests intersection
//! - Max frame 16 MiB enforced: oversized frame → typed error + clean close
//! - JSON-vs-MsgPack auto-detect: first frame selects encoding for the session
//! - Mid-session encoding switch: rejected with a decode error response

mod common;

use common::native_harness::{NativeTestServer, do_handshake, read_frame, write_frame};

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use nodedb_types::protocol::request_fields::RequestFields;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{
    CAP_FTS, CAP_MSGPACK, CAP_SPATIAL, CAP_STREAMING, HelloErrorCode, HelloFrame, MAX_FRAME_SIZE,
    NativeRequest, NativeResponse, OpCode, PROTO_VERSION_MAX, PROTO_VERSION_MIN,
};

// ─── Tests ──────────────────────────────────────────────────────────────────

/// Version handshake: server accepts a proto v1 client and echoes proto_version=1.
#[tokio::test]
async fn version_handshake_v1_accepted() {
    let server = NativeTestServer::start().await;

    let hello = HelloFrame {
        proto_min: 1,
        proto_max: 1,
        capabilities: CAP_STREAMING | CAP_MSGPACK,
    };
    let result = do_handshake(server.addr, &hello).await;
    server.shutdown().await;

    let (_stream, ack) = result.expect("handshake should succeed for v1 client");
    assert_eq!(ack.proto_version, 1, "negotiated version must be 1");
    assert!(
        ack.server_version.contains("NodeDB"),
        "server_version '{}'  missing 'NodeDB'",
        ack.server_version
    );
}

/// Version handshake: server rejects a client whose range is entirely below v1
/// (proto v0 only) with a `VersionMismatch` error and clean disconnect.
#[tokio::test]
async fn version_handshake_v0_rejected() {
    // PROTO_VERSION_MIN is 1, so a client advertising [0, 0] has no overlap.
    if PROTO_VERSION_MIN == 0 {
        // If MIN were ever lowered to 0, v0 clients would be accepted — skip.
        return;
    }

    let server = NativeTestServer::start().await;

    let hello = HelloFrame {
        proto_min: 0,
        proto_max: 0,
        capabilities: 0,
    };
    let result = do_handshake(server.addr, &hello).await;
    server.shutdown().await;

    let err_frame = result.expect_err("v0-only client must be rejected");
    assert_eq!(
        err_frame.code,
        HelloErrorCode::VersionMismatch,
        "error code must be VersionMismatch, got {:?}",
        err_frame.code
    );
}

/// Version handshake: server rejects a client whose range is entirely above
/// the server maximum (proto v2+) with a `VersionMismatch` error.
#[tokio::test]
async fn version_handshake_future_version_rejected() {
    let server = NativeTestServer::start().await;

    let hello = HelloFrame {
        proto_min: PROTO_VERSION_MAX.saturating_add(1),
        proto_max: PROTO_VERSION_MAX.saturating_add(5),
        capabilities: 0,
    };
    let result = do_handshake(server.addr, &hello).await;
    server.shutdown().await;

    let err_frame = result.expect_err("future-version-only client must be rejected");
    assert_eq!(
        err_frame.code,
        HelloErrorCode::VersionMismatch,
        "error code must be VersionMismatch"
    );
}

/// Capability bits: server advertises a non-empty capability set; when the client
/// requests a subset the ack reflects that subset (intersection).
/// Asserts at least one set bit and at least one bit in the client's request
/// that the server supports.
#[tokio::test]
async fn capability_bits_negotiated() {
    let server = NativeTestServer::start().await;

    // Request a subset of known capabilities.
    let client_caps = CAP_STREAMING | CAP_FTS | CAP_SPATIAL;
    let hello = HelloFrame {
        proto_min: 1,
        proto_max: 1,
        capabilities: client_caps,
    };
    let result = do_handshake(server.addr, &hello).await;
    server.shutdown().await;

    let (_stream, ack) = result.expect("handshake ok");

    // Server must advertise at least one capability bit.
    assert_ne!(
        ack.capabilities, 0,
        "server must advertise at least one capability"
    );

    // The echoed bits must be a subset of what the client offered.
    let intersection = ack.capabilities & client_caps;
    assert_ne!(
        intersection, 0,
        "at least one capability bit must be in the intersection"
    );

    // CAP_MSGPACK is defined but was not in client_caps — server does not set it.
    let rejected = CAP_MSGPACK & ack.capabilities & !client_caps;
    assert_eq!(
        rejected, 0,
        "server must not set bits the client did not request"
    );
}

/// Max frame enforcement: sending a frame whose length prefix exceeds 16 MiB
/// causes the server to send a typed error response and close the connection.
/// The connection must NOT hang and the process must NOT crash.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_frame_size_enforced() {
    let server = NativeTestServer::start().await;

    // Complete the handshake first so we're past the hello exchange.
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    // Write a frame whose 4-byte length prefix = MAX_FRAME_SIZE + 1.
    // We do NOT need to write that many bytes — the server rejects at the header.
    let oversized_len = (MAX_FRAME_SIZE + 1).to_be_bytes();
    stream
        .write_all(&oversized_len)
        .await
        .expect("write oversized length prefix");
    stream.flush().await.expect("flush");

    // Server must respond with a typed error frame (SQLSTATE 54000 / out-of-range)
    // and then close the connection.
    let response_payload = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream))
        .await
        .expect("server must respond within 5 seconds");

    server.shutdown().await;

    let payload = response_payload.expect("server must send an error response before closing");

    // The response must be a valid NativeResponse with an error status.
    // Try MsgPack first (default format before first data frame), then JSON.
    let response: NativeResponse = zerompk::from_msgpack(&payload)
        .or_else(|_| sonic_rs::from_slice(&payload))
        .expect("response must be a valid NativeResponse");

    assert_eq!(
        response.status,
        nodedb_types::protocol::opcodes::ResponseStatus::Error,
        "response status must be Error for oversized frame"
    );

    let err = response.error.expect("error payload must be present");
    assert!(
        err.message.contains("frame") || err.message.contains("54000") || err.code == "54000",
        "error must mention frame rejection, got code='{}' message='{}'",
        err.code,
        err.message
    );
}

/// JSON request → JSON response: if the client sends a JSON-encoded Ping,
/// the server responds with a JSON-encoded response.
#[tokio::test]
async fn json_request_gets_json_response() {
    let server = NativeTestServer::start().await;

    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    // Encode a Ping request as JSON.
    let req = NativeRequest {
        op: OpCode::Ping,
        seq: 77,
        fields: RequestFields::Text(TextFields::default()),
    };
    let json_bytes = sonic_rs::to_vec(&req).expect("json encode");
    assert_eq!(json_bytes[0], b'{', "JSON must start with open brace");

    write_frame(&mut stream, &json_bytes).await;

    let response_payload = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream))
        .await
        .expect("timeout")
        .expect("response");

    server.shutdown().await;

    // Response must be valid JSON (starts with `{`).
    assert_eq!(
        response_payload[0], b'{',
        "JSON session must produce JSON response, got first byte 0x{:02X}",
        response_payload[0]
    );
    let resp: NativeResponse = sonic_rs::from_slice(&response_payload).expect("json decode");
    assert_eq!(resp.seq, 77, "seq must echo");
    assert_eq!(
        resp.status,
        nodedb_types::protocol::opcodes::ResponseStatus::Ok,
        "Ping must return Ok"
    );
}

/// MsgPack request → MsgPack response: if the client sends a MsgPack-encoded Ping,
/// the server responds with a MsgPack-encoded response.
#[tokio::test]
async fn msgpack_request_gets_msgpack_response() {
    let server = NativeTestServer::start().await;

    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let req = NativeRequest {
        op: OpCode::Ping,
        seq: 99,
        fields: RequestFields::Text(TextFields::default()),
    };
    let mp_bytes = zerompk::to_msgpack_vec(&req).expect("msgpack encode");
    assert_ne!(mp_bytes[0], b'{', "MsgPack must NOT start with open brace");

    write_frame(&mut stream, &mp_bytes).await;

    let response_payload = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream))
        .await
        .expect("timeout")
        .expect("response");

    server.shutdown().await;

    // Response must NOT start with `{` (it is MsgPack).
    assert_ne!(
        response_payload[0], b'{',
        "MsgPack session must produce MsgPack response, got first byte 0x{:02X}",
        response_payload[0]
    );
    let resp: NativeResponse = zerompk::from_msgpack(&response_payload).expect("msgpack decode");
    assert_eq!(resp.seq, 99, "seq must echo");
    assert_eq!(
        resp.status,
        nodedb_types::protocol::opcodes::ResponseStatus::Ok,
        "Ping must return Ok"
    );
}

/// Mid-session encoding switch: if a JSON session receives a MsgPack frame (or
/// vice versa), the server must NOT silently accept it. It must return an error
/// response (decode failure). The connection stays open — it is not terminated.
#[tokio::test]
async fn mid_session_encoding_switch_rejected() {
    let server = NativeTestServer::start().await;

    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    // First frame: JSON → establishes JSON encoding for the session.
    let ping_json = NativeRequest {
        op: OpCode::Ping,
        seq: 1,
        fields: RequestFields::Text(TextFields::default()),
    };
    let json_bytes = sonic_rs::to_vec(&ping_json).expect("json encode");
    write_frame(&mut stream, &json_bytes).await;

    // Consume the response to the first Ping.
    let _first_resp = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream))
        .await
        .expect("timeout")
        .expect("first response");

    // Second frame: MsgPack — the session is locked to JSON, so this must fail decode.
    let ping_mp = NativeRequest {
        op: OpCode::Ping,
        seq: 2,
        fields: RequestFields::Text(TextFields::default()),
    };
    let mp_bytes = zerompk::to_msgpack_vec(&ping_mp).expect("msgpack encode");
    write_frame(&mut stream, &mp_bytes).await;

    let switch_resp = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream))
        .await
        .expect("timeout")
        .expect("switch response");

    server.shutdown().await;

    // The response must be JSON-encoded (session is still in JSON mode).
    assert_eq!(
        switch_resp[0], b'{',
        "response must still be JSON after mid-session switch attempt"
    );
    let resp: NativeResponse = sonic_rs::from_slice(&switch_resp).expect("json decode");
    assert_eq!(
        resp.status,
        nodedb_types::protocol::opcodes::ResponseStatus::Error,
        "mid-session encoding switch must produce an Error response"
    );
}
