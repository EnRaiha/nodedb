// SPDX-License-Identifier: BUSL-1.1

//! Minimal ILP (InfluxDB Line Protocol) client for integration tests.
//!
//! The ILP port has no credential grammar of its own: a connection must
//! complete the native Hello + exactly one native `Auth` request before raw
//! ILP bytes are accepted (`control/server/ilp_auth.rs`). This reuses the
//! same handshake/frame plumbing as `native_handshake_e2e.rs` and the
//! native harness in this crate — `do_handshake` for the Hello exchange and
//! `send_request` to encode/send the framed Auth request and decode its
//! response — rather than re-deriving the wire encoding. Once authenticated,
//! ILP lines are newline-delimited raw text written directly to the stream:
//! no further framing, and no per-line acknowledgement.

#![allow(dead_code)] // Not every test binary exercises every helper.

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::native_harness::{do_handshake_from, send_request};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{AuthMethod, HelloFrame, OpCode};

/// Complete the native Hello + Auth prelude against the ILP port and return
/// the stream ready to accept raw ILP lines.
///
/// Panics if the handshake or Auth request fails — a crash test's ILP write
/// must never silently degrade into "wrote nothing" being mistaken for a
/// durability failure downstream.
pub async fn connect_and_auth(
    addr: std::net::SocketAddr,
    username: &str,
    password: &str,
) -> TcpStream {
    connect_and_auth_from(None, addr, username, password).await
}

/// Like [`connect_and_auth`], but binds the client socket to `source` so the
/// server sees a chosen peer address — used by tests that assert an
/// address-scoped guard distinguishes two clients on the same host.
pub async fn connect_and_auth_from(
    source: Option<std::net::SocketAddr>,
    addr: std::net::SocketAddr,
    username: &str,
    password: &str,
) -> TcpStream {
    let (mut stream, _ack) = do_handshake_from(source, addr, &HelloFrame::current())
        .await
        .expect("ILP native Hello handshake");

    let auth = send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Password {
                username: username.into(),
                password: password.into(),
            }),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        auth.status,
        ResponseStatus::Ok,
        "ILP native Auth frame must succeed before raw ILP lines are accepted: {auth:?}"
    );

    stream
}

/// Write one raw ILP line to an authenticated stream, appending the
/// terminating newline if the caller did not include one.
///
/// ILP sends no per-line acknowledgement, so this only proves the bytes left
/// the client — the caller must poll a reader on another protocol to observe
/// when the write becomes visible.
pub async fn send_line(stream: &mut TcpStream, line: &str) {
    let mut framed = line.to_string();
    if !framed.ends_with('\n') {
        framed.push('\n');
    }
    stream
        .write_all(framed.as_bytes())
        .await
        .expect("ILP line write");
    stream.flush().await.expect("ILP line flush");
}
