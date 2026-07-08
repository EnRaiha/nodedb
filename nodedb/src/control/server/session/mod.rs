// SPDX-License-Identifier: BUSL-1.1

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpStream;

use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Maximum frame size: 16 MiB.
const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

mod auth;
mod dispatch;
mod lifecycle;

#[cfg(test)]
mod tests;

use super::conn_stream::ConnStream;

/// A client session on the Control Plane.
///
/// Each accepted TCP connection gets its own `Session`. The session handles
/// protocol framing, request parsing, and dispatching via the shared state.
/// This is `Send + Sync` — runs on the Tokio thread pool.
///
/// ## Wire Protocol (length-prefixed binary)
///
/// ```text
/// Request frame:  [4 bytes: payload_len (big-endian u32)] [payload_len bytes: JSON body]
/// Response frame: [4 bytes: payload_len (big-endian u32)] [payload_len bytes: JSON body]
/// ```
///
/// Request JSON:
/// ```json
/// {
///   "op": "point_get" | "vector_search" | "range_scan" | "crdt_read" | "crdt_apply",
///   "tenant_id": 1,
///   "collection": "users",
///   "document_id": "doc-1",    // for point_get, crdt_read, crdt_apply
///   "query_vector": [0.1, ...], // for vector_search
///   "top_k": 10,                // for vector_search
///   "field": "age",             // for range_scan
///   "limit": 100,               // for range_scan
///   "delta": "base64...",       // for crdt_apply
///   "peer_id": 12345            // for crdt_apply
/// }
/// ```
///
/// Response JSON:
/// ```json
/// {
///   "request_id": 1,
///   "status": "ok" | "error",
///   "payload": "base64...",
///   "watermark_lsn": 42,
///   "error_code": null | "deadline_exceeded" | ...
/// }
/// ```
pub struct Session {
    stream: ConnStream,
    peer_addr: SocketAddr,
    state: Arc<SharedState>,
    auth_mode: crate::config::auth::AuthMode,
    /// Bound after auth handshake. None until first frame is processed.
    identity: Option<crate::control::security::identity::AuthenticatedIdentity>,
    /// Wall-clock time when this session was accepted.
    connected_at: std::time::Instant,
    /// Stable session identifier (UUID allocated at construction).
    session_id: String,
    /// Credential version at bind time.  When the store's version for this
    /// user advances, the session rehydrates `identity` at the next request
    /// boundary.
    identity_version: u64,
    /// Kill signal from `SessionRegistry`.  Set after successful auth.
    kill_rx: Option<tokio::sync::watch::Receiver<crate::control::security::sessions::KillReason>>,
    /// Database bound to this session. Set once at first authenticated request;
    /// immutable for the session lifetime (a `USE DATABASE` issues a session reset).
    /// Resolution order: explicit (connection-string/handshake) > user default >
    /// tenant default > `DatabaseId::DEFAULT`.
    current_database: Option<DatabaseId>,
}

impl Session {
    fn with_stream(
        stream: ConnStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: crate::config::auth::AuthMode,
    ) -> Self {
        Self {
            stream,
            peer_addr,
            state,
            auth_mode,
            identity: None,
            connected_at: std::time::Instant::now(),
            session_id: uuid::Uuid::new_v4().to_string(),
            identity_version: 0,
            kill_rx: None,
            current_database: None,
        }
    }

    /// Create a session from a plain TCP stream.
    pub fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: crate::config::auth::AuthMode,
    ) -> Self {
        Self::with_stream(ConnStream::plain(stream), peer_addr, state, auth_mode)
    }

    /// Create a session from a TLS-wrapped stream.
    pub fn new_tls(
        stream: tokio_rustls::server::TlsStream<TcpStream>,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: crate::config::auth::AuthMode,
    ) -> Self {
        Self::with_stream(ConnStream::tls(stream), peer_addr, state, auth_mode)
    }
}
