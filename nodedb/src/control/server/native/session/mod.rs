// SPDX-License-Identifier: BUSL-1.1

//! Native protocol session: the run loop that reads frames, routes
//! by opcode, and writes responses.
//!
//! Replaces the legacy JSON-only `Session` with auto-detection of
//! JSON vs MessagePack and full SQL/DDL/transaction support.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::TcpStream;

use tokio::sync::OwnedSemaphorePermit;

use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::admission::{AdmissionRegistry, ConnectionPermit};
use crate::control::server::conn_stream::ConnStream;
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;

use super::codec::{self, FrameFormat};
use super::dispatch;

mod auth;
mod request;
mod run;

mod session_chunk;
mod session_stream;

#[cfg(test)]
mod tests;

use session_chunk::chunk_large_response;

/// A client session on the native binary protocol.
///
/// Auto-detects JSON vs MessagePack on the first frame. Supports all
/// operations: auth, SQL, DDL, transactions, direct Data Plane ops.
///
/// Admission is two-phase:
/// 1. A global connection permit is acquired at TCP accept (before this
///    struct is created) and handed in via `global_permit`.
/// 2. After successful authentication, per-database and per-tenant permits
///    are acquired from `admission_registry` and combined with the global
///    permit into a `ConnectionPermit` that is held for the connection's
///    lifetime.
pub struct NativeSession {
    stream: ConnStream,
    peer_addr: SocketAddr,
    state: Arc<SharedState>,
    auth_mode: AuthMode,
    identity: Option<AuthenticatedIdentity>,
    auth_context: Option<crate::control::security::auth_context::AuthContext>,
    format: Option<FrameFormat>,
    query_ctx: QueryContext,
    sessions: SessionStore,
    /// Wall-clock time when this session was accepted. Used for absolute
    /// session lifetime enforcement (`session_absolute_timeout_secs`).
    connected_at: Instant,
    /// Protocol version negotiated during the handshake.
    pub proto_ver: u16,
    /// Registry for per-database and per-tenant connection caps. Used after
    /// authentication to acquire Phase 2 admission permits.
    admission_registry: Arc<AdmissionRegistry>,
    /// Phase 1 global connection slot. Held until a `ConnectionPermit` is
    /// assembled after auth, at which point it is moved into the permit.
    /// `None` after the permit is assembled.
    global_permit: Option<OwnedSemaphorePermit>,
    /// Full three-level permit assembled after authentication.
    /// `None` until auth succeeds.
    connection_permit: Option<ConnectionPermit>,
}

impl NativeSession {
    fn with_stream(
        stream: ConnStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        admission_registry: Arc<AdmissionRegistry>,
        global_permit: OwnedSemaphorePermit,
    ) -> Self {
        let query_ctx = QueryContext::for_state(&state);
        Self {
            stream,
            peer_addr,
            state,
            auth_mode,
            identity: None,
            auth_context: None,
            format: None,
            query_ctx,
            sessions: SessionStore::new(),
            connected_at: Instant::now(),
            proto_ver: 0,
            admission_registry,
            global_permit: Some(global_permit),
            connection_permit: None,
        }
    }

    /// Create a session from a plain TCP stream.
    pub fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        admission_registry: Arc<AdmissionRegistry>,
        global_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self::with_stream(
            ConnStream::plain(stream),
            peer_addr,
            state,
            auth_mode,
            admission_registry,
            global_permit,
        )
    }

    /// Create a session from a TLS-wrapped stream.
    pub fn new_tls(
        stream: tokio_rustls::server::TlsStream<TcpStream>,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        admission_registry: Arc<AdmissionRegistry>,
        global_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self::with_stream(
            ConnStream::tls(stream),
            peer_addr,
            state,
            auth_mode,
            admission_registry,
            global_permit,
        )
    }
}
