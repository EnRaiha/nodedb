// SPDX-License-Identifier: BUSL-1.1

//! Native protocol session: the run loop that reads frames, routes
//! by opcode, and writes responses.
//!
//! Replaces the legacy JSON-only `Session` with auto-detection of
//! JSON vs MessagePack and full SQL/DDL/transaction support.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tracing::{debug, instrument};

use nodedb_types::protocol::{MAX_FRAME_SIZE, NativeResponse, OpCode, RequestFields};

use tokio::sync::OwnedSemaphorePermit;

use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::admission::{AdmissionRegistry, ConnectionPermit};
use crate::control::server::conn_stream::ConnStream;
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;

use super::codec::{self, FrameFormat};
use super::dispatch::{self, DispatchCtx};
use session_chunk::chunk_large_response;

#[path = "session_chunk.rs"]
mod session_chunk;

#[path = "session_stream.rs"]
mod session_stream;

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

    /// Run the session loop: read frames, route by opcode, write responses.
    #[instrument(skip(self), fields(peer = %self.peer_addr))]
    pub async fn run(mut self) -> crate::Result<()> {
        // Perform the version-negotiation handshake before any frame exchange.
        let limits = self.state.limits.clone();
        self.proto_ver =
            super::handshake::perform_server_handshake(&mut self.stream, &limits).await?;

        let idle_timeout_secs = self.state.idle_timeout_secs();
        let absolute_timeout_secs = self.state.session_absolute_timeout_secs();

        loop {
            // Enforce absolute session lifetime (SQLSTATE 57P01 "admin shutdown").
            if absolute_timeout_secs > 0
                && self.connected_at.elapsed().as_secs() >= absolute_timeout_secs
            {
                debug!(
                    "session absolute timeout ({}s), closing connection",
                    absolute_timeout_secs
                );
                let shutdown_resp = NativeResponse::error(
                    0,
                    "57P01",
                    "session timeout: absolute lifetime exceeded",
                );
                if let Ok(bytes) = super::codec::encode_response(
                    &shutdown_resp,
                    self.format.unwrap_or(FrameFormat::MessagePack),
                ) {
                    let _ = super::codec::write_frame(&mut self.stream, &bytes).await;
                }
                return Ok(());
            }

            // Read a frame with idle timeout.
            let frame_result = if idle_timeout_secs > 0 {
                match tokio::time::timeout(
                    Duration::from_secs(idle_timeout_secs),
                    codec::read_frame(&mut self.stream),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        debug!("session idle timeout ({}s)", idle_timeout_secs);
                        return Ok(());
                    }
                }
            } else {
                codec::read_frame(&mut self.stream).await
            };

            let payload = match frame_result {
                Ok(Some(p)) => p,
                Ok(None) => return Ok(()), // clean EOF
                Err(crate::Error::BadRequest { detail }) => {
                    // Send a typed error before closing so the client knows why.
                    let err_resp =
                        NativeResponse::error(0, "54000", format!("frame rejected: {detail}"));
                    let format = self.format.unwrap_or(FrameFormat::MessagePack);
                    if let Ok(bytes) = codec::encode_response(&err_resp, format) {
                        let _ = codec::write_frame(&mut self.stream, &bytes).await;
                    }
                    return Ok(());
                }
                Err(e) => return Err(e),
            };

            // Auto-detect format on first frame.
            if self.format.is_none() {
                self.format = Some(FrameFormat::detect(payload[0]));
            }
            let Some(format) = self.format else {
                return Err(crate::Error::BadRequest {
                    detail: "format detection failed after first frame".into(),
                });
            };

            // Decode and handle.
            let outcome = match codec::decode_request(&payload, format) {
                Ok(req) => self.handle_request(req).await,
                Err(e) => dispatch::SqlOutcome::Response(Box::new(NativeResponse::error(
                    0,
                    "42601",
                    format!("{e}"),
                ))),
            };

            match outcome {
                dispatch::SqlOutcome::Response(response) => {
                    // Encode and write response — chunk if it exceeds frame limit.
                    let resp_bytes = codec::encode_response(&response, format)?;
                    if resp_bytes.len() <= MAX_FRAME_SIZE as usize {
                        codec::write_frame(&mut self.stream, &resp_bytes).await?;
                    } else {
                        // Response too large for a single frame — split rows.
                        let frames = chunk_large_response(*response, format)?;
                        for frame in &frames {
                            codec::write_frame(&mut self.stream, frame).await?;
                        }
                    }
                }
                dispatch::SqlOutcome::Stream(sql_stream) => {
                    session_stream::emit_sql_stream(&mut self.stream, sql_stream, format).await?;
                }
            }
        }
    }

    /// Route a decoded request to the appropriate handler.
    ///
    /// Returns a [`SqlOutcome`]: every op produces a materialized
    /// `SqlOutcome::Response` except an eligible streamable SELECT on the
    /// `Sql`/`Ddl` path, which yields `SqlOutcome::Stream` for the run loop to
    /// emit as multiple frames.
    async fn handle_request(
        &mut self,
        req: nodedb_types::protocol::NativeRequest,
    ) -> dispatch::SqlOutcome {
        use dispatch::SqlOutcome;
        let seq = req.seq;
        let op = req.op;

        // Auth handling.
        if op == OpCode::Auth {
            return SqlOutcome::Response(Box::new(self.handle_auth(seq, &req.fields).await));
        }

        // Ping requires no auth.
        if op == OpCode::Ping {
            return SqlOutcome::Response(Box::new(dispatch::handle_ping(seq)));
        }

        // Status requires no auth — returns current startup phase.
        if op == OpCode::Status {
            let health = crate::control::startup::health::observe(&self.state.startup);
            let native_status = crate::control::startup::health::to_native_status(&health);
            return SqlOutcome::Response(Box::new(NativeResponse::status_row(
                seq,
                native_status.to_string(),
            )));
        }

        // All other ops require authentication.
        if self.identity.is_none() {
            if self.auth_mode == AuthMode::Trust {
                let trust_id = super::super::session_auth::trust_identity(&self.state, "anonymous");
                self.auth_context = Some(super::super::session_auth::build_auth_context(&trust_id));
                self.identity = Some(trust_id);
            } else {
                return SqlOutcome::Response(Box::new(NativeResponse::error(
                    seq,
                    "28000",
                    "not authenticated. Send Auth request first.",
                )));
            }
        }

        let identity = match self.identity.as_ref() {
            Some(id) => id,
            None => {
                return SqlOutcome::Response(Box::new(NativeResponse::error(
                    seq,
                    "28000",
                    "not authenticated",
                )));
            }
        };

        // Build a default AuthContext if not yet set (shouldn't happen but be safe).
        let default_auth_ctx;
        let auth_ctx = match self.auth_context.as_ref() {
            Some(ctx) => ctx,
            None => {
                default_auth_ctx = super::super::session_auth::build_auth_context(identity);
                &default_auth_ctx
            }
        };

        let ctx = DispatchCtx {
            state: &self.state,
            identity,
            auth_context: auth_ctx,
            query_ctx: &self.query_ctx,
            sessions: &self.sessions,
            peer_addr: &self.peer_addr,
        };

        let fields = match &req.fields {
            RequestFields::Text(f) => f,
            _ => {
                return SqlOutcome::Response(Box::new(NativeResponse::error(
                    seq,
                    "0A000",
                    "unsupported request field format for this server version",
                )));
            }
        };

        // SQL / DDL is the only path that can stream — handle it before the
        // materialized `match op` below so its `SqlOutcome` flows up unchanged.
        if matches!(op, OpCode::Sql | OpCode::Ddl) {
            let sql = match &fields.sql {
                Some(s) => s.as_str(),
                None => {
                    return SqlOutcome::Response(Box::new(NativeResponse::error(
                        seq,
                        "42601",
                        "missing 'sql' field",
                    )));
                }
            };
            return dispatch::handle_sql_streaming(&ctx, seq, sql, fields.sql_params.as_deref())
                .await;
        }

        let response = match op {
            // SQL handled above (streaming-capable).
            OpCode::Sql | OpCode::Ddl => unreachable!("SQL/DDL handled before this match"),

            // Session parameters.
            OpCode::Set => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => {
                        // Also support SET via sql field: "SET key = value"
                        if let Some(sql) = &fields.sql {
                            return SqlOutcome::Response(Box::new(
                                dispatch::handle_sql(&ctx, seq, sql, None).await,
                            ));
                        }
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'key' field",
                        )));
                    }
                };
                let value = fields.value.as_deref().unwrap_or("");
                dispatch::handle_set(&ctx, seq, key, value)
            }
            OpCode::Show => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => {
                        if let Some(sql) = &fields.sql {
                            return SqlOutcome::Response(Box::new(
                                dispatch::handle_sql(&ctx, seq, sql, None).await,
                            ));
                        }
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'key' field",
                        )));
                    }
                };
                dispatch::handle_show(&ctx, seq, key)
            }
            OpCode::Reset => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => {
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'key' field",
                        )));
                    }
                };
                dispatch::handle_reset(&ctx, seq, key)
            }

            // Transaction control.
            OpCode::Begin => dispatch::handle_begin(&ctx, seq),
            OpCode::Commit => dispatch::handle_commit(&ctx, seq).await,
            OpCode::Rollback => dispatch::handle_rollback(&ctx, seq).await,

            // Explain.
            OpCode::Explain => {
                let sql = match &fields.sql {
                    Some(s) => s.as_str(),
                    None => {
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'sql' field",
                        )));
                    }
                };
                dispatch::handle_sql(&ctx, seq, &format!("EXPLAIN {sql}"), None).await
            }

            // Direct Data Plane operations.
            OpCode::PointGet
            | OpCode::PointPut
            | OpCode::PointDelete
            | OpCode::VectorSearch
            | OpCode::RangeScan
            | OpCode::CrdtRead
            | OpCode::CrdtApply
            | OpCode::GraphRagFusion
            | OpCode::AlterCollectionPolicy
            | OpCode::GraphHop
            | OpCode::GraphNeighbors
            | OpCode::GraphPath
            | OpCode::GraphSubgraph
            | OpCode::EdgePut
            | OpCode::EdgeDelete
            | OpCode::TextSearch
            | OpCode::HybridSearch
            | OpCode::SpatialScan
            | OpCode::TimeseriesScan
            | OpCode::TimeseriesIngest
            | OpCode::KvScan
            | OpCode::KvExpire
            | OpCode::KvPersist
            | OpCode::KvGetTtl
            | OpCode::KvBatchGet
            | OpCode::KvBatchPut
            | OpCode::KvFieldGet
            | OpCode::KvFieldSet
            | OpCode::DocumentUpdate
            | OpCode::DocumentScan
            | OpCode::DocumentUpsert
            | OpCode::DocumentBulkUpdate
            | OpCode::DocumentBulkDelete
            | OpCode::VectorInsert
            | OpCode::VectorMultiSearch
            | OpCode::VectorDelete
            | OpCode::GraphAlgo
            | OpCode::ColumnarScan
            | OpCode::ColumnarInsert
            | OpCode::RecursiveScan
            | OpCode::DocumentTruncate
            | OpCode::DocumentEstimateCount
            | OpCode::DocumentInsertSelect
            | OpCode::DocumentRegister
            | OpCode::DocumentDropIndex
            | OpCode::KvRegisterIndex
            | OpCode::KvDropIndex
            | OpCode::KvTruncate
            | OpCode::VectorSetParams
            | OpCode::KvIncr
            | OpCode::KvIncrFloat
            | OpCode::KvCas
            | OpCode::KvGetSet
            | OpCode::KvRegisterSortedIndex
            | OpCode::KvDropSortedIndex
            | OpCode::KvSortedIndexRank
            | OpCode::KvSortedIndexTopK
            | OpCode::KvSortedIndexRange
            | OpCode::KvSortedIndexCount
            | OpCode::KvSortedIndexScore => dispatch::handle_direct_op(&ctx, seq, op, fields).await,

            // MATCH: dedicated path that unwraps the DP `{rows, frontier}`
            // envelope into the bare rows array the native row decoder expects.
            OpCode::GraphMatch => dispatch::handle_graph_match(&ctx, seq, fields).await,

            // Batch ops: direct Data Plane dispatch.
            OpCode::VectorBatchInsert | OpCode::DocumentBatchInsert => {
                dispatch::handle_direct_op(&ctx, seq, op, fields).await
            }

            // Copy from file.
            OpCode::CopyFrom => {
                let sql = match &fields.sql {
                    Some(s) => s.as_str(),
                    None => {
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'sql' field",
                        )));
                    }
                };
                dispatch::handle_sql(&ctx, seq, sql, None).await
            }

            // Auth/Ping/Status handled above.
            OpCode::Auth | OpCode::Ping | OpCode::Status => unreachable!(),
            // OpCode is #[non_exhaustive]; future opcodes that reach this
            // handler before session.rs is updated return a typed error.
            _ => NativeResponse::error(seq, "0A000", "opcode not supported by this server version"),
        };

        SqlOutcome::Response(Box::new(response))
    }

    /// Handle authentication request.
    async fn handle_auth(&mut self, seq: u64, fields: &RequestFields) -> NativeResponse {
        // Re-authentication is not supported on the native protocol. Once a
        // session has assembled its three-level admission permit, the identity
        // is fixed for the connection's lifetime — allowing re-auth would let
        // a client silently swap to a different (database, tenant) scope while
        // still holding the original scope's connection slots.
        if self.identity.is_some() || self.connection_permit.is_some() {
            return NativeResponse::error(
                seq,
                "0A000",
                "already authenticated; reconnect to switch identity",
            );
        }

        let auth = match fields {
            RequestFields::Text(f) => match &f.auth {
                Some(a) => a,
                None => {
                    return NativeResponse::error(seq, "28000", "missing 'auth' field");
                }
            },
            _ => {
                return NativeResponse::error(seq, "0A000", "unsupported request fields variant");
            }
        };

        match dispatch::handle_auth(
            &self.state,
            &self.auth_mode,
            auth,
            &self.peer_addr.to_string(),
        )
        .await
        {
            Ok((identity, warning)) => {
                // Phase 2 admission: acquire per-database and per-tenant permits
                // now that we know the identity. The database scope is the
                // identity's default database (or DEFAULT if none is set).
                let db_id = identity
                    .default_database
                    .unwrap_or(nodedb_types::DatabaseId::DEFAULT);
                let tenant_id = identity.tenant_id;

                let db_permit = match self.admission_registry.try_acquire_database(db_id) {
                    Ok(p) => p,
                    Err(e) => {
                        return NativeResponse::error(
                            seq,
                            nodedb_types::error::sqlstate::QUOTA_EXCEEDED,
                            format!("{e}"),
                        );
                    }
                };
                let tenant_permit =
                    match self.admission_registry.try_acquire_tenant(db_id, tenant_id) {
                        Ok(p) => p,
                        Err(e) => {
                            // db_permit is dropped here, releasing the DB slot.
                            drop(db_permit);
                            return NativeResponse::error(
                                seq,
                                nodedb_types::error::sqlstate::QUOTA_EXCEEDED,
                                format!("{e}"),
                            );
                        }
                    };

                // Assemble the three-level permit. The global slot moves from
                // `global_permit` into the `ConnectionPermit`. The re-auth
                // guard at the top of this function ensures `global_permit`
                // is still `Some` here — it is initialized at construction
                // and only consumed on the auth path.
                let Some(global) = self.global_permit.take() else {
                    // Release the freshly acquired Phase 2 permits so we
                    // don't leak slots into the per-DB / per-tenant pools.
                    drop(tenant_permit);
                    drop(db_permit);
                    return NativeResponse::error(
                        seq,
                        "XX000",
                        "internal error: global admission permit missing during auth assembly",
                    );
                };
                self.connection_permit = Some(ConnectionPermit {
                    global,
                    database: db_permit,
                    tenant: tenant_permit,
                    db_id,
                    tenant_id,
                });

                let mut resp = NativeResponse::auth_ok(
                    seq,
                    identity.username.clone(),
                    identity.tenant_id.as_u64(),
                );
                if let Some(w) = warning {
                    resp.warnings.push(w);
                }
                self.auth_context = Some(super::super::session_auth::build_auth_context(&identity));
                self.identity = Some(identity);
                resp
            }
            // A transient login rate-limit is distinct from a credential
            // failure: it maps to TOO_MANY_CONNECTIONS (53300), which clients
            // recognise as retryable, and carries a distinct message. Every
            // other auth error (wrong password, lockout, unknown user) stays
            // collapsed into the generic invalid-password 28P01 so none can be
            // distinguished from the others.
            Err(e @ crate::Error::RateExceeded { .. }) => NativeResponse::error(
                seq,
                nodedb_types::error::sqlstate::TOO_MANY_CONNECTIONS,
                format!("{e}"),
            ),
            Err(e) => NativeResponse::error(seq, "28P01", format!("{e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;
    use nodedb_types::protocol::opcodes::ResponseStatus;

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
}
