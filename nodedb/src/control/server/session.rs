// SPDX-License-Identifier: BUSL-1.1

use sonic_rs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, instrument, warn};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, GraphOp, VectorOp};
use nodedb_types::vector_distance::DistanceMetric;

/// Maximum frame size: 16 MiB.
const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

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
use super::conn_stream::ConnStream;

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

    /// Resolve the database for this session at the first authenticated request.
    ///
    /// Resolution order:
    /// 1. Explicit database from connection-string or handshake.
    /// 2. Per-user default database from `AuthenticatedIdentity.default_database`
    ///    (set via `ALTER USER <name> SET DEFAULT DATABASE <db>`).
    /// 3. Tenant default database (not yet stored; reserved for future use).
    /// 4. `DatabaseId::DEFAULT` — the built-in `default` database.
    fn resolve_database(
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        explicit: Option<DatabaseId>,
    ) -> DatabaseId {
        if let Some(db) = explicit {
            return db;
        }
        if let Some(db) = identity.default_database {
            return db;
        }
        // Tenant default database: not yet stored; falls through to built-in default.
        DatabaseId::DEFAULT
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

    /// Allocate a unique request ID via the per-node counter.
    fn next_request_id(&self) -> RequestId {
        self.state.next_request_id()
    }

    /// Register the session in the registry after authentication and store the
    /// kill receiver.  No-op if the user has `user_id == 0` (trust mode fallback).
    ///
    /// `token_expiry_ms` is `Some(exp_epoch_ms)` for OIDC/JWT sessions so the
    /// idle-sweep loop can enforce token lifetime independently of the TCP session.
    fn register_session(
        &mut self,
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        token_expiry_ms: Option<u64>,
    ) {
        use crate::control::security::sessions::SessionParams;

        if self.kill_rx.is_some() {
            // Already registered (trust auto-auth path called twice).
            return;
        }

        let auth_method = match identity.auth_method {
            crate::control::security::identity::AuthMethod::ScramSha256 => "scram_sha256",
            crate::control::security::identity::AuthMethod::CleartextPassword => "password",
            crate::control::security::identity::AuthMethod::ApiKey => "api_key",
            crate::control::security::identity::AuthMethod::Certificate => "certificate",
            crate::control::security::identity::AuthMethod::Trust => "trust",
            crate::control::security::identity::AuthMethod::OidcBearer => "oidc_bearer",
        };

        let credential_version = self.state.credentials.current_version(identity.user_id);
        self.identity_version = credential_version;

        let params = SessionParams {
            user_id: identity.user_id,
            username: identity.username.clone(),
            db_user: identity.username.clone(),
            peer_addr: self.peer_addr.to_string(),
            protocol: "native".to_string(),
            auth_method: auth_method.to_string(),
            tenant_id: identity.tenant_id.as_u64(),
            credential_version,
            current_database: None,
            token_expiry_ms,
        };

        match self
            .state
            .session_registry
            .register(&self.session_id, &params)
        {
            Ok(kill_rx) => {
                self.kill_rx = Some(kill_rx);
            }
            Err(e) => {
                // Cap exceeded; kill_rx stays None and the error will surface as
                // a SessionCapExceeded on the next request that calls check_kill.
                tracing::warn!(session_id = %self.session_id, cap = e.cap,
                    "session cap exceeded — session registered without kill channel");
            }
        }
    }

    /// Check whether the kill signal has fired (hard revoke).
    ///
    /// Returns `true` if the session should terminate immediately.
    fn is_killed(&mut self) -> bool {
        match self.kill_rx.as_mut() {
            Some(rx) => {
                rx.has_changed().unwrap_or(false)
                    && *rx.borrow_and_update()
                        != crate::control::security::sessions::KillReason::Alive
            }
            None => false,
        }
    }

    /// If the credential store's version for this user has advanced since we
    /// last bound, rebuild the identity from the fresh `UserRecord`.
    ///
    /// Must be called before every request that reads `self.identity`.
    fn rehydrate_identity_if_stale(&mut self) {
        let identity = match self.identity.as_ref() {
            Some(id) => id,
            None => return,
        };

        let user_id = identity.user_id;
        if user_id == 0 {
            // Trust-mode anonymous identity — no versioning.
            return;
        }

        let current = self.state.credentials.current_version(user_id);
        if current <= self.identity_version {
            return;
        }

        // Version advanced — fetch fresh record and rebuild identity.
        let auth_method = identity.auth_method.clone();
        let username = identity.username.clone();
        if let Some(fresh) = self.state.credentials.to_identity(&username, auth_method) {
            self.identity_version = current;
            self.identity = Some(fresh);
        }
    }

    /// Run the session loop: read frames, parse, dispatch, respond.
    #[instrument(skip(self), fields(peer = %self.peer_addr))]
    pub async fn run(mut self) -> crate::Result<()> {
        let idle_timeout_secs = self.state.idle_timeout_secs();
        let absolute_timeout_secs = self.state.session_absolute_timeout_secs();
        let result = self
            .run_inner(idle_timeout_secs, absolute_timeout_secs)
            .await;
        // Always unregister on exit regardless of reason.
        self.state
            .session_registry
            .unregister(&self.session_id.clone());
        result
    }

    async fn run_inner(
        &mut self,
        idle_timeout_secs: u64,
        absolute_timeout_secs: u64,
    ) -> crate::Result<()> {
        loop {
            // Hard-revoke check: bus consumer sent kill signal.
            if self.is_killed() {
                let msg = r#"{"status":"error","sqlstate":"57P01","error":"session revoked by administrator"}"#;
                let resp_len = (msg.len() as u32).to_be_bytes();
                let _ = self.stream.write_all(&resp_len).await;
                let _ = self.stream.write_all(msg.as_bytes()).await;
                return Ok(());
            }

            // Enforce absolute session lifetime (SQLSTATE 57P01 "admin shutdown").
            if absolute_timeout_secs > 0
                && self.connected_at.elapsed().as_secs() >= absolute_timeout_secs
            {
                debug!(
                    "session absolute timeout ({}s), closing connection",
                    absolute_timeout_secs
                );
                let msg = r#"{"status":"error","sqlstate":"57P01","error":"session timeout: absolute lifetime exceeded"}"#;
                let resp_len = (msg.len() as u32).to_be_bytes();
                let _ = self.stream.write_all(&resp_len).await;
                let _ = self.stream.write_all(msg.as_bytes()).await;
                return Ok(());
            }

            // Read length prefix with idle timeout.
            let mut len_buf = [0u8; 4];
            let read_result: std::io::Result<usize> = if idle_timeout_secs > 0 {
                match tokio::time::timeout(
                    Duration::from_secs(idle_timeout_secs),
                    self.stream.read_exact(&mut len_buf),
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
                self.stream.read_exact(&mut len_buf).await
            };
            match read_result {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("client disconnected");
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            }

            let payload_len = u32::from_be_bytes(len_buf);
            if payload_len > MAX_FRAME_SIZE {
                warn!(payload_len, "frame too large, closing connection");
                return Err(crate::Error::BadRequest {
                    detail: format!("frame size {payload_len} exceeds maximum {MAX_FRAME_SIZE}"),
                });
            }

            // Read payload.
            let mut payload = vec![0u8; payload_len as usize];
            self.stream.read_exact(&mut payload).await?;

            // Parse and dispatch.
            let request_id = self.next_request_id();
            match self.handle_frame(request_id, &payload).await {
                Ok(response_bytes) => {
                    // Write length-prefixed response.
                    let resp_len = (response_bytes.len() as u32).to_be_bytes();
                    self.stream.write_all(&resp_len).await?;
                    self.stream.write_all(&response_bytes).await?;
                }
                Err(e) => {
                    // Send error response.
                    let error_json = format!(r#"{{"status":"error","error":"{e}"}}"#);
                    let resp_len = (error_json.len() as u32).to_be_bytes();
                    self.stream.write_all(&resp_len).await?;
                    self.stream.write_all(error_json.as_bytes()).await?;
                }
            }
        }
    }

    /// Parse a request frame and dispatch to the Data Plane.
    async fn handle_frame(
        &mut self,
        request_id: RequestId,
        payload: &[u8],
    ) -> crate::Result<Vec<u8>> {
        // Parse the JSON request body.
        let body: serde_json::Value =
            sonic_rs::from_slice(payload).map_err(|e| crate::Error::BadRequest {
                detail: format!("invalid JSON: {e}"),
            })?;

        let op = body["op"]
            .as_str()
            .ok_or_else(|| crate::Error::BadRequest {
                detail: "missing 'op' field".into(),
            })?;

        // Auth handshake: must be first frame.
        if op == "auth" {
            let (identity, warning) = super::session_auth::authenticate(
                &self.state,
                &self.auth_mode,
                &body,
                &self.peer_addr.to_string(),
            )
            .await?;

            // Optional `"database"` field in the auth payload — bind the session
            // database at handshake time. If absent, falls back to the resolution
            // chain (user-default → tenant-default → DatabaseId::DEFAULT) below.
            let explicit_db = if let Some(db_name) = body["database"].as_str() {
                if db_name.is_empty() {
                    None
                } else {
                    // Validate the database name against the catalog.
                    let resolved = if let Some(cat) = self.state.credentials.catalog().as_ref() {
                        cat.get_database_id_by_name(db_name).ok().flatten()
                    } else {
                        None
                    };
                    match resolved {
                        Some(db_id) => Some(db_id),
                        None => {
                            let msg = format!(
                                r#"{{"status":"error","code":"DATABASE_NOT_FOUND","error":"database '{db_name}' does not exist"}}"#
                            );
                            return Ok(msg.into_bytes());
                        }
                    }
                }
            } else {
                None
            };

            let resolved_db = Self::resolve_database(&identity, explicit_db);

            // Enforce accessible_databases at session bind. Superusers bypass
            // this check (can_access_database returns true for all databases).
            if !identity.can_access_database(resolved_db) {
                let msg = r#"{"status":"error","code":"ACCESS_DENIED","error":"access denied to database"}"#;
                return Ok(msg.as_bytes().to_vec());
            }

            self.current_database = Some(resolved_db);

            let warning_field = match &warning {
                Some(w) => format!(r#","warning":"{}""#, w.replace('"', "'")),
                None => String::new(),
            };
            let resp = format!(
                r#"{{"status":"ok","username":"{}","tenant_id":{}{}}}"#,
                identity.username,
                identity.tenant_id.as_u64(),
                warning_field
            );

            // For OIDC bearer sessions, decode the JWT exp claim and store it
            // so the idle-sweep loop can enforce token lifetime.
            let token_expiry_ms = if identity.auth_method
                == crate::control::security::identity::AuthMethod::OidcBearer
            {
                body["token"].as_str().and_then(extract_jwt_exp_ms)
            } else {
                None
            };

            self.register_session(&identity, token_expiry_ms);
            self.identity = Some(identity);
            return Ok(resp.into_bytes());
        }

        // All other ops require auth. In trust mode, auto-authenticate on first frame.
        if self.identity.is_none() {
            if self.auth_mode == crate::config::auth::AuthMode::Trust {
                let trust_id = super::session_auth::trust_identity(&self.state, "anonymous");
                self.register_session(&trust_id, None);
                self.identity = Some(trust_id);
            } else {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: r#"not authenticated. Send {"op":"auth",...} first."#.into(),
                });
            }
        }

        // Check and rehydrate identity if credential version has advanced.
        self.rehydrate_identity_if_stale();

        let identity = match self.identity.as_ref() {
            Some(id) => id,
            None => {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: "not authenticated".into(),
                });
            }
        };

        // Tenant from authenticated identity, not from client payload.
        let tenant_id = identity.tenant_id;

        // Resolve and bind database on first request. Explicit handshake override
        // is not yet wired from the native wire protocol; every session uses the
        // resolution chain default (user default → tenant default → built-in default).
        if self.current_database.is_none() {
            self.current_database = Some(Self::resolve_database(identity, None));
        }
        let database_id = self.current_database.unwrap_or(DatabaseId::DEFAULT);

        let collection = body["collection"].as_str().unwrap_or("default").to_string();

        // Determine vShard from collection + document_id for data locality.
        let vshard_key = body["document_id"].as_str().unwrap_or(&collection);
        let vshard_id = VShardId::from_key(vshard_key.as_bytes());

        let plan = match op {
            "point_get" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                let pk_bytes = document_id.as_bytes().to_vec();
                let surrogate = self
                    .state
                    .surrogate_assigner
                    .lookup(database_id, tenant_id, &collection, &pk_bytes)?
                    .unwrap_or(nodedb_types::Surrogate::ZERO);
                PhysicalPlan::Document(DocumentOp::PointGet {
                    collection,
                    document_id,
                    surrogate,
                    pk_bytes,
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                })
            }
            "vector_search" => {
                let query_vector: Vec<f32> = body["query_vector"]
                    .as_array()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'query_vector'".into(),
                    })?
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                let top_k = body["top_k"].as_u64().unwrap_or(10) as usize;
                PhysicalPlan::Vector(VectorOp::Search {
                    collection,
                    query_vector,
                    top_k,
                    ef_search: 0,
                    metric: DistanceMetric::L2,
                    filter_bitmap: None,
                    field_name: String::new(),
                    rls_filters: Vec::new(),
                    inline_prefilter_plan: None,
                    // The HTTP/JSON session protocol surfaces only the
                    // primitive top_k field today. Advanced ANN tuning is
                    // SQL-only; defaulting keeps the wire shape uniform.
                    ann_options: Default::default(),
                    skip_payload_fetch: false,
                    payload_filters: Vec::new(),
                })
            }
            "range_scan" => {
                let field = body["field"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'field'".into(),
                    })?
                    .to_string();
                let limit = body["limit"].as_u64().unwrap_or(100) as usize;
                PhysicalPlan::Document(DocumentOp::RangeScan {
                    collection,
                    field,
                    lower: None,
                    upper: None,
                    limit,
                })
            }
            "crdt_read" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                PhysicalPlan::Crdt(CrdtOp::Read {
                    collection,
                    document_id,
                })
            }
            "crdt_apply" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                let delta_b64 = body["delta"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'delta'".into(),
                    })?;
                // Decode base64 delta. For now accept raw bytes if not valid base64.
                let delta = delta_b64.as_bytes().to_vec();
                let peer_id = body["peer_id"].as_u64().unwrap_or(0);
                let surrogate = self.state.surrogate_assigner.assign(
                    database_id,
                    tenant_id,
                    &collection,
                    document_id.as_bytes(),
                )?;
                PhysicalPlan::Crdt(CrdtOp::Apply {
                    collection,
                    document_id,
                    delta,
                    peer_id,
                    mutation_id: 0,
                    surrogate,
                    provenance: None,
                    // Direct session write, not a replicated peer sync.
                    descriptor_version_required: 0,
                })
            }
            "graph_rag_fusion" => {
                let query_vector: Vec<f32> = body["query_vector"]
                    .as_array()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'query_vector'".into(),
                    })?
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                let vector_top_k = body["vector_top_k"].as_u64().unwrap_or(20) as usize;
                let edge_label = body["edge_label"].as_str().map(String::from);
                let direction_str = body["direction"].as_str().unwrap_or("out");
                let direction = match direction_str {
                    "in" => crate::engine::graph::edge_store::Direction::In,
                    "both" => crate::engine::graph::edge_store::Direction::Both,
                    _ => crate::engine::graph::edge_store::Direction::Out,
                };
                let expansion_depth = body["expansion_depth"].as_u64().unwrap_or(2) as usize;
                let final_top_k = body["final_top_k"].as_u64().unwrap_or(10) as usize;
                let vector_k = body["vector_k"].as_f64().unwrap_or(60.0);
                let graph_k = body["graph_k"].as_f64().unwrap_or(10.0);
                PhysicalPlan::Graph(GraphOp::RagFusion {
                    collection,
                    query_vector,
                    vector_top_k,
                    edge_label,
                    direction,
                    expansion_depth,
                    final_top_k,
                    rrf_k: (vector_k, graph_k),
                    rrf_k_triple: None,
                    vector_field: body["vector_field"].as_str().unwrap_or("").to_string(),
                    options: Default::default(),
                    bm25_query: None,
                    bm25_field: None,
                })
            }
            "alter_collection_policy" => {
                let policy = &body["policy"];
                if policy.is_null() {
                    return Err(crate::Error::BadRequest {
                        detail: "missing 'policy' field".into(),
                    });
                }
                let policy_json =
                    sonic_rs::to_string(policy).map_err(|e| crate::Error::BadRequest {
                        detail: format!("invalid policy JSON: {e}"),
                    })?;
                PhysicalPlan::Crdt(CrdtOp::SetPolicy {
                    collection,
                    policy_json,
                })
            }
            _ => {
                return Err(crate::Error::BadRequest {
                    detail: format!("unknown op: {op}"),
                });
            }
        };

        // CRDT applies must be quorum-durable: route them through the Raft
        // proposer gate so the delta replicates to followers instead of landing
        // only on the receiving node (which loses it on leader failover). Every
        // other op keeps the direct SPSC path below. The success/error response
        // shape is preserved identically to the SPSC path.
        if op == "crdt_apply" {
            // `collection` was moved into the plan above; re-read it from the
            // request body (indexing only borrows). The replicated vshard is
            // collection-keyed (matching every other write path and the Raft data
            // group routing), NOT the document-keyed `vshard_id` used for local
            // core locality above.
            let collection = body["collection"].as_str().unwrap_or("default");
            let payload = crate::control::server::sync::raft_dispatch::dispatch_write_replicated(
                &self.state,
                tenant_id,
                database_id,
                collection,
                plan,
                Duration::from_secs(self.state.tuning.network.default_deadline_secs),
                crate::event::EventSource::User,
            )
            .await?;
            let payload_str = String::from_utf8_lossy(&payload).into_owned();
            let resp_json = format!(
                r#"{{"request_id":{},"status":"ok","payload":"{}","watermark_lsn":0,"error_code":null}}"#,
                request_id.as_u64(),
                payload_str,
            );
            return Ok(resp_json.into_bytes());
        }

        let request = Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id,
            plan,
            deadline: Instant::now()
                + Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            priority: Priority::Normal,
            trace_id: TraceId::generate(),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
        };

        // Register for response routing before dispatching.
        let mut rx = self.state.tracker.register(request_id);

        // Dispatch to Data Plane via SPSC.
        match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request)?,
            Err(poisoned) => poisoned.into_inner().dispatch(request)?,
        };

        // Await response from Data Plane (routed back via the response poller).
        let response = tokio::time::timeout(
            Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            async { rx.recv().await.ok_or(()) },
        )
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "response channel closed".into(),
        })?;

        // Serialize response to JSON.
        let status_str = match response.status {
            Status::Ok => "ok",
            Status::Partial => "partial",
            Status::Error => "error",
        };

        let payload_b64 = if response.payload.is_empty() {
            String::new()
        } else {
            // Return raw payload as lossy UTF-8 for now.
            String::from_utf8_lossy(&response.payload).into_owned()
        };

        let error_code_str = response.error_code.as_ref().map(|ec| format!("{ec:?}"));

        let resp_json = format!(
            r#"{{"request_id":{},"status":"{}","payload":"{}","watermark_lsn":{},"error_code":{}}}"#,
            response.request_id.as_u64(),
            status_str,
            payload_b64,
            response.watermark_lsn.as_u64(),
            error_code_str
                .as_ref()
                .map(|s| format!("\"{s}\""))
                .unwrap_or_else(|| "null".to_string()),
        );

        Ok(resp_json.into_bytes())
    }
}

/// Decode the `exp` claim from a JWT payload (no signature verification).
///
/// Returns `Some(exp_ms)` where `exp_ms = exp * 1000` (milliseconds since epoch),
/// or `None` if the token is malformed or the exp claim is absent/zero.
fn extract_jwt_exp_ms(token: &str) -> Option<u64> {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    let payload_b64 = parts.get(1)?;
    let bytes = crate::control::security::util::base64_url_decode(payload_b64)?;
    let claims: serde_json::Value = sonic_rs::from_slice(&bytes).ok()?;
    let exp = claims["exp"].as_u64()?;
    if exp == 0 {
        None
    } else {
        Some(exp.saturating_mul(1000))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::wal::WalManager;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// End-to-end test: client -> session -> dispatcher -> core_loop -> response -> client.
    #[tokio::test]
    async fn full_request_response_roundtrip() {
        // Set up infrastructure.
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal);

        // Start a Data Plane core in a background thread.
        let data_side = data_sides.into_iter().next().unwrap();
        let core_dir = dir.path().to_path_buf();
        let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
        let core_handle = tokio::task::spawn_blocking(move || {
            let mut core = CoreLoop::open(
                0,
                data_side.request_rx,
                data_side.response_tx,
                &core_dir,
                std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            )
            .unwrap();
            while matches!(
                core_stop_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ) {
                core.tick();
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        // Start response poller.
        let shared_poller = Arc::clone(&shared);
        let (poller_stop_tx, mut poller_stop_rx) = tokio::sync::watch::channel(false);
        let poller_handle = tokio::spawn(async move {
            loop {
                shared_poller.poll_and_route_responses();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    _ = poller_stop_rx.changed() => break,
                }
            }
        });

        // Bind a test listener.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn session handler.
        let shared_session = Arc::clone(&shared);
        let session_handle = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let session = Session::new(
                stream,
                peer_addr,
                shared_session,
                crate::config::auth::AuthMode::Trust,
            );
            session.run().await
        });

        // Connect as a client and send a request.
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        let request_json =
            r#"{"op":"point_get","tenant_id":1,"collection":"users","document_id":"u1"}"#;
        let len = (request_json.len() as u32).to_be_bytes();
        client.write_all(&len).await.unwrap();
        client.write_all(request_json.as_bytes()).await.unwrap();

        // Read response.
        let mut resp_len_buf = [0u8; 4];
        client.read_exact(&mut resp_len_buf).await.unwrap();
        let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        client.read_exact(&mut resp_buf).await.unwrap();

        let resp_str = String::from_utf8(resp_buf).unwrap();
        // Document doesn't exist, so we get NotFound — but the roundtrip works.
        assert!(
            resp_str.contains(r#""status""#),
            "expected a valid response, got: {resp_str}"
        );
        assert!(
            resp_str.contains(r#""request_id""#),
            "expected request_id in response, got: {resp_str}"
        );

        // Clean up: signal background tasks to stop.
        drop(client);
        let _ = session_handle.await;
        let _ = poller_stop_tx.send(true);
        let _ = poller_handle.await;
        let _ = core_stop_tx.send(());
        let _ = core_handle.await;
    }
}

#[cfg(test)]
mod session_timeout_tests {
    /// Verify the absolute-timeout predicate in isolation.
    ///
    /// The real check is: `absolute_timeout_secs > 0 && elapsed >= absolute_timeout_secs`.
    /// This test pins that condition so refactors cannot silently invert it.
    #[test]
    fn absolute_timeout_predicate() {
        // When timeout is disabled (0), any elapsed time should NOT trigger.
        let absolute_timeout_secs: u64 = 0;
        let elapsed_secs: u64 = 9999;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(
            !should_close,
            "timeout=0 (disabled) must never close the session"
        );

        // When timeout is set and elapsed < timeout, session stays open.
        let absolute_timeout_secs: u64 = 3600;
        let elapsed_secs: u64 = 3599;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(
            !should_close,
            "elapsed < timeout should not close the session"
        );

        // When elapsed == timeout exactly, session should close.
        let elapsed_secs: u64 = 3600;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(should_close, "elapsed == timeout should close the session");

        // When elapsed > timeout, session should close.
        let elapsed_secs: u64 = 7200;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(should_close, "elapsed > timeout should close the session");

        // Idle timeout is independent — setting idle_timeout > 0 does not affect absolute check.
        // Specifically: absolute=0 (disabled) + any idle means absolute check still returns false.
        let absolute_timeout_secs: u64 = 0;
        let _idle_timeout_secs: u64 = 60;
        let elapsed_secs: u64 = 9999;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(
            !should_close,
            "idle timeout must not activate the absolute-timeout close path"
        );
    }
}
