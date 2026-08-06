// SPDX-License-Identifier: BUSL-1.1

//! ILP (InfluxDB Line Protocol) TCP listener for timeseries ingest.
//!
//! Accepts plain TCP connections on the configured port. Each connection
//! reads newline-delimited ILP lines, parses them, and dispatches
//! `TimeseriesIngest` plans to the Data Plane via SPSC.
//!
//! Protocol: native Hello/Auth prelude followed by one ILP line per newline.
//! The prelude is mandatory; direct unauthenticated ILP clients are rejected.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::BufReader;

/// Maximum byte length of a single ILP line. Lines exceeding this are
/// rejected and the connection is dropped to prevent memory exhaustion.
const MAX_ILP_LINE_BYTES: usize = 10 * 1024 * 1024; // 10 MiB
/// Per-connection aggregate cap. A batch must not turn many individually
/// valid lines into an unbounded allocation before its timer/line flush.
const MAX_ILP_BATCH_BYTES: usize = MAX_ILP_LINE_BYTES;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use crate::config::auth::AuthMode;
use crate::control::server::conn_stream::ConnStream;
use crate::control::server::ilp_auth::AuthenticatedIlpContext;
use crate::control::server::shared::{ConnectionFutureOutcome, isolate_connection_future};
use crate::control::state::SharedState;
use crate::types::TenantId;

#[path = "ilp_batch/mod.rs"]
mod ilp_batch;
#[path = "ilp_drop.rs"]
mod ilp_drop;
#[path = "ilp_line_read.rs"]
mod ilp_line_read;
pub(crate) use ilp_batch::flush_authenticated_ilp_batch;
use ilp_batch::{IlpRateEstimator, flush_ilp_batch};
use ilp_drop::{IlpDropCause, terminate_with_buffered_flush};
use ilp_line_read::read_bounded_ilp_line;

/// ILP TCP listener.
pub struct IlpListener {
    tcp: TcpListener,
    addr: SocketAddr,
}

impl IlpListener {
    /// Bind to the given address.
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        let tcp = TcpListener::bind(addr).await.map_err(crate::Error::Io)?;
        let local_addr = tcp.local_addr().map_err(crate::Error::Io)?;
        info!(%local_addr, "ILP TCP listener bound");
        Ok(Self {
            tcp,
            addr: local_addr,
        })
    }

    /// Returns the local address the listener is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// Run the accept loop until shutdown.
    pub async fn run(
        self,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        conn_semaphore: Arc<Semaphore>,
        tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
        startup_gate: Arc<crate::control::startup::StartupGate>,
        bus: crate::control::shutdown::ShutdownBus,
    ) -> crate::Result<()> {
        // This JoinSet owns active ILP connection tasks until their graceful
        // drain (or forced abort) completes, so it gates later shutdown phases.
        let drain_guard = bus.register_critical_task(
            crate::control::shutdown::ShutdownPhase::DrainingListeners,
            "ilp",
        );
        let mut shutdown_handle = bus.handle();

        let tls_label = if tls_acceptor.is_some() {
            "tls"
        } else {
            "plain"
        };
        info!(addr = %self.addr, tls = tls_label, "ILP listener bound — waiting for GatewayEnable");

        startup_gate
            .await_phase(crate::control::startup::StartupPhase::GatewayEnable)
            .await
            .map_err(crate::Error::from)?;

        info!(addr = %self.addr, tls = tls_label, "ILP listener accepting connections");

        let mut connections = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            let permit = match conn_semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    debug!(%peer, "ILP connection rejected: max connections");
                                    continue;
                                }
                            };
                            let state = Arc::clone(&state);
                            let auth_mode = auth_mode.clone();

                            if let Some(ref acceptor) = tls_acceptor {
                                let acceptor = acceptor.clone();
                                connections.spawn(async move {
                                    let outcome = isolate_connection_future(async move {
                                        let _permit = permit;
                                        match tokio::time::timeout(
                                            std::time::Duration::from_secs(10),
                                            acceptor.accept(stream),
                                        )
                                        .await
                                        {
                                            Ok(Ok(tls_stream)) => {
                                                let cs = ConnStream::tls(tls_stream);
                                                if let Err(e) = handle_ilp_connection(cs, peer, &state, &auth_mode).await {
                                                    warn!(%peer, error = %e, "ILP TLS connection error (data may be lost)");
                                                }
                                            }
                                            Ok(Err(e)) => {
                                                warn!(%peer, error = %e, "ILP TLS handshake failed");
                                            }
                                            Err(_) => {
                                                warn!(%peer, "ILP TLS handshake timed out");
                                            }
                                        }
                                        peer
                                    })
                                    .await;
                                    if matches!(outcome, ConnectionFutureOutcome::Panicked) {
                                        warn!(%peer, "ILP TLS connection panicked");
                                    }
                                    peer
                                });
                            } else {
                                connections.spawn(async move {
                                    let outcome = isolate_connection_future(async move {
                                        let _permit = permit;
                                        let cs = ConnStream::plain(stream);
                                        if let Err(e) = handle_ilp_connection(cs, peer, &state, &auth_mode).await {
                                            warn!(%peer, error = %e, "ILP connection error (data may be lost)");
                                        }
                                        peer
                                    })
                                    .await;
                                    if matches!(outcome, ConnectionFutureOutcome::Panicked) {
                                        warn!(%peer, "ILP connection panicked");
                                    }
                                    peer
                                });
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "ILP accept error");
                        }
                    }
                }
                result = connections.join_next(), if !connections.is_empty() => {
                    if matches!(result, Some(Err(_))) {
                        warn!("ILP connection task ended unexpectedly");
                    }
                }
                _ = shutdown_handle.await_phase(crate::control::shutdown::ShutdownPhase::DrainingListeners) => {
                    info!(addr = %self.addr, "ILP listener shutting down");
                    break;
                }
            }
        }

        // Drain remaining connections with timeout.
        let drain = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while connections.join_next().await.is_some() {}
        });
        if drain.await.is_err() {
            warn!(addr = %self.addr, "ILP connection drain timed out; aborting remaining tasks");
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
        drain_guard.report_drained();
        Ok(())
    }
}

/// Handle a single ILP TCP connection with adaptive batch coalescing.
///
/// Batch size adapts to ingest rate:
/// - High rate (>100K lines/s): batch up to 10K lines or 10ms window
/// - Medium rate (1K-100K/s): batch up to 1K lines or 50ms window
/// - Low rate (<1K/s): batch per 100 lines or 100ms window
///
/// Larger batches amortize per-batch overhead (WAL append, memtable lock,
/// partition lookup).
async fn handle_ilp_connection(
    mut stream: ConnStream,
    peer: SocketAddr,
    state: &Arc<SharedState>,
    auth_mode: &AuthMode,
) -> crate::Result<()> {
    // Captured before the Hello/Auth prelude borrows the stream and the
    // ingest loop moves it into a `BufReader`, after which the TLS session is
    // no longer reachable.
    let transport = stream.transport_security();

    // The native Hello/Auth prelude must finish before line parsing, tenant
    // accounting, or any ingest side effect. Authentication failures consume
    // no ILP bytes and the dropped stream cannot enter the ingest loop.
    let authenticated_context = crate::control::server::ilp_auth::authenticate_ilp_connection(
        &mut stream,
        state,
        auth_mode,
        &peer.to_string(),
    )
    .await
    .map_err(|_| crate::Error::BadRequest {
        detail: "ILP authentication failed".into(),
    })?;
    // The TLS policy is evaluated before any ingest capacity is acquired: the
    // identity (and with it the superuser flag the cleartext carve-out needs)
    // exists only now, and a refused connection must not hold a slot.
    if crate::control::server::session_auth::check_transport_security(
        state,
        authenticated_context.identity(),
        transport,
        authenticated_context.peer_addr(),
    )
    .is_err()
    {
        crate::control::server::ilp_auth::write_ilp_auth_failure(
            &mut stream,
            &authenticated_context,
        )
        .await;
        return Err(crate::Error::BadRequest {
            detail: "ILP authentication failed".into(),
        });
    }

    let _admission = match IlpConnectionAdmission::acquire(state, &authenticated_context) {
        Ok(admission) => admission,
        Err(_) => {
            crate::control::server::ilp_auth::write_ilp_auth_failure(
                &mut stream,
                &authenticated_context,
            )
            .await;
            return Err(crate::Error::BadRequest {
                detail: "ILP authentication failed".into(),
            });
        }
    };
    crate::control::server::ilp_auth::write_ilp_auth_success(&mut stream, &authenticated_context)
        .await
        .map_err(|_| crate::Error::BadRequest {
            detail: "ILP authentication failed".into(),
        })?;

    debug!(%peer, "authenticated ILP connection accepted");

    let mut reader = BufReader::new(stream);
    let mut line_buf: Vec<u8> = Vec::with_capacity(4096);
    let mut batch = String::new();
    let mut line_count = 0u64;
    let mut total_ingested = 0u64;

    // Adaptive batch coalescing state.
    let mut rate_estimator = IlpRateEstimator::new();
    let mut batch_target = 1000u64;
    let mut window = tokio::time::interval(std::time::Duration::from_millis(50));
    window.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // Read next line with an enforced byte-length cap.
            result = read_bounded_ilp_line(&mut reader, &mut line_buf, MAX_ILP_LINE_BYTES) => {
                match result {
                    Ok(false) => break, // Connection closed (EOF).
                    Ok(true) => {
                        // Strip trailing newline / CRLF.
                        let line_bytes = line_buf
                            .strip_suffix(b"\r\n")
                            .or_else(|| line_buf.strip_suffix(b"\n"))
                            .unwrap_or(&line_buf);

                        let line = match std::str::from_utf8(line_bytes) {
                            Ok(s) => s,
                            // The framing is broken and cannot be resynchronized, so the
                            // connection still ends here — but the lines already accepted
                            // into `batch` are dispatched first and the termination is
                            // recorded, because ILP acks nothing and the client would
                            // otherwise have no way to learn they were discarded.
                            Err(_) => {
                                return Err(terminate_with_buffered_flush(
                                    state,
                                    &authenticated_context,
                                    peer,
                                    IlpDropCause::InvalidUtf8,
                                    &batch,
                                    line_count,
                                )
                                .await);
                            }
                        };

                        if line.trim().is_empty() || line.trim_start().starts_with('#') {
                            line_buf.clear();
                            continue;
                        }

                        let separator_bytes = if batch.is_empty() { 0 } else { 1 };
                        if batch
                            .len()
                            .saturating_add(separator_bytes)
                            .saturating_add(line.len())
                            > MAX_ILP_BATCH_BYTES
                        {
                            let flushed = line_count;
                            total_ingested +=
                                flush_ilp_batch(state, &authenticated_context, &batch).await?;
                            batch.clear();
                            line_count = 0;

                            rate_estimator.record(flushed);
                            let (new_target, new_window_ms) = rate_estimator.suggest_batch_params();
                            batch_target = new_target;
                            window = tokio::time::interval(
                                std::time::Duration::from_millis(new_window_ms),
                            );
                            window.set_missed_tick_behavior(
                                tokio::time::MissedTickBehavior::Delay,
                            );
                        }

                        if !batch.is_empty() {
                            batch.push('\n');
                        }
                        batch.push_str(line);
                        line_count += 1;
                        line_buf.clear();

                        // Flush when batch reaches adaptive target.
                        if line_count >= batch_target {
                            let flushed = line_count;
                            total_ingested +=
                                flush_ilp_batch(state, &authenticated_context, &batch).await?;
                            batch.clear();
                            line_count = 0;

                            // Update rate estimator and recalculate batch target.
                            rate_estimator.record(flushed);
                            let (new_target, new_window_ms) = rate_estimator.suggest_batch_params();
                            batch_target = new_target;
                            window = tokio::time::interval(
                                std::time::Duration::from_millis(new_window_ms),
                            );
                            window.set_missed_tick_behavior(
                                tokio::time::MissedTickBehavior::Delay,
                            );
                        }
                    }
                    Err(error) => {
                        warn!(
                            %peer,
                            error = %error,
                            limit = MAX_ILP_LINE_BYTES,
                            "ILP line read failed — rejecting connection"
                        );
                        // Same contract as the decode failure above: the connection
                        // ends, but not before the accepted lines are dispatched and
                        // the loss surface is recorded.
                        return Err(terminate_with_buffered_flush(
                            state,
                            &authenticated_context,
                            peer,
                            IlpDropCause::LineReadFailed,
                            &batch,
                            line_count,
                        )
                        .await);
                    }
                }
            }
            // Timer-based flush (for low-rate connections).
            _ = window.tick() => {
                if !batch.is_empty() {
                    let flushed = line_count;
                    total_ingested +=
                        flush_ilp_batch(state, &authenticated_context, &batch).await?;
                    batch.clear();
                    line_count = 0;

                    rate_estimator.record(flushed);
                    let (new_target, new_window_ms) = rate_estimator.suggest_batch_params();
                    batch_target = new_target;
                    window = tokio::time::interval(
                        std::time::Duration::from_millis(new_window_ms),
                    );
                    window.set_missed_tick_behavior(
                        tokio::time::MissedTickBehavior::Delay,
                    );
                }
            }
        }
    }

    // Flush remaining.
    if !batch.is_empty() {
        total_ingested += flush_ilp_batch(state, &authenticated_context, &batch).await?;
    }

    debug!(
        %peer,
        total_ingested,
        database_id = ?authenticated_context.database_id(),
        "ILP connection closed"
    );
    Ok(())
}

/// Connection-scoped tenant accounting and quota permits.
///
/// The permit fields release configured database/tenant limits on every return
/// path, while `Drop` balances the legacy tenant activity accounting.
struct IlpConnectionAdmission<'a> {
    state: &'a SharedState,
    tenant_id: TenantId,
    _database_permit: Option<OwnedSemaphorePermit>,
    _tenant_permit: Option<OwnedSemaphorePermit>,
}

impl<'a> IlpConnectionAdmission<'a> {
    fn acquire(state: &'a SharedState, context: &AuthenticatedIlpContext) -> crate::Result<Self> {
        let tenant_id = context.identity().tenant_id;
        let database_id = context.database_id();

        let database_permit = state
            .admission_registry
            .try_acquire_database(database_id)
            .map_err(|_| crate::Error::BadRequest {
                detail: "ILP admission denied".into(),
            })?;
        let tenant_permit = state
            .admission_registry
            .try_acquire_tenant(database_id, tenant_id)
            .map_err(|_| crate::Error::BadRequest {
                detail: "ILP admission denied".into(),
            })?;

        start_tenant_connection(state, tenant_id)?;
        Ok(Self {
            state,
            tenant_id,
            _database_permit: database_permit,
            _tenant_permit: tenant_permit,
        })
    }
}

impl Drop for IlpConnectionAdmission<'_> {
    fn drop(&mut self) {
        self.state.tenant_connection_end(self.tenant_id);
    }
}

/// Atomically check and account for the legacy per-tenant connection cap.
///
/// The database/tenant semaphore permits above cover configured catalog
/// quotas; this lock also protects the legacy tenant-isolation counter from a
/// check-then-increment race when it has its own max-connections setting.
fn start_tenant_connection(state: &SharedState, tenant_id: TenantId) -> crate::Result<()> {
    let mut tenants = state
        .tenants
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !matches!(
        tenants.check_connection(tenant_id),
        crate::control::security::tenant::QuotaCheck::Allowed
    ) {
        return Err(crate::Error::BadRequest {
            detail: "ILP admission denied".into(),
        });
    }
    tenants.connection_start(tenant_id);
    Ok(())
}
