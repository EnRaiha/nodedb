// SPDX-License-Identifier: Apache-2.0

//! Bounded connection pool for native protocol connections.
//!
//! Uses a semaphore for max-size enforcement and a std::sync::Mutex
//! idle queue (so Drop can return connections synchronously).
//! Health checks (ping) are performed on idle connections before
//! handing them out.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{AuthMethod, Limits};
use tokio::sync::{Semaphore, SemaphorePermit};

use super::connection::NativeConnection;

/// Configuration for the connection pool.
///
/// There is no `Default` impl — construct via [`PoolConfig::new`], which
/// requires an explicit `auth` identity. See that constructor's doc comment
/// for why a defaulted identity is unsafe.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Server address (host:port).
    pub addr: String,
    /// Maximum number of connections.
    pub max_size: usize,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Idle connection timeout (connections idle longer than this are dropped).
    pub idle_timeout: Duration,
    /// Authentication method. Required — no default identity (see
    /// [`PoolConfig::new`]).
    pub auth: AuthMethod,
    /// Target database name sent in the auth handshake frame.
    ///
    /// `None` means the server default (`"default"` / `DatabaseId::DEFAULT`).
    pub database: Option<String>,
    /// TLS configuration. Default: disabled.
    pub tls: super::connection::TlsConfig,
}

impl PoolConfig {
    /// Construct a pool configuration with an explicit connection address and
    /// authentication identity.
    ///
    /// There is intentionally no `Default` impl for `PoolConfig`: trust
    /// authentication is passwordless, so a server configured for trust auth
    /// grants full privileges to whatever username the client claims. A
    /// default identity (e.g. a hardcoded `admin`) would let a caller connect
    /// as the server's most privileged account simply by forgetting to
    /// configure auth — privilege escalation by omission. Every caller must
    /// state who they are connecting as.
    ///
    /// Every other tuning field keeps its previous default and can still be
    /// overridden via struct-update syntax:
    ///
    /// ```rust,ignore
    /// use nodedb_client::native::pool::PoolConfig;
    /// use nodedb_types::protocol::AuthMethod;
    ///
    /// let auth = AuthMethod::Trust {
    ///     username: "alice".into(),
    /// };
    /// let config = PoolConfig {
    ///     max_size: 2,
    ///     ..PoolConfig::new("127.0.0.1:6433", auth)
    /// };
    /// ```
    pub fn new(addr: impl Into<String>, auth: AuthMethod) -> Self {
        Self {
            addr: addr.into(),
            max_size: 10,
            connect_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            auth,
            database: None,
            tls: Default::default(),
        }
    }
}

/// Negotiated connection metadata from the first handshake performed by this pool.
#[derive(Debug, Clone, Default)]
pub struct NegotiatedMeta {
    pub proto_version: u16,
    pub capabilities: u64,
    pub server_version: String,
    pub limits: Limits,
}

/// Shared state for idle connection return.
///
/// Uses `std::sync::Mutex` (not tokio) so the `Drop` impl can
/// return connections synchronously without spawning async tasks.
struct PoolInner {
    idle: Mutex<VecDeque<NativeConnection>>,
    /// Negotiated metadata from the first handshake.
    meta: Mutex<Option<NegotiatedMeta>>,
    max_size: usize,
}

/// A bounded pool of `NativeConnection` instances.
pub struct Pool {
    config: PoolConfig,
    inner: Arc<PoolInner>,
    semaphore: Semaphore,
}

impl Pool {
    /// Create a new pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        let max_size = config.max_size;
        let semaphore = Semaphore::new(max_size);
        Self {
            config,
            inner: Arc::new(PoolInner {
                idle: Mutex::new(VecDeque::new()),
                meta: Mutex::new(None),
                max_size,
            }),
            semaphore,
        }
    }

    /// Returns the negotiated connection metadata from the first handshake, or
    /// `None` if no connection has been established yet.
    pub fn negotiated_meta(&self) -> Option<NegotiatedMeta> {
        self.inner
            .meta
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Acquire a connection from the pool.
    ///
    /// Returns an idle connection if available, otherwise creates a new one.
    /// Blocks if `max_size` connections are already in use.
    pub async fn acquire(&self) -> NodeDbResult<PooledConnection<'_>> {
        let permit = tokio::time::timeout(self.config.connect_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| NodeDbError::sync_connection_failed("pool acquire timeout"))?
            .map_err(|_| NodeDbError::sync_connection_failed("pool closed"))?;

        // std::sync::Mutex is intentional here (not tokio::sync::Mutex):
        // 1. Critical section is trivial (pop_front / push_back only)
        // 2. No async operations while holding the lock
        // 3. Enables synchronous return in Drop (no spawned tasks)
        // 4. Poison is handled gracefully via unwrap_or_else
        let idle_conn = {
            let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
            idle.pop_front()
        };

        if let Some(mut conn) = idle_conn {
            // Health check: ping to verify the connection is still alive.
            if conn.ping().await.is_ok() {
                return Ok(PooledConnection {
                    conn: Some(conn),
                    inner: Arc::clone(&self.inner),
                    _permit: permit,
                });
            }
            // Connection dead — create a new one below.
        }

        // Create a new connection (plain TCP or TLS).
        let addr = self.config.addr.clone();
        let tls_cfg = self.config.tls.clone();
        let timeout = self.config.connect_timeout;
        let mut conn = tokio::time::timeout(timeout, async move {
            if tls_cfg.enabled {
                NativeConnection::connect_tls(&addr, &tls_cfg).await
            } else {
                NativeConnection::connect(&addr).await
            }
        })
        .await
        .map_err(|_| NodeDbError::sync_connection_failed("connect timeout"))??;

        // `NativeConnection::connect` already performed the handshake.
        // Calling it a second time here would write a stale HelloFrame
        // into the post-handshake stream and confuse the framed read
        // path (the bytes get mis-parsed as a regular response frame).

        // Authenticate — pass the optional database name for handshake binding.
        conn.authenticate(self.config.auth.clone(), self.config.database.as_deref())
            .await?;

        // Capture negotiated metadata from the first handshake.
        {
            let mut meta = self.inner.meta.lock().unwrap_or_else(|e| e.into_inner());
            if meta.is_none() {
                *meta = Some(NegotiatedMeta {
                    proto_version: conn.proto_version,
                    capabilities: conn.capabilities,
                    server_version: conn.server_version.clone(),
                    limits: conn.limits.clone(),
                });
            }
        }

        Ok(PooledConnection {
            conn: Some(conn),
            inner: Arc::clone(&self.inner),
            _permit: permit,
        })
    }
}

/// A connection borrowed from the pool.
///
/// Returns to the idle queue synchronously on drop (no spawned tasks).
pub struct PooledConnection<'a> {
    conn: Option<NativeConnection>,
    inner: Arc<PoolInner>,
    _permit: SemaphorePermit<'a>,
}

impl std::ops::Deref for PooledConnection<'_> {
    type Target = NativeConnection;
    fn deref(&self) -> &NativeConnection {
        self.conn.as_ref().expect("connection taken")
    }
}

impl std::ops::DerefMut for PooledConnection<'_> {
    fn deref_mut(&mut self) -> &mut NativeConnection {
        self.conn.as_mut().expect("connection taken")
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Synchronous return to idle queue — no async, no spawned tasks.
            let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
            if idle.len() < self.inner.max_size {
                idle.push_back(conn);
            }
            // else: too many idle connections — drop this one.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trust(username: &str) -> AuthMethod {
        AuthMethod::Trust {
            username: username.to_string(),
        }
    }

    #[test]
    fn pool_config_new_sets_tuning_defaults() {
        let cfg = PoolConfig::new("127.0.0.1:6433", trust("alice"));
        assert_eq!(cfg.addr, "127.0.0.1:6433");
        assert_eq!(cfg.max_size, 10);
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
    }

    /// `PoolConfig::new` must carry the caller's identity through to the
    /// built config unchanged — asserted explicitly so a future refactor
    /// cannot quietly reintroduce a hardcoded default identity.
    #[test]
    fn pool_config_new_carries_caller_identity() {
        let cfg = PoolConfig::new("127.0.0.1:6433", trust("alice"));
        match cfg.auth {
            AuthMethod::Trust { username } => assert_eq!(username, "alice"),
            other => panic!("expected AuthMethod::Trust, got {other:?}"),
        }
    }

    #[test]
    fn pool_creates_semaphore() {
        let pool = Pool::new(PoolConfig {
            max_size: 5,
            ..PoolConfig::new("127.0.0.1:6433", trust("alice"))
        });
        assert_eq!(pool.semaphore.available_permits(), 5);
    }
}
