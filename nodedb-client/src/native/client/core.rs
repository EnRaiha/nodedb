// SPDX-License-Identifier: Apache-2.0

//! `NativeClient` struct definition and connection/session helpers.

use nodedb_types::error::{ErrorDetails, NodeDbError, NodeDbResult};
use nodedb_types::protocol::AuthMethod;
use nodedb_types::result::QueryResult;

use super::super::pool::{Pool, PoolConfig};

/// Native protocol client for NodeDB.
///
/// Connects via the binary MessagePack protocol. Supports all operations:
/// SQL, DDL, direct Data Plane ops, transactions, session parameters.
pub struct NativeClient {
    pub(super) pool: Pool,
}

impl NativeClient {
    /// Create a client with the given pool configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            pool: Pool::new(config),
        }
    }

    /// Connect to a NodeDB server, authenticating as `auth`.
    ///
    /// The identity is a required argument, not a default: trust
    /// authentication is passwordless, so a default identity would let a
    /// caller who forgot to configure auth connect as whatever account the
    /// default names — privilege escalation by omission. Use
    /// [`PoolConfig::new`] plus [`NativeClient::new`] directly if other pool
    /// tuning (max size, timeouts, TLS, database) needs to be non-default too.
    pub fn connect_as(addr: &str, auth: AuthMethod) -> Self {
        Self::new(PoolConfig::new(addr, auth))
    }

    /// Execute a SQL query and return structured results.
    ///
    /// Retries once with a fresh connection on I/O failure.
    pub async fn query(&self, sql: &str) -> NodeDbResult<QueryResult> {
        let mut conn = self.pool.acquire().await?;
        match conn.execute_sql(sql).await {
            Ok(r) => Ok(r),
            Err(e) if is_connection_error(&e) => {
                drop(conn);
                let mut conn = self.pool.acquire().await?;
                conn.execute_sql(sql).await
            }
            Err(e) => Err(e),
        }
    }

    /// Execute a DDL command.
    pub async fn ddl(&self, sql: &str) -> NodeDbResult<QueryResult> {
        let mut conn = self.pool.acquire().await?;
        match conn.execute_ddl(sql).await {
            Ok(r) => Ok(r),
            Err(e) if is_connection_error(&e) => {
                drop(conn);
                let mut conn = self.pool.acquire().await?;
                conn.execute_ddl(sql).await
            }
            Err(e) => Err(e),
        }
    }

    /// Begin a transaction.
    pub async fn begin(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.begin().await
    }

    /// Commit the current transaction.
    pub async fn commit(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.commit().await
    }

    /// Rollback the current transaction.
    pub async fn rollback(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.rollback().await
    }

    /// Set a session parameter.
    pub async fn set_parameter(&self, key: &str, value: &str) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.set_parameter(key, value).await
    }

    /// Show a session parameter.
    pub async fn show_parameter(&self, key: &str) -> NodeDbResult<String> {
        let mut conn = self.pool.acquire().await?;
        conn.show_parameter(key).await
    }

    /// Ping the server.
    pub async fn ping(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.ping().await
    }
}

/// Check if an error is a connection-level failure (worth retrying).
pub(super) fn is_connection_error(e: &NodeDbError) -> bool {
    matches!(
        e.details(),
        ErrorDetails::SyncConnectionFailed | ErrorDetails::Storage { .. }
    )
}
