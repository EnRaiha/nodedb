// SPDX-License-Identifier: Apache-2.0

//! Fluent builder for `NativeClient` connections.
//!
//! ```rust,ignore
//! let client = ConnectionBuilder::new("127.0.0.1:6433")
//!     .username("alice")
//!     .password("s3cr3t")
//!     .database("analytics")
//!     .max_connections(20)
//!     .build()?;
//! # Ok::<(), nodedb_types::error::NodeDbError>(())
//! ```

use std::time::Duration;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::AuthMethod;

use super::client::NativeClient;
use super::connection::TlsConfig;
use super::pool::PoolConfig;

/// Fluent builder for a [`NativeClient`] connection.
///
/// Call [`build`](Self::build) to construct the client once all options
/// have been set.
#[derive(Debug, Default)]
pub struct ConnectionBuilder {
    addr: Option<String>,
    username: Option<String>,
    password: Option<String>,
    api_key: Option<String>,
    database: Option<String>,
    max_connections: Option<usize>,
    connect_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    tls: Option<TlsConfig>,
}

impl ConnectionBuilder {
    /// Start building a connection to `addr` (e.g. `"127.0.0.1:6433"`).
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: Some(addr.into()),
            ..Default::default()
        }
    }

    /// Set the username for trust or password authentication.
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Set the password (enables SCRAM-SHA-256 / cleartext authentication).
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set an API key token (enables API key authentication).
    pub fn api_key(mut self, token: impl Into<String>) -> Self {
        self.api_key = Some(token.into());
        self
    }

    /// Set the target database name.
    ///
    /// The database name is sent in the auth handshake frame so every
    /// connection in the pool executes within this database context.
    /// Equivalent to `psql -d <name>` for the native protocol.
    pub fn database(mut self, name: impl Into<String>) -> Self {
        self.database = Some(name.into());
        self
    }

    /// Set the maximum number of pooled connections (default: 10).
    pub fn max_connections(mut self, n: usize) -> Self {
        self.max_connections = Some(n);
        self
    }

    /// Set the connection timeout (default: 5 seconds).
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = Some(d);
        self
    }

    /// Set the idle connection timeout (default: 5 minutes).
    pub fn idle_timeout(mut self, d: Duration) -> Self {
        self.idle_timeout = Some(d);
        self
    }

    /// Configure TLS.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Build the `NativeClient`.
    ///
    /// The connection address falls back to `127.0.0.1:6433` when unset —
    /// that is a convenience default, not a security decision. Identity is
    /// the one option with no default: trust auth is passwordless, so a
    /// defaulted username would silently authenticate as whatever that
    /// default happened to be (privilege escalation by omission). Callers
    /// must supply either [`api_key`](Self::api_key) (which carries its own
    /// identity via the token) or [`username`](Self::username) — do not
    /// reintroduce a fallback for either. Never add a default for `addr`
    /// beyond the existing one either; keep the two concerns separate.
    ///
    /// # Errors
    ///
    /// Returns an error if neither `api_key` nor `username` was set.
    pub fn build(self) -> NodeDbResult<NativeClient> {
        let addr = self.addr.unwrap_or_else(|| "127.0.0.1:6433".to_string());
        let auth = resolve_auth(self.username, self.password, self.api_key)?;

        // `PoolConfig::new` already carries the identity (`auth`, always
        // built above — never omitted) plus this crate's tuning defaults;
        // only override the tuning fields the caller actually set.
        let default_config = PoolConfig::new(addr, auth);

        let config = PoolConfig {
            database: self.database,
            max_size: self.max_connections.unwrap_or(default_config.max_size),
            connect_timeout: self
                .connect_timeout
                .unwrap_or(default_config.connect_timeout),
            idle_timeout: self.idle_timeout.unwrap_or(default_config.idle_timeout),
            tls: self.tls.unwrap_or_default(),
            ..default_config
        };

        Ok(NativeClient::new(config))
    }
}

/// Resolve the builder's optional identity fields into a required
/// [`AuthMethod`].
///
/// This is a disjunction, not three independent defaults: an `api_key`
/// carries its own identity via the token, so it alone is sufficient. The
/// trust and password branches have no such built-in identity, so they
/// require an explicit `username` — there is no fallback for either.
fn resolve_auth(
    username: Option<String>,
    password: Option<String>,
    api_key: Option<String>,
) -> NodeDbResult<AuthMethod> {
    if let Some(token) = api_key {
        return Ok(AuthMethod::ApiKey { token });
    }
    let username = username.ok_or_else(|| {
        NodeDbError::config(
            "no authentication identity configured: call .username(...) or .api_key(...)",
        )
    })?;
    Ok(match password {
        Some(password) => AuthMethod::Password { username, password },
        None => AuthMethod::Trust { username },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_auth_with_username_carries_it_into_trust() {
        let auth = resolve_auth(Some("alice".to_string()), None, None).expect("username given");
        match auth {
            AuthMethod::Trust { username } => assert_eq!(username, "alice"),
            other => panic!("expected AuthMethod::Trust, got {other:?}"),
        }
    }

    #[test]
    fn resolve_auth_with_username_and_password_carries_it_into_password() {
        let auth = resolve_auth(Some("bob".to_string()), Some("secret".to_string()), None)
            .expect("username given");
        match auth {
            AuthMethod::Password { username, password } => {
                assert_eq!(username, "bob");
                assert_eq!(password, "secret");
            }
            other => panic!("expected AuthMethod::Password, got {other:?}"),
        }
    }

    #[test]
    fn resolve_auth_with_api_key_succeeds_without_username() {
        let auth = resolve_auth(None, None, Some("token-123".to_string())).expect("api_key given");
        assert!(matches!(auth, AuthMethod::ApiKey { token } if token == "token-123"));
    }

    #[test]
    fn resolve_auth_with_no_identity_errors() {
        // Regression lock: no username, no api_key — must fail, never
        // silently authenticate as some default identity.
        let err = resolve_auth(None, None, None).expect_err("must reject a missing identity");
        assert!(err.message().contains("authentication identity"));
    }

    #[test]
    fn resolve_auth_with_password_but_no_username_errors() {
        let err = resolve_auth(None, Some("secret".to_string()), None)
            .expect_err("must reject password auth without a username");
        assert!(err.message().contains("authentication identity"));
    }

    #[test]
    fn builder_with_username_succeeds() {
        let client = ConnectionBuilder::new("127.0.0.1:6433")
            .username("alice")
            .build();
        assert!(client.is_ok());
    }

    #[test]
    fn builder_with_api_key_succeeds_without_username() {
        let client = ConnectionBuilder::new("127.0.0.1:6433")
            .api_key("token-123")
            .build();
        assert!(client.is_ok());
    }

    #[test]
    fn builder_with_no_identity_errors() {
        // Regression lock: no username, no api_key — must fail, never
        // silently authenticate as some default identity.
        // `NativeClient` is not `Debug` (it owns a live connection pool), so
        // the Ok payload is discarded before `expect_err`.
        let err = ConnectionBuilder::new("127.0.0.1:6433")
            .build()
            .map(|_| ())
            .expect_err("build() must reject a missing identity");
        assert!(err.message().contains("authentication identity"));
    }

    #[test]
    fn builder_password_without_username_errors() {
        let err = ConnectionBuilder::new("127.0.0.1:6433")
            .password("secret")
            .build()
            .map(|_| ())
            .expect_err("build() must reject password auth without a username");
        assert!(err.message().contains("authentication identity"));
    }

    #[test]
    fn builder_with_database() {
        let _client = ConnectionBuilder::new("127.0.0.1:6433")
            .username("alice")
            .database("analytics")
            .build()
            .expect("username was supplied, build() must succeed");
    }

    #[test]
    fn builder_password_auth() {
        let _client = ConnectionBuilder::new("127.0.0.1:6433")
            .username("bob")
            .password("secret")
            .database("prod")
            .build()
            .expect("username was supplied, build() must succeed");
    }
}
