// SPDX-License-Identifier: BUSL-1.1

//! Second-connection helpers (`connect_as` / `connect_as_database`) and the
//! `with_database` constructor on [`TestServer`].

use super::query::pg_error_detail;
use super::types::TestServer;

impl TestServer {
    /// Open a second pgwire connection on the same listener under a different
    /// username. Returns a client and its background connection task handle.
    pub async fn connect_as(
        &self,
        user: &str,
        password: &str,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), String> {
        self.connect_as_database(user, password, "default").await
    }

    /// Open a second pgwire connection under a user-selected database.
    pub async fn connect_as_database(
        &self,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), String> {
        let conn_str = format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            self.pg_port, user, password, database
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|error| pg_error_detail(&error))?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok((client, handle))
    }

    /// Spawn a server and connect to a named database.
    ///
    /// The database is created inside the running server after startup. A
    /// UUID suffix is appended to `name` to guarantee uniqueness across
    /// parallel test runs (e.g. `emp_prod_<uuid>`). The returned name is
    /// the full suffixed name so callers can reference it in subsequent
    /// queries.
    pub async fn with_database(name: &str) -> (Self, String) {
        let server = Self::start().await;
        let unique_name = format!("{}_{}", name, uuid_v4_hex());
        server
            .client
            .simple_query(&format!("CREATE DATABASE {unique_name}"))
            .await
            .unwrap_or_else(|e| panic!("with_database: CREATE DATABASE {unique_name} failed: {e}"));
        server
            .client
            .simple_query(&format!("USE DATABASE {unique_name}"))
            .await
            .unwrap_or_else(|e| panic!("with_database: USE DATABASE {unique_name} failed: {e}"));
        (server, unique_name)
    }
}

/// Generate a short hex string suitable for unique test name suffixes.
fn uuid_v4_hex() -> String {
    let id = uuid::Uuid::new_v4();
    let bytes = id.as_bytes();
    // Use the first 8 bytes (16 hex chars) — enough entropy for test isolation.
    format!(
        "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]
    )
}
