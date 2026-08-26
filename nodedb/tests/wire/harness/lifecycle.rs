// SPDX-License-Identifier: BUSL-1.1

//! `start*` / `open_on_path*` constructors, `take_dir`, and
//! `graceful_shutdown` on [`TestServer`].
//!
//! Each constructor spawns a real `nodedb` subprocess (see `process.rs`)
//! against a temp directory and connects a `tokio_postgres::Client` to it.

use std::time::Duration;

use super::config_toml::AuthMode;
use super::process::{self, SpawnedServer};
use super::types::{TestClient, TestDataDir, TestServer};

impl TestServer {
    /// Spawn a single-core NodeDB server and connect via pgwire (trust mode).
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(dir.path(), AuthMode::Trust, None, 1);
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a NodeDB server with `cores` Data Plane cores and connect via
    /// pgwire (trust mode). Exercises fan-out/gather across cores, which a
    /// single-core server cannot.
    pub async fn start_multicores(cores: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(dir.path(), AuthMode::Trust, None, cores);
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a single-core server in pgwire **password mode** (SCRAM-SHA-256)
    /// with the credential lockout policy enabled (`5` failures -> `300s`).
    ///
    /// The harness user `nodedb` keeps password `nodedb`; the returned
    /// client authenticates with it.
    pub async fn start_password() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(dir.path(), AuthMode::Password, None, 1);
        Self::connect_and_build(spawned, dir, AuthMode::Password).await
    }

    /// Spawn a single-core NodeDB server with a lowered
    /// `columnar_flush_threshold` so tests can observe segment-flush
    /// behaviour on small datasets without inserting 65k rows.
    pub async fn start_with_columnar_flush_threshold(flush_threshold: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(dir.path(), AuthMode::Trust, Some(flush_threshold), 1);
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Open a server backed by an existing data directory.
    ///
    /// The WAL and catalog inside `dir` are reopened in place by the
    /// subprocess, so any data written by a previous server is immediately
    /// visible after boot. `dir` is NOT consumed: ownership stays with the
    /// caller, who gets it back unchanged, exactly as
    /// `nodedb-test-support::pgwire_harness::TestServer::open_on_path` does.
    pub async fn open_on_path(dir: TestDataDir) -> (Self, TestDataDir) {
        let spawned = process::spawn(dir.path(), AuthMode::Trust, None, 1);
        let placeholder = tempfile::tempdir().expect("placeholder tempdir");
        let server = Self::connect_and_build(spawned, placeholder, AuthMode::Trust).await;
        (server, dir)
    }

    /// Open a server backed by an existing data directory with a custom
    /// `columnar_flush_threshold`. Pass the same value the original server
    /// used to keep flush behaviour consistent across the restart boundary.
    pub async fn open_on_path_with_columnar_flush_threshold(
        dir: TestDataDir,
        flush_threshold: usize,
    ) -> (Self, TestDataDir) {
        let spawned = process::spawn(dir.path(), AuthMode::Trust, Some(flush_threshold), 1);
        let placeholder = tempfile::tempdir().expect("placeholder tempdir");
        let server = Self::connect_and_build(spawned, placeholder, AuthMode::Trust).await;
        (server, dir)
    }

    /// Consume the data directory from a live server so it outlives the
    /// server's lifetime. The server keeps running until dropped, but
    /// ownership of the temp dir moves to the caller so the files survive
    /// the `Drop` of `TestServer`.
    pub fn take_dir(mut self) -> (Self, TestDataDir) {
        let placeholder = tempfile::tempdir().expect("placeholder tempdir");
        let original_dir = std::mem::replace(&mut self._dir, placeholder);
        (self, TestDataDir(original_dir))
    }

    /// Consume the server, close the harness connection, and send `SIGTERM`
    /// to the subprocess, waiting for its own graceful shutdown (WAL sync)
    /// to complete before returning.
    pub async fn graceful_shutdown(mut self) {
        let _ = self.client.take();
        if let Some(h) = self.conn_handle.take() {
            h.abort();
            let _ = h.await;
        }
        if let Some(spawned) = self.spawned.take() {
            spawned.graceful_shutdown().await;
        }
    }

    /// Connect the harness client to a just-spawned server and assemble the
    /// `TestServer`. `dir` becomes the new `_dir` field (either the real
    /// owned directory for a fresh `start*`, or a placeholder for
    /// `open_on_path*`, whose real directory the caller keeps).
    async fn connect_and_build(
        spawned: SpawnedServer,
        dir: tempfile::TempDir,
        auth_mode: AuthMode,
    ) -> Self {
        let conn_str = match auth_mode {
            AuthMode::Password => format!(
                "host=127.0.0.1 port={} user=nodedb password=nodedb dbname=default",
                spawned.ports.pgwire
            ),
            AuthMode::Trust => format!(
                "host=127.0.0.1 port={} user=nodedb dbname=default",
                spawned.ports.pgwire
            ),
        };
        // The subprocess reports /healthz ready before the pgwire listener
        // necessarily finishes its own startup gate; retry briefly rather
        // than treating the first refused connection as fatal.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let (client, connection) = loop {
            match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
                Ok(pair) => break pair,
                Err(_) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => panic!("pgwire connect failed: {e}"),
            }
        };
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Self {
            client: TestClient::new(client),
            pg_port: spawned.ports.pgwire,
            native_port: spawned.ports.native,
            http_port: spawned.ports.http,
            spawned: Some(spawned),
            conn_handle: Some(conn_handle),
            _dir: dir,
        }
    }
}
