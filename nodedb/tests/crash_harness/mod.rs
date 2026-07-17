// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill crash-recovery harness.
//!
//! This spawns the actual `nodedb` binary as a child process, lets tests
//! drive it over pgwire, then simulates a hard crash with `kill -9`
//! (`SIGKILL`, no graceful shutdown, no extra flush) followed by reaping the
//! zombie and spawning a fresh process on the SAME data directory. Reopening
//! triggers WAL replay through the normal binary boot path.
//!
//! This is deliberately distinct from the in-process `nodedb-test-support`
//! harnesses, which link the library directly and execute in the same OS
//! process as the test — they cannot simulate a real process crash because
//! there is no separate process to kill. Only an actual `kill -9` against a
//! separate child process exercises the boot-time WAL replay path the way a
//! real deployment would encounter it after a hard crash.

#![allow(dead_code)] // Not every crash-test binary uses every helper.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

pub fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local_addr").port()
}

pub fn check_healthz(port: u16) -> bool {
    let addr = format!("127.0.0.1:{port}");
    let mut stream = match TcpStream::connect_timeout(
        &addr.parse().expect("addr"),
        Duration::from_millis(200),
    ) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let req = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if stream.write_all(req).is_err() {
        return false;
    }
    let mut buf = [0u8; 256];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => {
            let resp = std::str::from_utf8(&buf[..n]).unwrap_or("");
            resp.starts_with("HTTP/1.1 200")
        }
        _ => false,
    }
}

pub fn wait_for_healthz(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() >= deadline {
            return false;
        }
        if check_healthz(port) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Owns a real `nodedb` child process plus the temp data directory it was
/// started against, so a test can crash it with `kill -9` and reopen the
/// same data directory to exercise WAL replay.
pub struct CrashHarness {
    bin: &'static str,
    tempdir: tempfile::TempDir,
    pub http_port: u16,
    pub pgwire_port: u16,
    pub native_port: u16,
    child: Option<std::process::Child>,
    /// Extra server env applied on EVERY spawn, including `reopen`. A test that
    /// tunes the server (short checkpoint interval, small WAL segments) needs
    /// the restarted process to boot under the same tuning as the one it
    /// killed, or the recovery half runs against a differently configured
    /// server than the crash half did.
    extra_env: Vec<(String, String)>,
}

impl CrashHarness {
    pub fn new() -> CrashHarness {
        let tempdir = tempfile::tempdir().expect("tempdir");
        CrashHarness {
            bin: env!("CARGO_BIN_EXE_nodedb"),
            tempdir,
            http_port: free_port(),
            pgwire_port: free_port(),
            native_port: free_port(),
            child: None,
            extra_env: Vec::new(),
        }
    }

    /// Add a server env override applied on every spawn. Call before `spawn`.
    pub fn with_env(mut self, key: &str, value: &str) -> CrashHarness {
        self.extra_env.push((key.to_string(), value.to_string()));
        self
    }

    /// The data directory this server was started against.
    pub fn data_dir(&self) -> &std::path::Path {
        self.tempdir.path()
    }

    /// File names of the WAL segments currently on disk, sorted.
    ///
    /// Reading the directory rather than asking the server keeps this honest:
    /// the question a truncation test must answer is whether the file was
    /// actually unlinked, which only the filesystem can answer.
    pub fn wal_segments(&self) -> Vec<String> {
        let dir = self.data_dir().join("wal");
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            // The WAL directory not existing yet is a legitimate "no segments"
            // answer during startup, not a test failure.
            Err(_) => return Vec::new(),
        };
        let mut names: Vec<String> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".seg"))
            .collect();
        names.sort();
        names
    }

    /// Spawn (or respawn) the `nodedb` binary against this harness's data
    /// directory and ports.
    pub fn spawn(&mut self) {
        let mut cmd = std::process::Command::new(self.bin);
        for (k, v) in &self.extra_env {
            cmd.env(k, v);
        }
        let child = cmd
            .env("NODEDB_DATA_DIR", self.tempdir.path())
            .env("NODEDB_DATA_PLANE_CORES", "1")
            .env("NODEDB_PORT_HTTP", self.http_port.to_string())
            .env("NODEDB_PORT_PGWIRE", self.pgwire_port.to_string())
            .env("NODEDB_PORT_NATIVE", self.native_port.to_string())
            // Pin the superuser password so the test can authenticate. Without
            // this the binary auto-generates a random password into
            // `<data_dir>/.superuser_password` (default auth mode is Password),
            // which the client would not know. The same value is used on reopen.
            .env("NODEDB_SUPERUSER_PASSWORD", "nodedb")
            .env("RUST_LOG", "error")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn nodedb binary");
        self.child = Some(child);
    }

    /// Block until `/healthz` reports ready, panicking on timeout.
    pub fn wait_ready(&self, timeout: Duration) {
        assert!(
            wait_for_healthz(self.http_port, timeout),
            "nodedb did not become ready within {timeout:?}"
        );
    }

    pub fn pgwire_conn_str(&self) -> String {
        format!(
            "host=127.0.0.1 port={} dbname=nodedb user=nodedb password=nodedb",
            self.pgwire_port
        )
    }

    /// Simulate a hard crash: `kill -9` with no graceful shutdown, no extra
    /// flush, then reap the zombie so the OS releases the process's ports.
    pub fn kill_9(&mut self) {
        let mut child = match self.child.take() {
            Some(c) => c,
            None => return,
        };
        #[cfg(unix)]
        unsafe {
            libc::kill(child.id() as i32, libc::SIGKILL);
        }
        #[cfg(not(unix))]
        {
            let _ = child.kill();
        }
        let _ = child.wait();
    }

    /// Spawn a fresh process on the same data directory (WAL replay on
    /// boot) and wait for it to become ready.
    pub fn reopen(&mut self) {
        self.spawn();
        self.wait_ready(Duration::from_secs(20));
    }

    /// Open a fresh pgwire connection, run one statement, and drop the
    /// connection. Panics on connect/exec error.
    pub async fn exec(&self, sql: &str) {
        let (client, connection) =
            tokio_postgres::connect(&self.pgwire_conn_str(), tokio_postgres::NoTls)
                .await
                .expect("connect for exec");
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        client.simple_query(sql).await.expect("exec");
        drop(client);
        let _ = conn_handle.await;
    }

    /// Run a query and return column `col` from every returned row, as text
    /// (via `simple_query`, so the value survives regardless of its type OID).
    pub async fn query_col(&self, sql: &str, col: &str) -> Vec<String> {
        let (client, connection) =
            tokio_postgres::connect(&self.pgwire_conn_str(), tokio_postgres::NoTls)
                .await
                .expect("connect for query");
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        let messages = client.simple_query(sql).await.expect("query");
        drop(client);
        let _ = conn_handle.await;
        messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(col).unwrap_or_default().to_string())
                }
                _ => None,
            })
            .collect()
    }

    /// Run a query and return column `idx` (0-based) from every returned row.
    ///
    /// Positional rather than by-name: a bare `COUNT(*)` — and even
    /// `COUNT(*) AS n` — does not surface a usable column name through the
    /// pgwire row description, so aggregates must be read by index.
    pub async fn query_col_idx(&self, sql: &str, idx: usize) -> Vec<String> {
        let (client, connection) =
            tokio_postgres::connect(&self.pgwire_conn_str(), tokio_postgres::NoTls)
                .await
                .expect("connect for query");
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        let messages = client.simple_query(sql).await.expect("query");
        drop(client);
        let _ = conn_handle.await;
        messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(idx).unwrap_or_default().to_string())
                }
                _ => None,
            })
            .collect()
    }
}

impl Default for CrashHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CrashHarness {
    fn drop(&mut self) {
        // Kill and reap any surviving process before the tempdir field
        // drops and removes the data directory, so we never leave an
        // orphan server process running against a deleted path.
        if self.child.is_some() {
            self.kill_9();
        }
    }
}
