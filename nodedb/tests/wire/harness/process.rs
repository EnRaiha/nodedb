// SPDX-License-Identifier: BUSL-1.1

//! Spawns the real `nodedb` binary as a subprocess against a data directory,
//! waits for `/healthz`, captures its stdout/stderr to a log file, and tears
//! it down with `SIGTERM` (falling back to `SIGKILL`) — mirroring
//! `tests/crash_harness/mod.rs`, the proven in-repo subprocess pattern.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use super::config_toml::{self, AuthMode};

/// Bind an ephemeral port then release it, handing the number to a child
/// process that will bind it itself. A second process can win the same port
/// before the child binds — `crash_harness::free_port` already accepts this
/// race, so this mirrors it rather than inventing a different scheme.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener
        .local_addr()
        .expect("ephemeral port local_addr")
        .port()
}

/// Ports a spawned server binds to, allocated once before each spawn.
pub(super) struct ServerPorts {
    pub(super) http: u16,
    pub(super) pgwire: u16,
    pub(super) native: u16,
    pub(super) sync: u16,
    pub(super) resp: u16,
}

impl ServerPorts {
    fn allocate() -> Self {
        Self {
            http: free_port(),
            pgwire: free_port(),
            native: free_port(),
            sync: free_port(),
            resp: free_port(),
        }
    }
}

/// Probe `/healthz` once. It returns 503 until the gateway is enabled, so
/// polling for HTTP 200 is a correct readiness wait.
fn check_healthz(port: u16) -> bool {
    let addr = format!("127.0.0.1:{port}");
    let mut stream = match TcpStream::connect_timeout(
        &addr.parse().expect("parse loopback addr"),
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

impl SpawnedServer {
    /// Poll `/healthz` until the server answers 200, it exits, or `timeout`
    /// elapses. The error string says which of the three happened.
    fn wait_until_ready(&mut self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            // A server that died during boot never answers, so waiting out the
            // full timeout would hide the real error. Report the exit instead.
            if let Some(child) = self.child.as_mut()
                && let Ok(Some(status)) = child.try_wait()
            {
                return Err(format!("nodedb exited during startup with {status}"));
            }
            if check_healthz(self.ports.http) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "nodedb did not answer /healthz within {}s",
                    timeout.as_secs()
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The server's captured stdout and stderr, or a note saying why it could
    /// not be read. Used to make a startup failure self-explaining.
    fn read_log(&self) -> String {
        std::fs::read_to_string(&self.log_path)
            .unwrap_or_else(|e| format!("(could not read {}: {e})", self.log_path.display()))
    }
}

/// A spawned `nodedb` server process plus the ports it is listening on.
pub(super) struct SpawnedServer {
    pub(super) ports: ServerPorts,
    log_path: PathBuf,
    /// `None` once `graceful_shutdown` has taken and reaped the child.
    child: Option<Child>,
}

/// Spawn the real `nodedb` binary against `data_dir`, writing its config file
/// there first. Blocks until `/healthz` reports ready, panicking after 20s.
pub(super) fn spawn(
    data_dir: &Path,
    auth_mode: AuthMode,
    columnar_flush_threshold: Option<usize>,
) -> SpawnedServer {
    let config_path = config_toml::write_config(data_dir, auth_mode, columnar_flush_threshold);
    let ports = ServerPorts::allocate();

    let log_path = data_dir.join("server.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .expect("open server log");
    let log_err = log.try_clone().expect("clone server log handle");

    let child = Command::new(env!("CARGO_BIN_EXE_nodedb"))
        .env("NODEDB_DATA_DIR", data_dir)
        .env("NODEDB_DATA_PLANE_CORES", "1")
        .env("NODEDB_CONFIG", &config_path)
        .env("NODEDB_PORT_HTTP", ports.http.to_string())
        .env("NODEDB_PORT_PGWIRE", ports.pgwire.to_string())
        .env("NODEDB_PORT_NATIVE", ports.native.to_string())
        .env("NODEDB_PORT_SYNC", ports.sync.to_string())
        .env("NODEDB_PORT_RESP", ports.resp.to_string())
        // Pinned so the harness client can authenticate in password mode;
        // trust mode ignores it. Same value on every spawn (including a
        // restart against the same data directory).
        .env("NODEDB_SUPERUSER_PASSWORD", "nodedb")
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::from(log))
        .stderr(std::process::Stdio::from(log_err))
        .spawn()
        .expect("failed to spawn nodedb binary");

    let mut server = SpawnedServer {
        ports,
        log_path: log_path.clone(),
        child: Some(child),
    };

    if let Err(reason) = server.wait_until_ready(READY_TIMEOUT) {
        // The data directory is a temp dir that is removed as soon as this
        // panic unwinds, taking the server log with it. Print the log here or
        // the failure is undiagnosable.
        panic!("{reason}\n--- server log ---\n{}", server.read_log());
    }
    server
}

/// How long a spawned server has to answer `/healthz`. Generous because the
/// whole suite runs servers in parallel, so a cold boot competes for CPU.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

impl SpawnedServer {
    /// Send `SIGTERM` and wait for the process to exit — the server's own
    /// signal handler runs its graceful shutdown, which syncs the WAL (see
    /// `src/bootstrap/signal.rs`). Falls back to `SIGKILL` if it does not
    /// exit within the timeout.
    pub(super) async fn graceful_shutdown(mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        #[cfg(unix)]
        unsafe {
            libc::kill(child.id() as i32, libc::SIGTERM);
        }
        #[cfg(not(unix))]
        {
            let _ = child.kill();
        }

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    eprintln!(
                        "nodedb did not exit within 20s of SIGTERM; force-killed. See {}",
                        self.log_path.display()
                    );
                    return;
                }
                Ok(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                Err(_) => return,
            }
        }
    }
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        // `child` is `None` when `graceful_shutdown` already ran.
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
