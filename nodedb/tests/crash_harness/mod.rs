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

// `pub` so a crash test can read faultbox reports directly (see
// `crash_metadata_applier_wedge.rs`), not just via the panic-path
// diagnostics this module already wires into `pgwire.rs`.
pub mod diagnostics;
// The ILP client helper lives in `nodedb-test-support`, because tests outside
// this harness drive the same handshake. A test that needs it imports it from
// there directly rather than through a re-export here, which every crash-test
// binary that does NOT use it would have to suppress as unused.
mod pgwire;
pub mod resp_client;

// Only `crash_ilp_timeseries_write.rs` names `Session` and
// `RetryableSchemaChange` directly; every other crash-test binary pulls in this
// module too, so the re-exports are unused there.
#[allow(unused_imports)]
pub use pgwire::{RetryableSchemaChange, Session};
#[path = "../support/mod.rs"]
mod support;

/// Re-exported so a crash test can state its own filesystem precondition
/// without pulling the support module in a second time.
#[allow(unused_imports)]
pub use support::direct_io::direct_io_supported;

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
    /// `None` only in the instant between `Drop` taking it out to decide
    /// whether to retain it (see the `Drop` impl) — always `Some` otherwise.
    tempdir: Option<tempfile::TempDir>,
    /// Cached from `tempdir.path()` at construction so callers don't need to
    /// unwrap the `Option` above for the common case of just reading the path.
    data_dir_path: std::path::PathBuf,
    /// Number of times `spawn()` has been called on this harness, used to
    /// mark each boot's lines in the server log unambiguously.
    boot_count: u32,
    pub http_port: u16,
    pub pgwire_port: u16,
    pub native_port: u16,
    /// The sync WebSocket port. Unused by the tests themselves, but it must
    /// be unique per harness: every protocol bind is boot-fatal, so two
    /// concurrent harnesses left on the default sync port would collide and
    /// one server would refuse to boot.
    pub sync_port: u16,
    /// The RESP port. Always allocated, like the other ports, to avoid a bind collision.
    pub resp_port: u16,
    /// The ILP (InfluxDB Line Protocol) port. Always allocated, like the
    /// other ports, to avoid a bind collision, and always exported to the
    /// spawned process so `reopen` reuses the same port a pre-crash ILP
    /// connection was made against.
    pub ilp_port: u16,
    child: Option<std::process::Child>,
    /// Extra server env applied on EVERY spawn, including `reopen`. A test that
    /// tunes the server (short checkpoint interval, small WAL segments) needs
    /// the restarted process to boot under the same tuning as the one it
    /// killed, or the recovery half runs against a differently configured
    /// server than the crash half did.
    extra_env: Vec<(String, String)>,
    /// `NODEDB_WAL_DIRECT_IO` value forced on every spawn, or `None` to boot
    /// the server on its shipped default.
    ///
    /// Decided once at construction by probing the real data directory, and
    /// reused for `reopen` so a restarted process runs the same WAL mode as the
    /// one it replaces — a recovery half booting differently from the crash
    /// half would be testing a configuration no deployment ever runs.
    wal_direct_io: Option<&'static str>,
}

impl CrashHarness {
    pub fn new() -> CrashHarness {
        let tempdir = tempfile::tempdir().expect("tempdir");
        Self::from_tempdir(tempdir)
    }

    /// Like [`CrashHarness::new`], but the data directory is created under
    /// `parent` — used to place it on a filesystem chosen by the test rather
    /// than on whatever `TMPDIR` points at.
    pub fn new_in(parent: &std::path::Path) -> CrashHarness {
        let tempdir = tempfile::tempdir_in(parent).expect("tempdir in parent");
        Self::from_tempdir(tempdir)
    }

    fn from_tempdir(tempdir: tempfile::TempDir) -> CrashHarness {
        let data_dir_path = tempdir.path().to_path_buf();
        // Probed once, against the directory the server will actually write to.
        let wal_direct_io = support::direct_io::wal_direct_io_override(&data_dir_path);
        CrashHarness {
            bin: env!("CARGO_BIN_EXE_nodedb"),
            tempdir: Some(tempdir),
            data_dir_path,
            boot_count: 0,
            http_port: free_port(),
            pgwire_port: free_port(),
            native_port: free_port(),
            sync_port: free_port(),
            resp_port: free_port(),
            ilp_port: free_port(),
            child: None,
            extra_env: Vec::new(),
            wal_direct_io,
        }
    }

    /// Demand direct I/O even if the probe says the filesystem cannot provide
    /// it.
    ///
    /// Direct I/O is already the default wherever the data directory supports
    /// it, so this is for the two tests whose subject *is* the direct-I/O path:
    /// they must never be quietly downgraded into proving nothing.
    pub fn with_direct_io_wal(mut self) -> CrashHarness {
        self.wal_direct_io = Some("true");
        self
    }

    /// Force the WAL open buffered instead of using direct I/O.
    ///
    /// Only for a test whose subject is buffered I/O itself — everything else
    /// runs the production configuration so the suite covers the write path a
    /// deployment actually takes.
    pub fn with_buffered_wal(mut self) -> CrashHarness {
        self.wal_direct_io = Some("false");
        self
    }

    /// Add a server env override applied on every spawn. Call before `spawn`.
    pub fn with_env(mut self, key: &str, value: &str) -> CrashHarness {
        self.extra_env.push((key.to_string(), value.to_string()));
        self
    }

    /// Set (or replace) a server env override in place, between spawns.
    ///
    /// [`CrashHarness::with_env`] is builder-style and pins the value for the
    /// whole life of the harness — right for tuning knobs the recovery half
    /// must inherit from the crash half. A crash-during-recovery test needs the
    /// opposite: `NODEDB_FAILPOINTS` armed for exactly ONE boot. Left armed,
    /// every later `reopen` aborts at the same point and recovery never gets to
    /// run to completion, so the test could never observe the state it exists
    /// to check.
    pub fn set_env(&mut self, key: &str, value: &str) {
        match self.extra_env.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = value.to_string(),
            None => self.extra_env.push((key.to_string(), value.to_string())),
        }
    }

    /// Drop a previously-set env override so subsequent spawns boot without it.
    ///
    /// The test process itself never exports the vars this harness sets, so
    /// removing the override here really does leave the child with the variable
    /// unset rather than falling back to an inherited value.
    pub fn clear_env(&mut self, key: &str) {
        self.extra_env.retain(|(k, _)| k != key);
    }

    /// The data directory this server was started against.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir_path
    }

    /// A note appended to diagnostic panics stating where the data
    /// directory is retained, or empty when `NODEDB_TEST_KEEP_DATA_DIR` is
    /// unset. See [`diagnostics::keep_data_dir_note`].
    pub(crate) fn keep_data_dir_note(&self) -> String {
        diagnostics::keep_data_dir_note(&self.data_dir_path)
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
    /// Path the server's stdout/stderr is appended to across every spawn.
    pub fn server_log_path(&self) -> std::path::PathBuf {
        self.data_dir_path.join("server.log")
    }

    /// The server output captured so far, or empty if nothing was written.
    pub fn server_log(&self) -> String {
        std::fs::read_to_string(self.server_log_path()).unwrap_or_default()
    }

    pub fn spawn(&mut self) {
        let mut cmd = std::process::Command::new(self.bin);
        for (k, v) in &self.extra_env {
            cmd.env(k, v);
        }
        // Capture the server's output instead of discarding it. When a crash
        // test fails, the reason is almost always in the server's own log —
        // discarding it leaves nothing to debug but the timeout itself.
        // Appended, not truncated, so a `reopen` keeps the pre-crash half.
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.server_log_path())
            .expect("open server log");
        let log_err = log.try_clone().expect("clone server log handle");
        // Left unset in the common case so the child boots the same WAL mode a
        // deployment does; set only where the probe found no direct-I/O support
        // or a test asked for a specific mode.
        if let Some(value) = self.wal_direct_io {
            cmd.env("NODEDB_WAL_DIRECT_IO", value);
        }
        let child = cmd
            .env("NODEDB_DATA_DIR", &self.data_dir_path)
            .env("NODEDB_DATA_PLANE_CORES", "1")
            .env("NODEDB_PORT_HTTP", self.http_port.to_string())
            .env("NODEDB_PORT_PGWIRE", self.pgwire_port.to_string())
            .env("NODEDB_PORT_NATIVE", self.native_port.to_string())
            .env("NODEDB_PORT_SYNC", self.sync_port.to_string())
            .env("NODEDB_PORT_RESP", self.resp_port.to_string())
            // Unset by default (`config/server/env/host_ports.rs`), which
            // leaves the ILP listener disabled — a test that drives ILP must
            // set this to enable it, same as a real deployment opting in.
            .env("NODEDB_PORT_ILP", self.ilp_port.to_string())
            // Pin the superuser password so the test can authenticate. Without
            // this the binary auto-generates a random password into
            // `<data_dir>/.superuser_password` (default auth mode is Password),
            // which the client would not know. The same value is used on reopen.
            .env("NODEDB_SUPERUSER_PASSWORD", "nodedb")
            // A test that needs server diagnostics overrides this via
            // `with_env`, so it is set only when the test did not ask for
            // something else.
            .env(
                "RUST_LOG",
                self.extra_env
                    .iter()
                    .find(|(k, _)| k == "RUST_LOG")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("error"),
            )
            .stdout(std::process::Stdio::from(log))
            .stderr(std::process::Stdio::from(log_err))
            .spawn()
            .expect("failed to spawn nodedb binary");
        self.boot_count += 1;
        // Mark which boot these log lines belong to before the child has had
        // a chance to write its own first line — the log accumulates across
        // `spawn()`/`reopen()` in one file, so a tail dump is otherwise
        // ambiguous about which boot each line came from.
        diagnostics::mark_boot(&self.server_log_path(), self.boot_count, child.id());
        self.child = Some(child);
    }

    /// Block until `/healthz` reports ready, panicking on timeout.
    pub fn wait_ready(&self, timeout: Duration) {
        assert!(
            wait_for_healthz(self.http_port, timeout),
            "nodedb did not become ready within {timeout:?}"
        );
    }

    /// Spawn the server and assert that boot FAILS-STOP rather than coming up.
    ///
    /// Used by tests that make boot impossible before spawning — an unreadable
    /// checkpoint on disk, a WAL open the filesystem must refuse. The server
    /// must NEVER report `/healthz`-ready, and must exit non-zero
    /// within `timeout` (the fail-stop path aborts boot, `main` returns `Err`,
    /// and the process exits with a failure code). Panics if the server becomes
    /// ready, exits cleanly, or neither exits nor becomes ready in time.
    pub fn spawn_expect_boot_failure(&mut self, timeout: Duration) {
        self.spawn();
        let deadline = Instant::now() + timeout;
        loop {
            // A fail-stopped boot must never open the gateway / report ready.
            assert!(
                !check_healthz(self.http_port),
                "server became ready despite a boot condition it must fail-stop on"
            );
            if let Some(child) = self.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        assert!(
                            !status.success(),
                            "server exited cleanly (0) on a boot condition it must fail-stop on; \
                             expected a non-zero exit (status: {status:?})"
                        );
                        return;
                    }
                    Ok(None) => {}
                    Err(e) => panic!("failed to poll server process: {e}"),
                }
            }
            assert!(
                Instant::now() < deadline,
                "server neither became ready nor exited within {timeout:?}; \
                 fail-stop boot-abort did not occur"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
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

    /// Wait for the server to die on its own and reap it.
    ///
    /// Used with an armed `NODEDB_FAILPOINTS` abort: the crash happens inside
    /// the server, at an exact point the test could never hit from outside
    /// with `kill -9`. A timeout here means the injection never fired, so the
    /// test must fail rather than go on to prove nothing.
    pub fn await_self_crash(&mut self, timeout: Duration) {
        let mut child = match self.child.take() {
            Some(c) => c,
            None => panic!("no server process to wait on"),
        };
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait().expect("try_wait on server") {
                Some(_status) => return,
                None if Instant::now() >= deadline => {
                    #[cfg(unix)]
                    unsafe {
                        libc::kill(child.id() as i32, libc::SIGKILL);
                    }
                    let _ = child.wait();
                    let log = self.server_log();
                    let lines: Vec<&str> = log.lines().collect();
                    // Both ends matter: boot decides whether the subsystem
                    // under test even came up, the tail shows what it was
                    // doing when the wait expired. Budget (and so the head/tail
                    // split) comes from `NODEDB_TEST_LOG_TAIL_LINES`.
                    let budget = diagnostics::tail_line_count();
                    let half = budget / 2;
                    let excerpt = if lines.len() <= budget {
                        lines.join("\n")
                    } else {
                        format!(
                            "{}\n… {} lines elided …\n{}",
                            lines[..half].join("\n"),
                            lines.len() - 2 * half,
                            lines[lines.len() - half..].join("\n")
                        )
                    };
                    panic!(
                        "server was still alive after {timeout:?} — the injected fail point never \
                         fired, so this test proves NOTHING about crashing at that point.\n\
                         {}Server output ({} lines):\n{excerpt}",
                        self.keep_data_dir_note(),
                        lines.len()
                    );
                }
                None => std::thread::sleep(Duration::from_millis(100)),
            }
        }
    }

    /// Spawn a fresh process on the same data directory (WAL replay on
    /// boot) and wait for it to become ready.
    pub fn reopen(&mut self) {
        self.spawn();
        self.wait_ready(Duration::from_secs(20));
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
        // `tempdir` is always `Some` here except during this very drop, so
        // `take()` always succeeds.
        if let Some(dir) = self.tempdir.take()
            && diagnostics::keep_data_dir_requested()
        {
            // `keep()` consumes the guard without deleting the directory, so
            // the retained data survives past this drop.
            let kept = dir.keep();
            eprintln!(
                "\n\n=== NODEDB_TEST_KEEP_DATA_DIR: data directory retained at {} ===\n\n",
                kept.display()
            );
        }
        // else: `dir` drops here and removes the directory, same as today's
        // default behavior.
    }
}
