// SPDX-License-Identifier: BUSL-1.1

//! D-δ integration test 1: nodedb binary exits within 1 second of SIGTERM.
//!
//! Spawns the real `nodedb` binary via `std::process::Command`, waits for
//! it to become ready (HTTP /healthz returns 200 via raw TCP), sends SIGTERM,
//! and asserts the process exits within 1,100 ms (1 s budget + 100 ms slack).
//!
//! Real process. Real signal. Real timer. No mocks.

mod support;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

/// How long to wait for the spawned server to report ready.
///
/// Generous on purpose: this is test setup, not a measured property. The server
/// boots while the rest of the suite saturates every core, so a tight budget
/// fails on machine load rather than on anything the test is checking.
const READY_BUDGET: Duration = Duration::from_secs(60);

/// Allocate an ephemeral port by binding, recording the port, then releasing.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local_addr").port()
}

/// Send a raw HTTP GET /healthz request and return whether the response is 200.
fn check_healthz(port: u16) -> bool {
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

/// Poll HTTP /healthz until 200 or deadline.
fn wait_for_healthz(port: u16, timeout: Duration) -> bool {
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

#[test]
fn real_nodedb_binary_exits_within_1_second_of_sigterm() {
    let bin = env!("CARGO_BIN_EXE_nodedb");

    // Use a unique temp dir and ephemeral ports for this test.
    let dir = tempfile::tempdir().expect("tempdir");
    let http_port = free_port();
    let pgwire_port = free_port();
    let native_port = free_port();
    // Unique too: the sync bind is boot-fatal, so concurrent server
    // processes must not share the default sync port.
    let sync_port = free_port();

    let mut cmd = std::process::Command::new(bin);
    // The shutdown budget must hold for the WAL a deployment actually runs, so
    // direct I/O stays on wherever the data directory supports it.
    support::direct_io::apply_wal_direct_io(&mut cmd, dir.path());
    let mut child = cmd
        .env("NODEDB_DATA_DIR", dir.path())
        .env("NODEDB_DATA_PLANE_CORES", "1")
        .env("NODEDB_PORT_HTTP", http_port.to_string())
        .env("NODEDB_PORT_PGWIRE", pgwire_port.to_string())
        .env("NODEDB_PORT_NATIVE", native_port.to_string())
        .env("NODEDB_PORT_SYNC", sync_port.to_string())
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn nodedb binary");

    // Setup, not the property under test — the SIGTERM budget below is what
    // this test measures. Boot competes with the rest of the suite for cores,
    // and a 15s budget lost that race often enough to fail runs that had
    // nothing wrong with them; the shutdown assertions are unchanged.
    let ready = wait_for_healthz(http_port, READY_BUDGET);
    assert!(
        ready,
        "nodedb did not become ready within {READY_BUDGET:?} — startup failure"
    );

    // Send SIGTERM and start the timer.
    let start = Instant::now();
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    #[cfg(not(unix))]
    {
        child.kill().expect("kill");
    }

    let status = child.wait().expect("wait for child");
    let elapsed = start.elapsed();

    assert!(
        status.success() || status.code() == Some(0),
        "nodedb exited with unexpected status {status:?} after SIGTERM"
    );
    assert!(
        elapsed <= Duration::from_millis(1100),
        "nodedb took {elapsed:?} to exit after SIGTERM — budget is 1s (1100ms with slack)"
    );
}
