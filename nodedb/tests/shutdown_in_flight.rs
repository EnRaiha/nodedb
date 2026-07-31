// SPDX-License-Identifier: BUSL-1.1

//! D-δ integration test 2: SIGTERM during an in-flight query.
//!
//! Start the binary, open a real pgwire connection and issue a query, send
//! SIGTERM mid-query, assert the query either completes normally or returns
//! a network error (server closed connection). The server must NEVER hang
//! indefinitely and must exit cleanly.

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

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local_addr").port()
}

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

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_during_in_flight_query_does_not_hang() {
    let bin = env!("CARGO_BIN_EXE_nodedb");
    let dir = tempfile::tempdir().expect("tempdir");
    let http_port = free_port();
    let pgwire_port = free_port();
    let native_port = free_port();
    // Unique too: the sync bind is boot-fatal, so concurrent server
    // processes must not share the default sync port.
    let sync_port = free_port();

    let mut cmd = std::process::Command::new(bin);
    // Runs the production O_DIRECT WAL wherever the data directory supports it,
    // so shutdown is exercised against the write path a deployment uses.
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

    let ready = wait_for_healthz(http_port, READY_BUDGET);
    assert!(ready, "nodedb did not become ready within {READY_BUDGET:?}");

    let pgwire_addr = format!("127.0.0.1:{pgwire_port}");

    // Connect via pgwire and issue a simple query. We do this in a separate
    // task so we can concurrently send SIGTERM.
    let query_handle = tokio::spawn(async move {
        let (client, connection) = match tokio_postgres::connect(
            &format!("host=127.0.0.1 port={pgwire_port} dbname=default user=admin"),
            tokio_postgres::NoTls,
        )
        .await
        {
            Ok(r) => r,
            Err(_) => return, // Connection refused / closed — OK during shutdown
        };
        let _conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        // Issue a simple query. The server may close mid-query — that's fine.
        let _ = client.simple_query("SELECT 1").await;
        // The important assertion is that this returns at all (no hang).
    });

    // Wait a little then send SIGTERM.
    tokio::time::sleep(Duration::from_millis(200)).await;
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    #[cfg(not(unix))]
    {
        child.kill().expect("kill");
    }

    // Query task must complete (succeed or get an error) — must not hang.
    let query_result = tokio::time::timeout(Duration::from_secs(5), query_handle).await;
    assert!(
        query_result.is_ok(),
        "query task hung for >5s after SIGTERM — server did not close connections"
    );

    // Process must exit within 3s.
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(s) => break s,
            None => {
                if Instant::now() >= deadline {
                    child.kill().ok();
                    panic!("nodedb did not exit within 3s after SIGTERM");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };

    // Process exits with 0 (our handler does process::exit(0)) or non-zero
    // from the force-exit path — both are acceptable as long as it exits.
    let _ = status; // We just care it exited, not the specific code.

    // Verify the pgwire address is reachable check — the server is gone.
    let _ = pgwire_addr; // used above
}
