// SPDX-License-Identifier: BUSL-1.1

//! `nodedb healthcheck` subcommand.
//!
//! Exists primarily so distroless / Chainguard runtime images can run
//! a Docker `HEALTHCHECK` without shipping `curl`. Performs a single
//! synchronous HTTP `GET /healthz` against the local HTTP API port and
//! exits 0 on a 2xx response, non-zero otherwise.
//!
//! `/healthz` is the readiness probe: it reports 503 while the node is
//! draining or has not reached `GatewayEnable`, so a container runtime stops
//! routing to a node that cannot serve.
//!
//! Kept dependency-free (`std::net` only) so it's cheap to invoke from
//! the container runtime every few seconds — no tokio runtime spin-up,
//! no TLS handshake, no allocator arena init beyond what main.rs has
//! already done.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::Duration;

/// Default HTTP API port (matches `EXPOSE 6480` in the Dockerfile).
const DEFAULT_PORT: u16 = 6480;
/// Connect + read timeout. Tight: a healthy node responds in microseconds.
const TIMEOUT: Duration = Duration::from_secs(2);

/// Run the healthcheck. Returns process exit code:
/// - `0` — `/healthz` returned 2xx
/// - `1` — connection / I/O / non-2xx
pub fn run(port: u16) -> i32 {
    match probe(port) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("healthcheck failed: {e}");
            1
        }
    }
}

fn probe(port: u16) -> Result<(), String> {
    let addr = format!("127.0.0.1:{port}");
    let mut stream = TcpStream::connect_timeout(
        &addr.parse().map_err(|e| format!("bad addr: {e}"))?,
        TIMEOUT,
    )
    .map_err(|e| format!("connect {addr}: {e}"))?;

    stream.set_read_timeout(Some(TIMEOUT)).ok();
    stream.set_write_timeout(Some(TIMEOUT)).ok();

    let req = b"GET /healthz HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(req).map_err(|e| format!("write: {e}"))?;
    stream.flush().ok();

    // Read just enough to see the status line.
    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).map_err(|e| format!("read: {e}"))?;
    let _ = stream.shutdown(Shutdown::Both);

    if n < 12 {
        return Err(format!("short response: {n} bytes"));
    }
    // Status line: "HTTP/1.x SSS ..."
    let status = std::str::from_utf8(&buf[9..12]).map_err(|_| "non-utf8 status".to_string())?;
    let code: u16 = status
        .parse()
        .map_err(|_| format!("bad status: {status:?}"))?;
    if (200..300).contains(&code) {
        Ok(())
    } else {
        Err(format!("status {code}"))
    }
}

/// Parse `--port N` (optional, defaults to [`DEFAULT_PORT`]).
pub fn parse_port(args: &[String]) -> Result<u16, String> {
    let flags = super::args::parse_flags(args)?;
    match flags.get("port") {
        Some(s) => s
            .parse::<u16>()
            .map_err(|_| "--port must be a u16".to_string()),
        None => Ok(DEFAULT_PORT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_port_default() {
        assert_eq!(parse_port(&[]).unwrap(), DEFAULT_PORT);
    }

    #[test]
    fn parse_port_explicit() {
        let args = vec!["--port".to_string(), "9999".to_string()];
        assert_eq!(parse_port(&args).unwrap(), 9999);
    }

    #[test]
    fn parse_port_invalid() {
        let args = vec!["--port".to_string(), "not-a-number".to_string()];
        assert!(parse_port(&args).is_err());
    }

    #[test]
    fn probe_unreachable_port_errors() {
        // Port 1 is virtually never bound; expect a connect error.
        assert!(probe(1).is_err());
    }
}
