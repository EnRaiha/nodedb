// SPDX-License-Identifier: BUSL-1.1

//! The ILP ingest path's admission gate must see the client's real socket
//! address.
//!
//! `flush_authenticated_ilp_batch` resolves a request scope and runs
//! `check_blacklist_and_status` before a single line is parsed. Both halves of
//! that gate — the IP blacklist and the adaptive-auth risk stamp — parse the
//! address they are handed and ignore anything that is not one. A scope built
//! without the accepted socket's address, or with a fixed transport label in
//! its place, therefore disables the blacklist and silently withholds every
//! `REQUIRE IP` grant across the whole ingest surface (native ILP, OTLP, and
//! PromQL remote write all enter through this function) while still appearing
//! to call the gate.
//!
//! The blacklist is the observable half. An ILP connection sends no per-line
//! acknowledgement, but a refused batch IS observable without polling: the
//! handler returns on the failed flush and drops the stream, so the client
//! sees EOF. Keying on that rather than on whether a row arrived is what makes
//! these deterministic — an "the row landed within N seconds" poll measures
//! ingest throughput, and under a loaded suite it reports starvation as a
//! policy failure. One test below still asserts row arrival, because something
//! has to prove ingest works at all; the two that assert the *policy* do not.
//!
//! Two details make the assertion precise:
//!
//! - The ILP client binds its source to `127.0.0.2` while the admin session
//!   stays on `127.0.0.1`. Loopback is a `/8`, so both are the same host but
//!   distinct addresses — the ban can name the ingest client exactly, and the
//!   session that must observe the outcome is never caught by it. A test that
//!   banned all of `127.0.0.0/8` would lock itself out of its own verification
//!   query.
//! - The connection authenticates BEFORE the ban is placed, so what is
//!   measured is the per-batch admission gate rather than the connection's
//!   authentication prelude.
//!
//! This runs against the real server binary rather than the in-process
//! harness: ILP ingest is a Calvin-sequenced write, which the single-process
//! harness does not run.

mod crash_harness;

use std::time::Duration;

use crash_harness::CrashHarness;
use nodedb_test_support::ilp_client;

/// Long enough for the server's adaptive batch timer to flush a single line
/// many times over, so "the row never arrived" cannot just mean "not yet".
const INGEST_WAIT: Duration = Duration::from_secs(15);

/// How long to wait for the server to close a connection whose batch it
/// refused. Generous, because it bounds a failure rather than a success: the
/// close follows the refusal immediately, so overshooting costs nothing on the
/// passing path.
const CLOSE_WAIT: Duration = Duration::from_secs(10);

/// How long an admitted connection must stay open to count as not-refused.
/// Unlike an ingest poll this does not depend on throughput — an idle open
/// socket stays open however loaded the machine is.
const STAY_OPEN_WAIT: Duration = Duration::from_secs(2);

/// The ILP prelude is a native `Auth` frame, so ingest needs a real
/// credential of its own.
const INGEST_USER: &str = "ilp_address_user";
const INGEST_PASSWORD: &str = "ilp-address-secret-1";

/// The source address the ILP client binds, distinct from the `127.0.0.1` the
/// admin pgwire session uses.
const INGEST_SOURCE: &str = "127.0.0.2:0";

/// Boot a server with the ILP listener enabled and a collection to ingest
/// into, and wait until a Calvin-routed write can actually succeed.
async fn start_ingest_server(collection: &str) -> CrashHarness {
    let mut harness = CrashHarness::new();
    harness.spawn();
    harness.wait_ready(Duration::from_secs(20));
    harness.wait_for_calvin_ready(Duration::from_secs(20)).await;

    harness
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await;
    harness
        .exec(&format!(
            "CREATE USER {INGEST_USER} PASSWORD '{INGEST_PASSWORD}'"
        ))
        .await;
    harness
        .exec(&format!("GRANT ROLE readwrite TO {INGEST_USER}"))
        .await;

    harness
}

/// Authenticate an ILP connection sourced from [`INGEST_SOURCE`].
async fn connect_ingest(harness: &CrashHarness) -> tokio::net::TcpStream {
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", harness.ilp_port)
        .parse()
        .expect("loopback ILP address must parse");
    ilp_client::connect_and_auth_from(
        Some(INGEST_SOURCE.parse().expect("ingest source must parse")),
        addr,
        INGEST_USER,
        INGEST_PASSWORD,
    )
    .await
}

fn ilp_line(collection: &str, value: f64) -> String {
    let ts_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    format!("{collection},metric=cpu value={value} {ts_ns}")
}

/// Poll until `collection` holds a row, reporting whether it ever did within
/// [`INGEST_WAIT`].
async fn row_arrives(harness: &CrashHarness, collection: &str) -> bool {
    let session = harness.connect().await;
    let deadline = std::time::Instant::now() + INGEST_WAIT;
    loop {
        let counted = session
            .try_query_col_idx(&format!("SELECT COUNT(*) FROM {collection}"), 0)
            .await
            .ok()
            .and_then(|rows| rows.first().cloned());
        if counted.is_some_and(|count| count != "0") {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Whether the server closed its side of `stream` within `wait`.
///
/// A refused batch is fatal to the connection that carried it, so this is the
/// direct, poll-free signal that the admission gate rejected the sender.
async fn server_closed(stream: &mut tokio::net::TcpStream, wait: Duration) -> bool {
    use tokio::io::AsyncReadExt;
    let mut sink = Vec::new();
    matches!(
        tokio::time::timeout(wait, stream.read_to_end(&mut sink)).await,
        Ok(Ok(_))
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_client_ip_cannot_keep_ingesting_over_ilp() {
    let harness = start_ingest_server("ilp_blacklist_rows").await;
    let mut stream = connect_ingest(&harness).await;

    harness
        .exec("BLACKLIST IP '127.0.0.2' REASON 'test ban'")
        .await;

    ilp_client::send_line(&mut stream, &ilp_line("ilp_blacklist_rows", 42.5)).await;

    assert!(
        server_closed(&mut stream, CLOSE_WAIT).await,
        "an ILP batch from a blacklisted client IP must be refused, which drops the \
         connection; it stayed open, so the gate was handed no usable peer address"
    );
}

/// Regression guard for the fix above: threading the real address must not
/// turn every ILP batch into a refusal. The ban here names a different
/// address, so this ingest is the same shape as the refused one and differs
/// only in whether the client's own address matches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_ip_outside_the_blacklisted_range_is_not_refused_over_ilp() {
    let harness = start_ingest_server("ilp_allowed_rows").await;
    let mut stream = connect_ingest(&harness).await;

    harness
        .exec("BLACKLIST IP '10.0.0.0/8' REASON 'test ban'")
        .await;

    ilp_client::send_line(&mut stream, &ilp_line("ilp_allowed_rows", 7.25)).await;

    assert!(
        !server_closed(&mut stream, STAY_OPEN_WAIT).await,
        "a client outside the blacklisted range must not be refused; the server \
         dropped the connection, so threading the real address over-refuses"
    );
}

/// A ban must be liftable. `BLACKLIST IP` persists to the system catalog and
/// is reloaded at boot, so without a working removal command an operator who
/// bans a range has no way back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifting_a_ban_restores_ingest() {
    let harness = start_ingest_server("ilp_lifted_rows").await;
    let mut stream = connect_ingest(&harness).await;

    harness
        .exec("BLACKLIST IP '127.0.0.2' REASON 'test ban'")
        .await;
    ilp_client::send_line(&mut stream, &ilp_line("ilp_lifted_rows", 1.5)).await;
    assert!(
        server_closed(&mut stream, CLOSE_WAIT).await,
        "the ban must take effect before the lift is meaningful"
    );

    harness.exec("UNBLACKLIST IP '127.0.0.2'").await;

    // The refusal above already dropped the connection, so resuming means
    // dialling again — exactly what a real client does once its ban is lifted.
    drop(stream);
    let mut stream = connect_ingest(&harness).await;
    ilp_client::send_line(&mut stream, &ilp_line("ilp_lifted_rows", 2.5)).await;
    assert!(
        row_arrives(&harness, "ilp_lifted_rows").await,
        "ingest must resume once the ban on the client's address is lifted"
    );
    drop(stream);
}
