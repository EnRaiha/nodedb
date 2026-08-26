// SPDX-License-Identifier: BUSL-1.1

//! Wire-level coverage for the session-handle resolver's missing hygiene
//! layer: rate limiting + visibility on miss. `SET LOCAL
//! nodedb.auth_session = '<handle>'` currently resolves with no
//! per-connection throttle and no observable signal on miss. Acceptance: 100
//! failed calls must close the connection with a pgwire error and record an
//! audit event, encoded here with no assumption about internal API shape.

use crate::harness::TestServer;

#[tokio::test]
async fn set_local_auth_session_flood_closes_connection() {
    let server = TestServer::start().await;

    // 100 distinct bogus handles so each is a genuine resolve-miss, not a
    // short-circuit on a repeated value.
    let mut closed = false;
    let mut attempts_before_close = 0usize;
    for i in 0..100 {
        attempts_before_close = i + 1;
        let sql = format!("SET LOCAL nodedb.auth_session = 'nds_bogus_{i:032x}'");
        if server.client.simple_query(&sql).await.is_err() {
            closed = true;
            break;
        }
    }

    assert!(
        closed,
        "server accepted {attempts_before_close} consecutive failed \
         `SET LOCAL nodedb.auth_session` calls on one connection without \
         throttling or error — the resolver is unthrottled and unobservable. \
         Expected: connection closed with a pgwire error well before 100 \
         attempts (20/min default)"
    );
}
