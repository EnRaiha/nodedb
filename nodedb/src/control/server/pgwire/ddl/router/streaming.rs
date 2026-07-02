// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // Stream consumption: SELECT * FROM STREAM <name> CONSUMER GROUP <group>
    if upper.starts_with("SELECT ")
        && upper.contains("FROM STREAM ")
        && upper.contains("CONSUMER GROUP")
    {
        return Some(super::super::stream_select::select_from_stream(state, identity, parts).await);
    }

    // Streaming materialized views: CREATE MATERIALIZED VIEW ... STREAMING AS ...
    // CREATE MATERIALIZED VIEW (including STREAMING mode) is fully dispatched via typed AST (ast.rs).

    // Topics (CREATE / DROP / SHOW TOPIC + PUBLISH TO) are served by the
    // protocol-neutral DDL router, which runs before this pgwire router.

    // Stream/Topic consumption: SELECT * FROM STREAM/TOPIC ... CONSUMER GROUP ...
    if upper.starts_with("SELECT ")
        && upper.contains("FROM TOPIC ")
        && upper.contains("CONSUMER GROUP")
    {
        // Rewrite: topics use "topic:<name>" buffer keys.
        // The stream_select handler works for both — we just need to prefix the name.
        return Some(super::helpers::select_from_topic(state, identity, parts).await);
    }

    None
}
