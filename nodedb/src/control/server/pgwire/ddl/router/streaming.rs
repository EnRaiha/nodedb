// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // Consumer Groups: CREATE/DROP/SHOW CONSUMER GROUP + COMMIT OFFSET(S)
    // CREATE CONSUMER GROUP is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP CONSUMER GROUP ") {
        return Some(super::super::consumer_group::drop_consumer_group(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW CONSUMER GROUPS ") {
        return Some(super::super::consumer_group::show_consumer_groups(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW PARTITIONS ") {
        return Some(super::super::consumer_group::show_partitions(
            state, identity, parts,
        ));
    }
    if upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ") {
        return Some(super::super::consumer_group::commit_offset(
            state, identity, parts,
        ));
    }

    // Stream consumption: SELECT * FROM STREAM <name> CONSUMER GROUP <group>
    if upper.starts_with("SELECT ")
        && upper.contains("FROM STREAM ")
        && upper.contains("CONSUMER GROUP")
    {
        return Some(super::super::stream_select::select_from_stream(state, identity, parts).await);
    }

    // Streaming materialized views: CREATE MATERIALIZED VIEW ... STREAMING AS ...
    // CREATE MATERIALIZED VIEW (including STREAMING mode) is fully dispatched via typed AST (ast.rs).

    // Topics: CREATE/DROP/SHOW TOPIC + PUBLISH TO
    if upper.starts_with("CREATE TOPIC ") {
        return Some(super::super::topic::create_topic(
            state, identity, parts, sql,
        ));
    }
    if upper.starts_with("DROP TOPIC ") {
        return Some(super::super::topic::drop_topic(state, identity, parts));
    }
    if upper.starts_with("SHOW TOPIC") {
        return Some(super::super::topic::show_topics(state, identity));
    }
    if upper.starts_with("PUBLISH TO ") {
        return Some(super::super::topic::handle_publish(state, identity, sql).await);
    }

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
