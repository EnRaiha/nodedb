// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

pub(super) async fn dispatch(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    _parts: &[&str],
    _database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // NDB_CHUNK_TEXT table-valued function has been migrated to the
    // protocol-neutral router (`shared::ddl::neutral::chunk_text`), which is
    // tried before this transitional pgwire delegation runs.

    // CRDT DSL functions (`SELECT crdt_state(...)` / `SELECT crdt_apply(...)`)
    // and `MATCH` pattern queries have been migrated to the protocol-neutral
    // router (`shared::ddl::neutral::crdt_ops` / `match_ops`), which is tried
    // before this transitional pgwire delegation runs. Graph parse errors for
    // `GRAPH ` / `MATCH ` / `SHOW GRAPH STATS` prefixed inputs are reproduced
    // here so those inputs still surface a 42601 rather than falling through to
    // the SQL planner.
    if upper.starts_with("GRAPH ")
        || upper.starts_with("MATCH ")
        || upper.starts_with("OPTIONAL MATCH ")
        || upper.starts_with("SHOW GRAPH STATS")
    {
        match nodedb_sql::ddl_ast::parse(sql) {
            Some(Err(e)) => {
                return Some(Err(super::super::super::types::sqlstate_error(
                    "42601",
                    &e.to_string(),
                )));
            }
            Some(Ok(_)) => {}
            None => {}
        }
    }

    // Bulk import (`COPY <collection> FROM STDIN [WITH (...)]`) has been migrated
    // to the protocol-neutral router (`shared::ddl::neutral::bulk`), which is
    // tried before this transitional pgwire delegation runs.

    // INSERT INTO x { } (object literal) and UPSERT INTO (both VALUES and { }
    // object literal forms) have been migrated to the protocol-neutral router
    // (`shared::ddl::neutral::collection::dml`), which is tried before this
    // transitional pgwire delegation runs.

    // SHOW CHANGES FOR <collection> has been migrated to the protocol-neutral
    // router (`shared::ddl::neutral::show_changes`), which is tried before this
    // transitional pgwire delegation runs.

    // ESTIMATE_COUNT('collection', 'field') has been migrated to the
    // protocol-neutral router (`shared::ddl::neutral::estimate_count`), which is
    // tried before this transitional pgwire delegation runs.

    // TRUNCATE — handled by SQL planner (plan_truncate_stmt → DocumentOp::Truncate).

    None
}
