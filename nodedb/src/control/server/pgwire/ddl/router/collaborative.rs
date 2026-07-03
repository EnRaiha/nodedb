// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
    _database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // Pub/Sub: SUBSCRIBE TO (legacy).
    if upper.starts_with("SUBSCRIBE TO ") {
        return Some(super::super::pubsub::subscribe_to(
            state, identity, sql, parts,
        ));
    }

    // Period lock management.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD PERIOD LOCK") {
        return Some(super::super::period_lock::add_period_lock(
            state, identity, sql,
        ));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP PERIOD LOCK") {
        return Some(super::super::period_lock::drop_period_lock(
            state, identity, parts,
        ));
    }

    // Permission tree management (ALTER COLLECTION … SET/DROP PERMISSION_TREE)
    // has been migrated to the protocol-neutral router
    // (`shared::ddl::neutral::permission_tree`), which is tried before this
    // transitional pgwire delegation runs.

    None
}
