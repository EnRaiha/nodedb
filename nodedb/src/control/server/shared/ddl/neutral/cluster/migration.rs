// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral migration DDL command: SHOW MIGRATIONS. It reads the
//! migration tracker `wire_cluster_handle` installs, which the rebalancer's
//! migration executor reports to.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// SHOW MIGRATIONS — list active and recent migrations.
///
/// Superuser only.
pub fn show_migrations(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view migrations",
        ));
    }

    let tracker = state.migration_tracker.as_ref().ok_or_else(|| {
        ddl_err(
            "XX000",
            "the migration tracker is not installed: this node's cluster is not wired",
        )
    })?;

    let snapshots = tracker.snapshot();

    let columns = vec![
        "vshard_id".to_string(),
        "phase".to_string(),
        "elapsed_ms".to_string(),
        "active".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    let mut rows = Vec::new();
    for s in &snapshots {
        let active_str = if s.is_active { "yes" } else { "no" };

        let mut row = Map::new();
        row.insert(
            "vshard_id".to_string(),
            JsonValue::String((s.vshard_id as i64).to_string()),
        );
        row.insert("phase".to_string(), JsonValue::String(s.phase.clone()));
        row.insert(
            "elapsed_ms".to_string(),
            JsonValue::String((s.elapsed_ms as i64).to_string()),
        );
        row.insert(
            "active".to_string(),
            JsonValue::String(active_str.to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::cluster::test_one_node;
    use crate::control::security::identity::{DatabaseSet, Role};
    use crate::types::TenantId;

    fn superuser() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            0,
            "show_migrations_test",
            TenantId::new(1),
            vec![Role::Superuser],
            true,
            None,
            DatabaseSet::All,
        )
    }

    fn row_count(results: &[DdlResult]) -> usize {
        match results {
            [DdlResult::Rows(shaped)] => shaped.rows.len(),
            _ => panic!("SHOW MIGRATIONS returns one row set"),
        }
    }

    /// A booted node's tracker is the one its migration executor reports
    /// to, and SHOW MIGRATIONS lists what it holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn show_migrations_lists_the_executors_migrations() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let identity = superuser();

        let empty = show_migrations(state, &identity).expect("SHOW MIGRATIONS on a booted node");
        assert_eq!(row_count(&empty), 0);

        let mut migration = nodedb_cluster::MigrationState::new(7, 1, 1, 1, 2, 500_000);
        migration.start_base_copy(10);
        state
            .migration_tracker
            .as_ref()
            .expect("wire_cluster_handle installs the tracker")
            .record(uuid::Uuid::new_v4(), &migration);

        let listed = show_migrations(state, &identity).expect("SHOW MIGRATIONS");
        assert_eq!(row_count(&listed), 1);
        cluster.shutdown().await;
    }
}
