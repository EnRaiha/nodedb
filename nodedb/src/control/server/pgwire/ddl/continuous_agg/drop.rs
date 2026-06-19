// SPDX-License-Identifier: BUSL-1.1

//! `DROP CONTINUOUS AGGREGATE` handler.

use std::time::Duration;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::{catalog_propose, sync_dispatch};
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

/// `DROP CONTINUOUS AGGREGATE <name>`.
///
/// `parts` is the whitespace-tokenised statement; positions 0..=2 are
/// `["DROP", "CONTINUOUS", "AGGREGATE"]` and position 3 is the name.
pub async fn drop_continuous_aggregate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP CONTINUOUS AGGREGATE <name>",
        ));
    }

    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id;

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteContinuousAggregate {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
    };
    let log_index = catalog_propose::propose_and_apply(state, &entry)?;

    // Single-node / no-applier path: mirror the unregister dispatch the
    // raft-applier path would have done so the local manager forgets the
    // aggregate immediately.
    if log_index == 0 {
        let plan = PhysicalPlan::Meta(MetaOp::UnregisterContinuousAggregate { name: name.clone() });
        sync_dispatch::dispatch_async(
            state,
            tenant_id,
            database_id,
            &name,
            plan,
            Duration::from_secs(5),
        )
        .await
        .map_err(|e| sqlstate_error("XX000", &format!("dispatch failed: {e}")))?;
    }

    tracing::info!(name, "continuous aggregate dropped");

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "DROP CONTINUOUS AGGREGATE",
    ))])
}
