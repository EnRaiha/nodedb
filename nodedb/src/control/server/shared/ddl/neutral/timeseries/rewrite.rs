// SPDX-License-Identifier: BUSL-1.1

//! `REWRITE PARTITIONS FOR <name>`
//!
//! The Data Plane core that owns the collection rewrites each sealed or merged
//! partition with the collection's current schema and codecs, dropping every
//! column the schema no longer holds. The statement returns once every
//! partition is rewritten, and reports how many were.

use nodedb_physical::physical_plan::{PartitionsAction, TimeseriesOp};
use nodedb_types::DatabaseId;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{ddl_err, require_timeseries};

/// The count the Data Plane reports.
#[derive(Debug, Default, serde::Deserialize)]
struct Rewritten {
    affected: u64,
}

/// REWRITE PARTITIONS FOR <name>
pub async fn rewrite_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 || !parts[2].eq_ignore_ascii_case("FOR") {
        return Err(ddl_err(
            "42601",
            "syntax: REWRITE PARTITIONS FOR <timeseries>",
        ));
    }
    let name = parse_ident_token(parts[3])?;
    let tenant_id = identity.tenant_id;
    require_timeseries(state, database_id, tenant_id, &name)?;

    // A rewrite changes the collection's storage layout: an ALTER of it.
    let gate = CollectionReadGate::for_request(state, identity, database_id);
    gate.authorize_permission(&name, Permission::Alter)?;
    let mut plan = PhysicalPlan::Timeseries(TimeseriesOp::Partitions {
        collection: nodedb_types::QualifiedCollection::new(database_id, &name),
        action: PartitionsAction::Rewrite,
    });
    gate.inject_rls(&mut plan)?;

    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &name).vshard();
    let response = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        plan,
        TraceId::generate(),
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;
    let rewritten: Rewritten =
        crate::data::executor::response_codec::decode_payload(&response.payload)
            .map_err(|e| DdlError::from_error(&e))?;

    tracing::info!(
        collection = name,
        rewritten = rewritten.affected,
        "REWRITE PARTITIONS completed"
    );
    Ok(vec![DdlResult::Status {
        command: "REWRITE PARTITIONS".to_string(),
        rows_affected: Some(rewritten.affected),
    }])
}
