// SPDX-License-Identifier: BUSL-1.1

//! `SHOW PARTITIONS FOR <name>`
//!
//! The router sends `SHOW PARTITIONS FOR` here and `SHOW PARTITIONS ON` to the
//! consumer-group handler. The partitions live in the registry of the Data
//! Plane core that owns the collection, so the listing is read there.

use nodedb_physical::physical_plan::{PartitionsAction, TimeseriesOp};
use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::engine::timeseries::partition_registry::PartitionListing;
use crate::types::TraceId;
use nodedb_types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{ddl_err, format_bytes, require_timeseries};

/// SHOW PARTITIONS FOR <name>
pub async fn show_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 || !parts[2].eq_ignore_ascii_case("FOR") {
        return Err(ddl_err("42601", "syntax: SHOW PARTITIONS FOR <timeseries>"));
    }
    let name = parse_ident_token(parts[3])?;
    let tenant_id = identity.tenant_id;
    require_timeseries(state, database_id, tenant_id, &name)?;

    let gate = CollectionReadGate::open(state, identity, database_id, &name)?;
    let mut plan = PhysicalPlan::Timeseries(TimeseriesOp::Partitions {
        collection: nodedb_types::QualifiedCollection::new(database_id, &name),
        action: PartitionsAction::List,
    });
    // A read policy cannot narrow a listing of every row's partitions, so the
    // injection pass refuses the plan under one.
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
    let listing: Vec<PartitionListing> =
        crate::data::executor::response_codec::decode_payload(&response.payload)
            .map_err(|e| DdlError::from_error(&e))?;

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        COLUMNS
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect(),
        COLUMNS.iter().map(|(_, ty)| *ty).collect(),
        listing.iter().map(listing_row).collect(),
    ))])
}

/// The result columns, in order.
const COLUMNS: [(&str, DdlColType); 7] = [
    ("partition", DdlColType::Text),
    ("state", DdlColType::Text),
    ("min_ts", DdlColType::Int8),
    ("max_ts", DdlColType::Int8),
    ("rows", DdlColType::Int8),
    ("size", DdlColType::Text),
    ("interval_ms", DdlColType::Int8),
];

/// One result row for `listing`.
fn listing_row(listing: &PartitionListing) -> Map<String, JsonValue> {
    let mut row = Map::new();
    row.insert(
        "partition".to_string(),
        JsonValue::String(listing.name.clone()),
    );
    row.insert(
        "state".to_string(),
        JsonValue::String(listing.state.clone()),
    );
    row.insert(
        "min_ts".to_string(),
        JsonValue::String(listing.min_ts.to_string()),
    );
    row.insert(
        "max_ts".to_string(),
        JsonValue::String(listing.max_ts.to_string()),
    );
    row.insert(
        "rows".to_string(),
        JsonValue::String(listing.row_count.to_string()),
    );
    row.insert(
        "size".to_string(),
        JsonValue::String(format_bytes(listing.size_bytes)),
    );
    row.insert(
        "interval_ms".to_string(),
        JsonValue::String(listing.interval_ms.to_string()),
    );
    row
}
