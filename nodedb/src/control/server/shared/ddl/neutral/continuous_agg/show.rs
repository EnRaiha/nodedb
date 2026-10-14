// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW CONTINUOUS AGGREGATES [FOR <source>]` handler.
//!
//! The catalog read (durable source of truth), the best-effort runtime-stats
//! merge from every local Data Plane core, the optional `FOR <source>`
//! filter, the decode-failure skip, and the exact column set run here. The
//! result is the protocol-neutral [`DdlResult::Rows`] over [`ShapedRows`].
//! The mixed text/`int8` column OIDs (`watermark_ts`, `rows_aggregated`,
//! `materialized_buckets` are `int8`) come from building `column_types`
//! manually; the `int8` cells are emitted as their decimal text form.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::StoredContinuousAggregate;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::{AggregateInfo, ContinuousAggregateDef};
use crate::engine::timeseries::retention_policy::autowire::tier_definitions;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::result::{DdlError, DdlResult};

/// `SHOW CONTINUOUS AGGREGATES [FOR <source>]`.
///
/// Reads the catalog (the source of truth: replicated, persisted,
/// survives restart), adds the downsample tiers of the database's retention
/// policies, and merges in best-effort runtime stats from the
/// local Data Plane manager. A node that has just restarted but hasn't
/// finished replaying registers will still show the aggregate via the
/// catalog row; the runtime columns surface as zero / blank until the
/// manager catches up.
pub async fn show_continuous_aggregates(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let source_filter = if parts.len() >= 5 && parts[3].to_uppercase() == "FOR" {
        Some(parse_ident_token(parts[4])?)
    } else {
        None
    };

    let tenant_id = identity.tenant_id;

    // Catalog rows — the durable source of truth.
    let stored_aggs: Vec<StoredContinuousAggregate> = state
        .credentials
        .catalog()
        .list_continuous_aggregates(database_id.as_u64(), tenant_id.as_u64())
        .ok()
        .unwrap_or_default();

    // Best-effort runtime stats from the local manager.
    //
    // "Best effort" covers the dispatch not answering — a node still replaying
    // registers has no stats to give, and the catalog rows below carry the
    // listing regardless. It does NOT cover a payload that arrived and could
    // not be read: `MetaOp::ListContinuousAggregates` encodes with
    // `response_codec::encode_serde`, which is MessagePack, so a JSON parser
    // here would fail on every one of those payloads and default the failure
    // away — reporting every aggregate at watermark 0, zero rows
    // aggregated, zero materialized buckets, and the catalog's stale flag
    // instead of the live one, on every node, always.
    //
    // Every core registers every aggregate, but only the core that owns an
    // aggregate's root source folds it. So the listing asks every local core
    // and keeps, per aggregate, the core that folded the most rows.
    let per_core: Vec<AggregateInfo> =
        match crate::control::server::exchange::execute_plan_all_local_cores(
            state,
            tenant_id,
            database_id,
            PhysicalPlan::Meta(MetaOp::ListContinuousAggregates),
            crate::types::TraceId::ZERO,
            None,
        )
        .await
        {
            Ok(result) => crate::data::executor::response_codec::decode_payload(&result.payload)
                .map_err(|e| {
                    DdlError::from_error_in_context("continuous aggregate runtime stats", &e)
                })?,
            Err(_) => Vec::new(),
        };
    let runtime_infos = merge_by_name(per_core);

    let columns = vec![
        "name".to_string(),
        "source".to_string(),
        "bucket_interval".to_string(),
        "refresh_policy".to_string(),
        "watermark_ts".to_string(),
        "rows_aggregated".to_string(),
        "materialized_buckets".to_string(),
        "stale".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    // Decode the catalog-stored runtime def for the static columns (bucket
    // interval, refresh policy). Skip the row on a decode failure rather
    // than poisoning the whole listing.
    let mut listed: Vec<(String, String, ContinuousAggregateDef)> = Vec::new();
    for stored in &stored_aggs {
        let Ok(def) = zerompk::from_msgpack::<ContinuousAggregateDef>(&stored.def_bytes) else {
            tracing::warn!(
                cagg = %stored.name,
                tenant = stored.tenant_id,
                "continuous aggregate row has unreadable def_bytes; \
                 skipping in SHOW (the row is still durable in the catalog)"
            );
            continue;
        };
        listed.push((stored.name.clone(), stored.source.clone(), def));
    }
    // The downsample tiers of every retention policy in this database are
    // continuous aggregates too. Their stored policy defines them, as boot
    // and policy apply register them.
    for policy in state
        .retention_policy_registry
        .list_for_tenant_in_database(database_id.as_u64(), tenant_id.as_u64())
    {
        for def in tier_definitions(&policy) {
            listed.push((def.name.clone(), def.source.clone(), def));
        }
    }

    let mut rows = Vec::new();
    for (name, source, def) in &listed {
        if let Some(ref filter) = source_filter
            && source != filter
        {
            continue;
        }

        let runtime = runtime_infos.iter().find(|i| i.name == *name);
        let watermark = runtime.map(|i| i.watermark_ts).unwrap_or(0);
        let rows_agg = runtime.map(|i| i.rows_aggregated as i64).unwrap_or(0);
        let buckets = runtime.map(|i| i.materialized_buckets as i64).unwrap_or(0);
        let stale = runtime.map(|i| i.stale).unwrap_or(def.stale).to_string();

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(name.clone()));
        row.insert("source".to_string(), JsonValue::String(source.clone()));
        row.insert(
            "bucket_interval".to_string(),
            JsonValue::String(def.bucket_interval.clone()),
        );
        row.insert(
            "refresh_policy".to_string(),
            JsonValue::String(format!("{:?}", def.refresh_policy)),
        );
        row.insert(
            "watermark_ts".to_string(),
            JsonValue::String(watermark.to_string()),
        );
        row.insert(
            "rows_aggregated".to_string(),
            JsonValue::String(rows_agg.to_string()),
        );
        row.insert(
            "materialized_buckets".to_string(),
            JsonValue::String(buckets.to_string()),
        );
        row.insert("stale".to_string(), JsonValue::String(stale));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// One entry per aggregate name: the core that folded the most rows, which
/// is the core that owns the aggregate's root source. A stale flag on any
/// core marks the aggregate stale.
fn merge_by_name(per_core: Vec<AggregateInfo>) -> Vec<AggregateInfo> {
    let mut merged: Vec<AggregateInfo> = Vec::new();
    for info in per_core {
        match merged.iter_mut().find(|m| m.name == info.name) {
            Some(held) => {
                let stale = held.stale || info.stale;
                if (info.rows_aggregated, info.materialized_buckets)
                    > (held.rows_aggregated, held.materialized_buckets)
                {
                    *held = info;
                }
                held.stale = stale;
            }
            None => merged.push(info),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::RefreshPolicy;

    fn info(name: &str, rows: u64, stale: bool) -> AggregateInfo {
        AggregateInfo {
            name: name.into(),
            source: "metrics".into(),
            bucket_interval: "1m".into(),
            refresh_policy: RefreshPolicy::OnFlush,
            watermark_ts: i64::from(rows > 0),
            rows_aggregated: rows,
            materialized_buckets: rows,
            stale,
        }
    }

    #[test]
    fn the_owning_core_wins_the_merge() {
        let merged = merge_by_name(vec![
            info("a", 0, false),
            info("a", 7, false),
            info("a", 0, true),
            info("b", 0, false),
        ]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].rows_aggregated, 7);
        assert!(merged[0].stale, "a stale core marks the aggregate stale");
        assert_eq!(merged[1].name, "b");
    }
}
