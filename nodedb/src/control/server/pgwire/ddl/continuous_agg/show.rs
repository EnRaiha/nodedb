// SPDX-License-Identifier: BUSL-1.1

//! `SHOW CONTINUOUS AGGREGATES [FOR <source>]` handler.

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::StoredContinuousAggregate;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::sync_dispatch;
use crate::control::server::pgwire::types::{int8_field, sqlstate_error, text_field};
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::{AggregateInfo, ContinuousAggregateDef};
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

/// `SHOW CONTINUOUS AGGREGATES [FOR <source>]`.
///
/// Reads the catalog (the source of truth: replicated, persisted,
/// survives restart) and merges in best-effort runtime stats from the
/// local Data Plane manager. A node that has just restarted but hasn't
/// finished replaying registers will still show the aggregate via the
/// catalog row; the runtime columns surface as zero / blank until the
/// manager catches up.
pub async fn show_continuous_aggregates(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let source_filter = if parts.len() >= 5 && parts[3].to_uppercase() == "FOR" {
        Some(parts[4].to_lowercase())
    } else {
        None
    };

    let tenant_id = identity.tenant_id;

    // Catalog rows — the durable source of truth.
    let stored_aggs: Vec<StoredContinuousAggregate> = state
        .credentials
        .catalog()
        .as_ref()
        .and_then(|catalog| {
            catalog
                .list_continuous_aggregates(database_id.as_u64(), tenant_id.as_u64())
                .ok()
        })
        .unwrap_or_default();

    // Best-effort runtime stats from the local manager.
    let runtime_infos: Vec<AggregateInfo> = match sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        "__system",
        PhysicalPlan::Meta(MetaOp::ListContinuousAggregates),
        Duration::from_secs(5),
    )
    .await
    {
        Ok(payload) => sonic_rs::from_slice(&payload).unwrap_or_default(),
        Err(_) => Vec::new(),
    };

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("source"),
        text_field("bucket_interval"),
        text_field("refresh_policy"),
        int8_field("watermark_ts"),
        int8_field("rows_aggregated"),
        int8_field("materialized_buckets"),
        text_field("stale"),
    ]);

    let mut rows = Vec::new();
    for stored in &stored_aggs {
        if let Some(ref filter) = source_filter
            && stored.source != *filter
        {
            continue;
        }

        // Decode the catalog-stored runtime def for the static columns
        // (bucket interval, refresh policy). Skip the row on a decode
        // failure rather than poisoning the whole listing.
        let Ok(def) = zerompk::from_msgpack::<ContinuousAggregateDef>(&stored.def_bytes) else {
            tracing::warn!(
                cagg = %stored.name,
                tenant = stored.tenant_id,
                "continuous aggregate row has unreadable def_bytes; \
                 skipping in SHOW (the row is still durable in the catalog)"
            );
            continue;
        };

        let runtime = runtime_infos.iter().find(|i| i.name == stored.name);
        let watermark = runtime.map(|i| i.watermark_ts).unwrap_or(0);
        let rows_agg = runtime.map(|i| i.rows_aggregated as i64).unwrap_or(0);
        let buckets = runtime.map(|i| i.materialized_buckets as i64).unwrap_or(0);
        let stale = runtime.map(|i| i.stale).unwrap_or(def.stale).to_string();

        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&stored.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&stored.source)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&def.bucket_interval)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&format!("{:?}", def.refresh_policy))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&watermark)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&rows_agg)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&buckets)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&stale)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
