// SPDX-License-Identifier: BUSL-1.1

//! Aggregate result-cache key derivation.

use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};

/// Serialize complete group-key specs into a deterministic structural key.
/// Computed keys must include their expression; `field` is intentionally empty
/// for those keys and is not sufficient cache identity.
fn group_specs_key(group_by: &[GroupKeySpec]) -> String {
    group_by
        .iter()
        .map(|spec| match zerompk::to_msgpack_vec(spec) {
            Ok(bytes) => bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
            Err(_) => format!("{spec:?}"),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn expression_key(expr: &nodedb_query::expr::SqlExpr) -> String {
    match zerompk::to_msgpack_vec(expr) {
        Ok(bytes) => bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
        Err(_) => format!("{expr:?}"),
    }
}

fn aggregate_specs_key(aggregates: &[AggregateSpec]) -> String {
    aggregates
        .iter()
        .map(|agg| {
            let input = agg
                .expr
                .as_ref()
                .map(expression_key)
                .unwrap_or_else(|| agg.field.clone());
            format!(
                "{}({})->{}=>{}",
                agg.function,
                input,
                agg.alias,
                agg.user_alias.as_deref().unwrap_or(&agg.alias)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Complete query shape that constitutes an aggregate result's cache identity.
/// Every field participates in the key — two aggregate queries that differ in
/// any one of them (output aliases, `LIMIT`, `ORDER BY`, sub-aggregation) must
/// not collide on a shared cache entry.
pub(super) struct AggregateCacheKeyInputs<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub sub_group_by: &'a [String],
    pub sub_aggregates: &'a [AggregateSpec],
    pub limit: usize,
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
}

pub(super) fn aggregate_cache_key(
    inputs: AggregateCacheKeyInputs<'_>,
) -> (crate::types::DatabaseId, crate::types::TenantId, String) {
    use std::fmt::Write;
    let AggregateCacheKeyInputs {
        database_id,
        tid,
        collection,
        group_by,
        aggregates,
        sub_group_by,
        sub_aggregates,
        limit,
        sort_keys,
    } = inputs;
    let mut rest = format!(
        "{collection}\0{}\0{}",
        group_specs_key(group_by),
        aggregate_specs_key(aggregates)
    );
    if !sub_group_by.is_empty() || !sub_aggregates.is_empty() {
        let _ = write!(
            rest,
            "\0sub:{}\0{}",
            sub_group_by.join(","),
            aggregate_specs_key(sub_aggregates)
        );
    }
    let sort = sort_keys
        .iter()
        .map(|k| format!("{:?}:{}", k.expr, u8::from(k.ascending)))
        .collect::<Vec<_>>()
        .join(",");
    let _ = write!(rest, "\0limit:{limit}\0sort:{sort}");
    (
        crate::types::DatabaseId::new(database_id),
        crate::types::TenantId::new(tid),
        rest,
    )
}

pub(super) fn legacy_aggregate_pairs(
    aggregates: &[AggregateSpec],
) -> Option<Vec<(String, String)>> {
    aggregates
        .iter()
        .map(|agg| {
            if agg.expr.is_some() {
                None
            } else {
                Some((agg.function.clone(), agg.field.clone()))
            }
        })
        .collect()
}
