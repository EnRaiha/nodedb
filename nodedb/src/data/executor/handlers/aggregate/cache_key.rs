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

#[allow(clippy::too_many_arguments)] // complete query shape is the cache identity
pub(super) fn aggregate_cache_key(
    database_id: u64,
    tid: u64,
    collection: &str,
    group_by: &[GroupKeySpec],
    aggregates: &[AggregateSpec],
    sub_group_by: &[String],
    sub_aggregates: &[AggregateSpec],
    limit: usize,
    sort_keys: &[(String, bool)],
) -> (crate::types::DatabaseId, crate::types::TenantId, String) {
    use std::fmt::Write;
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
        .map(|(field, ascending)| format!("{field}:{}", u8::from(*ascending)))
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
