// SPDX-License-Identifier: BUSL-1.1

//! Aggregate result-cache key derivation.

use nodedb_physical::physical_plan::AggregateSpec;

pub(super) fn aggregate_cache_key(
    tid: u64,
    collection: &str,
    group_by: &[String],
    aggregates: &[AggregateSpec],
    sub_group_by: &[String],
    sub_aggregates: &[AggregateSpec],
) -> (crate::types::TenantId, String) {
    use std::fmt::Write;
    let mut rest = format!(
        "{collection}\0{}\0{}",
        group_by.join(","),
        aggregates
            .iter()
            .map(|agg| {
                if agg.expr.is_some() {
                    format!("{}(expr)->{}", agg.function, agg.alias)
                } else {
                    format!("{}({})->{}", agg.function, agg.field, agg.alias)
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    );
    if !sub_group_by.is_empty() || !sub_aggregates.is_empty() {
        let _ = write!(
            rest,
            "\0sub:{}\0{}",
            sub_group_by.join(","),
            sub_aggregates
                .iter()
                .map(|agg| {
                    if agg.expr.is_some() {
                        format!("{}(expr)->{}", agg.function, agg.alias)
                    } else {
                        format!("{}({})->{}", agg.function, agg.field, agg.alias)
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    (crate::types::TenantId::new(tid), rest)
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
