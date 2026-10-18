// SPDX-License-Identifier: Apache-2.0

//! Engine rules for a continuous aggregate read as a relation.
//!
//! A continuous aggregate is read-only: its rows are the finalized buckets
//! the aggregate maintains from its source. Every write refuses with
//! SQLSTATE `42809`. A read plans as a scan of the aggregate's rows, and an
//! aggregate query over it aggregates those rows. A key equality never
//! becomes a point lookup: the rows have no primary key.

use crate::engine_rules::*;
use crate::error::{Result, SqlError};
use crate::types::*;

pub struct ContinuousAggregateRules;

/// The refusal every write to continuous aggregate `name` gets.
fn read_only(op: &str, name: &str) -> SqlError {
    SqlError::WrongObjectType {
        detail: format!("continuous aggregate '{name}' is read-only; {op} is not allowed"),
    }
}

fn refuse_temporal(collection: &str, temporal: &crate::temporal::TemporalScope) -> Result<()> {
    if temporal.is_temporal() {
        return Err(SqlError::Unsupported {
            detail: format!(
                "FOR SYSTEM_TIME / FOR VALID_TIME is not supported on continuous \
                 aggregate '{collection}'"
            ),
        });
    }
    Ok(())
}

impl EngineRules for ContinuousAggregateRules {
    fn plan_insert(&self, p: InsertParams) -> Result<Vec<SqlPlan>> {
        Err(read_only("INSERT", &p.collection))
    }

    fn plan_insert_select(&self, p: InsertSelectParams) -> Result<Vec<SqlPlan>> {
        Err(read_only("INSERT ... SELECT", &p.collection))
    }

    fn plan_upsert(&self, p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Err(read_only("UPSERT", &p.collection))
    }

    fn plan_scan(&self, p: ScanParams) -> Result<SqlPlan> {
        refuse_temporal(&p.collection, &p.temporal)?;
        Ok(SqlPlan::Scan {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::ContinuousAggregate,
            filters: p.filters,
            projection: p.projection,
            sort_keys: p.sort_keys,
            limit: p.limit,
            offset: p.offset,
            distinct: p.distinct,
            window_functions: p.window_functions,
            temporal: p.temporal,
        })
    }

    /// The optimizer never rewrites a continuous-aggregate read into a point
    /// lookup (`has_point_lookup`), so no plan reaches here.
    fn plan_point_get(&self, p: PointGetParams) -> Result<SqlPlan> {
        Err(SqlError::Unsupported {
            detail: format!(
                "continuous aggregate '{}' has no primary key to look up",
                p.collection
            ),
        })
    }

    fn plan_update(&self, p: UpdateParams) -> Result<Vec<SqlPlan>> {
        Err(read_only("UPDATE", &p.collection))
    }

    fn plan_update_from(&self, p: UpdateFromParams) -> Result<Vec<SqlPlan>> {
        Err(read_only("UPDATE ... FROM", &p.collection))
    }

    fn plan_delete(&self, p: DeleteParams) -> Result<Vec<SqlPlan>> {
        Err(read_only("DELETE", &p.collection))
    }

    fn plan_truncate(&self, p: TruncateParams) -> Result<Vec<SqlPlan>> {
        Err(read_only("TRUNCATE", &p.collection))
    }

    /// Each row loads as one INSERT, which `plan_insert` refuses.
    fn plan_bulk_load(&self, _p: BulkLoadParams) -> Result<BulkLoadPlan> {
        Ok(BulkLoadPlan::RowInserts)
    }

    fn plan_aggregate(&self, p: AggregateParams) -> Result<SqlPlan> {
        refuse_temporal(&p.collection, &p.temporal)?;
        let base_scan = SqlPlan::Scan {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::ContinuousAggregate,
            filters: p.filters,
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: p.temporal,
        };
        Ok(SqlPlan::Aggregate {
            input: Box::new(base_scan),
            group_by: p.group_by,
            group_by_aliases: Vec::new(),
            output_order: Vec::new(),
            aggregates: p.aggregates,
            having: p.having,
            limit: p.limit,
            grouping_sets: p.grouping_sets,
            sort_keys: Vec::new(),
        })
    }

    fn plan_merge(&self, p: MergeParams) -> Result<Vec<SqlPlan>> {
        Err(read_only("MERGE", &p.collection))
    }

    /// The columns are the bucket, the GROUP BY columns and the outputs.
    fn accepts_undeclared_columns(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_write_is_refused_as_read_only() {
        let r = ContinuousAggregateRules;
        let err = r
            .plan_insert(InsertParams {
                collection: "cpu_1m".into(),
                columns: vec![],
                rows: vec![],
                volatile_defaults: false,
                if_absent: false,
                column_schema: vec![],
                primary_key: None,
            })
            .unwrap_err();
        assert!(
            matches!(&err, SqlError::WrongObjectType { detail } if detail.contains("read-only")),
            "{err:?}"
        );
        assert!(matches!(
            r.plan_truncate(TruncateParams {
                collection: "cpu_1m".into(),
                restart_identity: false,
            })
            .unwrap_err(),
            SqlError::WrongObjectType { .. }
        ));
    }
}
