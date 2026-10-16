// SPDX-License-Identifier: Apache-2.0

//! Engine rules for the ND sparse array engine.
//!
//! Array operations live behind a dedicated DDL/DML surface
//! (`CREATE ARRAY`, `INSERT INTO ARRAY`, `DELETE FROM ARRAY`,
//! `DROP ARRAY`). The standard SQL DML pathways are unsupported by
//! design — the planner refuses them with a hint pointing to the
//! correct surface (use ARRAY_SLICE / ARRAY_AGG for table-valued reads).

use crate::engine_rules::*;
use crate::error::{Result, SqlError};
use crate::types::*;

pub struct ArrayRules;

impl EngineRules for ArrayRules {
    fn plan_insert(&self, _p: InsertParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "INSERT",
            "use INSERT INTO ARRAY <name> COORDS (...) VALUES (...)",
        ))
    }

    fn plan_insert_select(&self, _p: InsertSelectParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "INSERT ... SELECT",
            "use INSERT INTO ARRAY for array engine mutations",
        ))
    }

    fn plan_upsert(&self, _p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported("UPSERT", "arrays do not support UPSERT"))
    }

    fn plan_scan(&self, _p: ScanParams) -> Result<SqlPlan> {
        Err(unsupported(
            "SELECT",
            "use ARRAY_SLICE or ARRAY_AGG for table-valued array reads",
        ))
    }

    fn plan_point_get(&self, _p: PointGetParams) -> Result<SqlPlan> {
        Err(unsupported(
            "point lookup",
            "arrays have no primary key; use ARRAY_SLICE for coord-range reads",
        ))
    }

    fn plan_update(&self, _p: UpdateParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "UPDATE",
            "arrays are write-by-coord; re-INSERT to overwrite",
        ))
    }

    fn plan_update_from(&self, _p: UpdateFromParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "UPDATE ... FROM",
            "arrays are write-by-coord; re-INSERT to overwrite",
        ))
    }

    fn plan_delete(&self, _p: DeleteParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "DELETE",
            "use DELETE FROM ARRAY <name> WHERE COORDS IN ((...), ...)",
        ))
    }

    fn plan_truncate(&self, _p: TruncateParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "TRUNCATE",
            "use DROP ARRAY <name> to remove the array, or \
             DELETE FROM ARRAY <name> WHERE COORDS IN (...) to remove cells",
        ))
    }

    /// A bulk load decodes its rows against the array's schema and loads them
    /// in chunks. One chunk is one `InsertArray` plan, so each chunk is one
    /// cell batch and one WAL record.
    fn plan_bulk_load(&self, p: BulkLoadParams) -> Result<BulkLoadPlan> {
        if p.cells.is_empty() {
            return Ok(BulkLoadPlan::CellChunks(Vec::new()));
        }
        Ok(BulkLoadPlan::CellChunks(vec![SqlPlan::InsertArray(
            InsertArrayPlan {
                name: p.collection,
                rows: p.cells,
            },
        )]))
    }

    fn plan_aggregate(&self, _p: AggregateParams) -> Result<SqlPlan> {
        Err(unsupported(
            "GROUP BY",
            "use ARRAY_AGG for table-valued array aggregates",
        ))
    }

    fn plan_merge(&self, _p: MergeParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "MERGE",
            "use INSERT INTO ARRAY / DELETE FROM ARRAY for array engine mutations",
        ))
    }

    /// Columns are the array's declared dims and attrs.
    fn accepts_undeclared_columns(&self) -> bool {
        false
    }
}

fn unsupported(op: &str, hint: &str) -> SqlError {
    SqlError::Unsupported {
        detail: format!("operation {op} not supported on array engine; {hint}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip() -> InsertParams {
        InsertParams {
            collection: "g".into(),
            columns: vec![],
            rows: vec![],
            volatile_defaults: false,
            if_absent: false,
            column_schema: vec![],
            primary_key: None,
        }
    }

    #[test]
    fn bulk_load_plans_one_insert_array_per_chunk() {
        let probe = ArrayRules
            .plan_bulk_load(BulkLoadParams {
                collection: "g".into(),
                cells: Vec::new(),
            })
            .unwrap();
        assert!(matches!(probe, BulkLoadPlan::CellChunks(ref p) if p.is_empty()));

        let row = crate::types_array::ArrayInsertRow {
            coords: vec![crate::types_array::ArrayCoordLiteral::Int64(1)],
            attrs: vec![crate::types_array::ArrayAttrLiteral::Float64(0.5)],
        };
        let chunk = ArrayRules
            .plan_bulk_load(BulkLoadParams {
                collection: "g".into(),
                cells: vec![row.clone(), row],
            })
            .unwrap();
        match chunk {
            BulkLoadPlan::CellChunks(plans) => {
                assert_eq!(plans.len(), 1);
                assert!(matches!(
                    &plans[0],
                    SqlPlan::InsertArray(InsertArrayPlan { name, rows }) if name == "g" && rows.len() == 2
                ));
            }
            BulkLoadPlan::RowInserts => panic!("an array loads in cell chunks"),
        }
    }

    #[test]
    fn every_arm_is_unsupported() {
        let r = ArrayRules;
        assert!(matches!(
            r.plan_insert(ip()).unwrap_err(),
            SqlError::Unsupported { .. }
        ));
        assert!(matches!(
            r.plan_upsert(UpsertParams {
                collection: "g".into(),
                columns: vec![],
                rows: vec![],
                volatile_defaults: false,
                on_conflict_updates: vec![],
                column_schema: vec![],
                primary_key: None,
            })
            .unwrap_err(),
            SqlError::Unsupported { .. }
        ));
    }
}
