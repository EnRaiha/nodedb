// SPDX-License-Identifier: BUSL-1.1

//! Derives the planner-authoritative [`OutputSchema`] from a compiled
//! `SqlPlan` list, for later threading into response shaping.
//!
//! Purely additive: nothing in this module is consumed by any existing
//! call site yet. The single call site that invokes [`build_output_schema`]
//! today discards the result.

use std::collections::HashMap;

use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::query::Projection;

use crate::control::server::response_shape::schema::{
    OutputColumn, OutputSchema, sql_data_type_to_ddl_col_type,
};
use crate::control::server::response_shape::types::DdlColType;

/// Maps one `Projection` entry to an `OutputColumn`, given a map of bare
/// column name -> resolved wire type for the collection in scope.
///
/// Mirrors `response_shape::project::expr_column_names`'s derivation rule:
/// for a qualified `table.column` reference, `lookup_key` keeps the full
/// dot-joined form (the join executor prefixes every key with its source
/// collection name) while `display_name` is the last segment. For a bare
/// column both are identical.
///
/// `Projection::Star` / `Projection::QualifiedStar` have no single concrete
/// column and return `None`; the caller sets `is_star` instead.
fn projection_to_column(
    p: &Projection,
    types: &HashMap<String, DdlColType>,
) -> Option<OutputColumn> {
    match p {
        Projection::Column(qname) => {
            let display_name = qname
                .rsplit('.')
                .next()
                .map(str::to_string)
                .unwrap_or_else(|| qname.clone());
            let ty = types
                .get(&display_name)
                .copied()
                .unwrap_or(DdlColType::Text);
            Some(OutputColumn {
                display_name,
                lookup_key: qname.clone(),
                ty,
            })
        }
        Projection::Computed { alias, .. } => Some(OutputColumn {
            display_name: alias.clone(),
            lookup_key: alias.clone(),
            ty: DdlColType::Text,
        }),
        Projection::Star | Projection::QualifiedStar(_) => None,
    }
}

/// Builds a `HashMap` of bare column name -> resolved wire type for
/// `collection`, via a best-effort catalog lookup. Returns an empty map
/// (never an error) when the lookup fails or the collection is unknown —
/// callers fall back to `DdlColType::Text` for every column in that case.
fn column_types_for<C: SqlCatalog>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> HashMap<String, DdlColType> {
    match catalog.get_collection(database_id, collection) {
        Ok(Some(info)) => info
            .columns
            .iter()
            .map(|c| (c.name.clone(), sql_data_type_to_ddl_col_type(&c.data_type)))
            .collect(),
        _ => HashMap::new(),
    }
}

/// Maps a projection list to an `OutputSchema` fragment using `types`.
fn schema_from_projection(
    projection: &[Projection],
    types: &HashMap<String, DdlColType>,
) -> OutputSchema {
    let mut columns = Vec::with_capacity(projection.len());
    let mut is_star = false;
    for p in projection {
        match projection_to_column(p, types) {
            Some(col) => columns.push(col),
            None => is_star = true,
        }
    }
    OutputSchema { columns, is_star }
}

/// Derives the planner-authoritative output schema of a compiled plan list.
///
/// Only the plan variants that carry a resolvable projection against a
/// single named collection are handled directly; other plan variants are
/// handled by later units in this effort and fall back to an empty schema.
pub fn build_output_schema<C: SqlCatalog>(
    plans: &[SqlPlan],
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
) -> OutputSchema {
    let Some(plan) = plans.first() else {
        return OutputSchema {
            columns: Vec::new(),
            is_star: false,
        };
    };

    match plan {
        SqlPlan::Scan {
            collection,
            projection,
            ..
        }
        | SqlPlan::DocumentIndexLookup {
            collection,
            projection,
            ..
        }
        | SqlPlan::SpatialScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::TimeseriesScan {
            collection,
            projection,
            ..
        } => {
            let types = column_types_for(catalog, database_id, collection);
            schema_from_projection(projection, &types)
        }
        SqlPlan::Join { projection, .. } => {
            // A join has no single source collection; column types default
            // to `Text` for every projected field rather than picking one
            // side arbitrarily.
            let types = HashMap::new();
            schema_from_projection(projection, &types)
        }
        SqlPlan::ConstantResult { columns, .. } => OutputSchema {
            columns: columns
                .iter()
                .map(|c| OutputColumn {
                    display_name: c.clone(),
                    lookup_key: c.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
        },
        // Other plan variants (Aggregate, Union, VectorSearch, HybridSearch,
        // Match, etc.) are handled by later units in this effort.
        _ => OutputSchema {
            columns: Vec::new(),
            is_star: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_column_uses_matching_type_from_map() {
        let mut types = HashMap::new();
        types.insert("foo".to_string(), DdlColType::Int8);
        let p = Projection::Column("foo".to_string());
        let col = projection_to_column(&p, &types).expect("Some for Column");
        assert_eq!(col.lookup_key, "foo");
        assert_eq!(col.display_name, "foo");
        assert_eq!(col.ty, DdlColType::Int8);
    }

    #[test]
    fn qualified_column_display_is_last_segment() {
        let types = HashMap::new();
        let p = Projection::Column("t.bar".to_string());
        let col = projection_to_column(&p, &types).expect("Some for Column");
        assert_eq!(col.lookup_key, "t.bar");
        assert_eq!(col.display_name, "bar");
        assert_eq!(col.ty, DdlColType::Text);
    }

    #[test]
    fn computed_uses_alias_for_both_and_defaults_to_text() {
        let types = HashMap::new();
        let p = Projection::Computed {
            expr: nodedb_sql::types_expr::SqlExpr::Wildcard,
            alias: "total".to_string(),
        };
        let col = projection_to_column(&p, &types).expect("Some for Computed");
        assert_eq!(col.lookup_key, "total");
        assert_eq!(col.display_name, "total");
        assert_eq!(col.ty, DdlColType::Text);
    }

    #[test]
    fn star_returns_none() {
        let types = HashMap::new();
        assert!(projection_to_column(&Projection::Star, &types).is_none());
        assert!(
            projection_to_column(&Projection::QualifiedStar("t".to_string()), &types).is_none()
        );
    }

    /// Catalog stub whose `get_collection` is never called by the
    /// `ConstantResult` branch under test; only required to satisfy the
    /// generic `SqlCatalog` bound on `build_output_schema`.
    struct NoCatalog;

    impl SqlCatalog for NoCatalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            _name: &str,
        ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
        {
            Ok(None)
        }
    }

    #[test]
    fn constant_result_columns_map_to_text_output_columns() {
        let plans = vec![SqlPlan::ConstantResult {
            columns: vec!["a".to_string(), "b".to_string()],
            values: vec![],
        }];
        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].display_name, "a");
        assert_eq!(schema.columns[0].lookup_key, "a");
        assert_eq!(schema.columns[1].display_name, "b");
        assert!(!schema.is_star);
    }
}
