// SPDX-License-Identifier: Apache-2.0

//! Column and table resolution against the catalog.

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_object_name_checked, table_name_from_factor};
use crate::types::{
    ArrayCatalogView, CollectionInfo, ColumnInfo, EngineType, SqlCatalog, SqlDataType,
};
use crate::types_array::{ArrayAttrType, ArrayDimType};

/// Resolved table reference: name, alias, and catalog info.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    pub name: String,
    pub alias: Option<String>,
    pub info: CollectionInfo,
}

impl ResolvedTable {
    /// The name to use for qualified column references.
    pub fn ref_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Context built during FROM clause resolution.
#[derive(Debug, Default)]
pub struct TableScope {
    /// Tables by reference name (alias or table name).
    pub tables: HashMap<String, ResolvedTable>,
    /// Insertion order for unambiguous column resolution.
    order: Vec<String>,
}

impl TableScope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a resolved table. Returns error if name conflicts.
    pub fn add(&mut self, table: ResolvedTable) -> Result<()> {
        let key = table.ref_name().to_string();
        if self.tables.contains_key(&key) {
            return Err(SqlError::Parse {
                detail: format!("duplicate table reference: {key}"),
            });
        }
        self.order.push(key.clone());
        self.tables.insert(key, table);
        Ok(())
    }

    /// Resolve a column name, optionally qualified with a table reference.
    ///
    /// For schemaless collections, any column is accepted (dynamic fields).
    /// For typed collections, the column must exist in the schema.
    pub fn resolve_column(
        &self,
        table_ref: Option<&str>,
        column: &str,
    ) -> Result<(String, String)> {
        let col = column.to_lowercase();

        if let Some(tref) = table_ref {
            let tref_lower = tref.to_lowercase();
            let table = self
                .tables
                .get(&tref_lower)
                .ok_or_else(|| SqlError::UnknownTable {
                    name: tref_lower.clone(),
                })?;
            self.validate_column(table, &col)?;
            return Ok((table.name.clone(), col));
        }

        // Unqualified: search all tables.
        let mut matches = Vec::new();
        for key in &self.order {
            let table = &self.tables[key];
            if self.column_exists(table, &col) {
                matches.push(table.name.clone());
            }
        }

        match matches.len() {
            0 => {
                // For single-table queries with schemaless, accept anything.
                if self.tables.len() == 1 {
                    let table = self
                        .tables
                        .values()
                        .next()
                        .expect("invariant: self.tables.len() == 1 checked immediately above");
                    if table.info.engine == EngineType::DocumentSchemaless {
                        return Ok((table.name.clone(), col));
                    }
                }
                Err(SqlError::UnknownColumn {
                    table: self
                        .order
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".into()),
                    column: col,
                })
            }
            1 => Ok((
                matches
                    .into_iter()
                    .next()
                    .expect("invariant: matches.len() == 1 guaranteed by this match arm"),
                col,
            )),
            _ => Err(SqlError::AmbiguousColumn { column: col }),
        }
    }

    fn column_exists(&self, table: &ResolvedTable, column: &str) -> bool {
        // Schemaless accepts any column.
        if table.info.engine == EngineType::DocumentSchemaless {
            return true;
        }
        table.info.columns.iter().any(|c| c.name == column)
    }

    fn validate_column(&self, table: &ResolvedTable, column: &str) -> Result<()> {
        if self.column_exists(table, column) {
            Ok(())
        } else {
            Err(SqlError::UnknownColumn {
                table: table.name.clone(),
                column: column.into(),
            })
        }
    }

    /// Get the single table in scope (for single-table queries).
    pub fn single_table(&self) -> Option<&ResolvedTable> {
        if self.tables.len() == 1 {
            self.tables.values().next()
        } else {
            Option::None
        }
    }

    /// Resolve tables from a FROM clause.
    pub fn resolve_from(
        catalog: &dyn SqlCatalog,
        from: &[sqlparser::ast::TableWithJoins],
    ) -> Result<Self> {
        let mut scope = Self::new();
        for table_with_joins in from {
            scope.resolve_table_factor(catalog, &table_with_joins.relation)?;
            for join in &table_with_joins.joins {
                scope.resolve_table_factor(catalog, &join.relation)?;
            }
        }
        Ok(scope)
    }

    fn resolve_table_factor(
        &mut self,
        catalog: &dyn SqlCatalog,
        factor: &sqlparser::ast::TableFactor,
    ) -> Result<()> {
        // ARRAY_*(...) table-valued function: synthesize a ResolvedTable
        // from the array's dim+attr schema so equi-join keys against the
        // TVF's output rows resolve.
        if let Some(resolved) = resolve_array_tvf(catalog, factor)? {
            self.add(resolved)?;
            return Ok(());
        }
        // LATERAL derived subquery: register the alias as a schemaless
        // collection so qualified column references (`alias.col`) resolve
        // without a catalog lookup. The actual inner plan is built separately.
        if let sqlparser::ast::TableFactor::Derived {
            lateral: true,
            alias: Some(alias),
            ..
        } = factor
        {
            let alias_str = crate::reserved::check_ast_identifier(&alias.name)?;
            self.add(ResolvedTable {
                name: alias_str.clone(),
                alias: Some(alias_str.clone()),
                info: CollectionInfo {
                    name: alias_str,
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: None,
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                },
            })?;
            return Ok(());
        }
        if let Some((name, alias)) = table_name_from_factor(factor)? {
            let info = catalog
                .resolve_relation(DatabaseId::DEFAULT, &name)?
                .ok_or_else(|| SqlError::UnknownTable { name: name.clone() })?;
            self.add(ResolvedTable { name, alias, info })?;
        }
        Ok(())
    }
}

/// If `factor` is `ARRAY_*(name, ...)`, look up the array via the
/// catalog and build a `ResolvedTable` whose columns mirror the array's
/// dims + attrs. Returns `Ok(None)` for any non-array-TVF factor.
fn resolve_array_tvf(
    catalog: &dyn SqlCatalog,
    factor: &sqlparser::ast::TableFactor,
) -> Result<Option<ResolvedTable>> {
    let (fn_name, args, alias) = match factor {
        sqlparser::ast::TableFactor::Table {
            name,
            args: Some(args),
            alias,
            ..
        } => (
            normalize_object_name_checked(name)?,
            args,
            alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?,
        ),
        _ => return Ok(None),
    };
    if !matches!(
        fn_name.as_str(),
        "array_slice" | "array_project" | "array_agg" | "array_elementwise"
    ) {
        return Ok(None);
    }

    // First positional arg is the array name as a string literal.
    let first = args.args.first().ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: missing array-name argument"),
    })?;
    let array_name = extract_string_literal_arg(first).ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: array-name argument must be a string literal"),
    })?;
    let view = catalog
        .lookup_array(&array_name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: array_name.clone(),
        })?;

    let info = CollectionInfo {
        name: view.name.clone(),
        engine: EngineType::Array,
        columns: array_columns(&view),
        primary_key: None,
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
    };
    Ok(Some(ResolvedTable {
        name: view.name,
        alias,
        info,
    }))
}

fn array_columns(view: &ArrayCatalogView) -> Vec<ColumnInfo> {
    let mut cols = Vec::with_capacity(view.dims.len() + view.attrs.len());
    for d in &view.dims {
        cols.push(ColumnInfo {
            name: d.name.clone(),
            data_type: dim_type_to_sql(d.dtype),
            nullable: false,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        });
    }
    for a in &view.attrs {
        cols.push(ColumnInfo {
            name: a.name.clone(),
            data_type: attr_type_to_sql(a.dtype),
            nullable: a.nullable,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        });
    }
    cols
}

fn dim_type_to_sql(t: ArrayDimType) -> SqlDataType {
    match t {
        ArrayDimType::Int64 => SqlDataType::Int64,
        ArrayDimType::Float64 => SqlDataType::Float64,
        ArrayDimType::TimestampMs => SqlDataType::Timestamp,
        ArrayDimType::String => SqlDataType::String,
    }
}

fn attr_type_to_sql(t: ArrayAttrType) -> SqlDataType {
    match t {
        ArrayAttrType::Int64 => SqlDataType::Int64,
        ArrayAttrType::Float64 => SqlDataType::Float64,
        ArrayAttrType::String => SqlDataType::String,
        ArrayAttrType::Bytes => SqlDataType::Bytes,
    }
}

fn extract_string_literal_arg(arg: &sqlparser::ast::FunctionArg) -> Option<String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value};
    let expr = match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => e,
        _ => return None,
    };
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType};
    use nodedb_types::PrimaryEngine;

    fn strict_collection(name: &str, columns: Vec<&str>) -> CollectionInfo {
        CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentStrict,
            columns: columns
                .into_iter()
                .map(|c| ColumnInfo {
                    name: c.into(),
                    data_type: SqlDataType::String,
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                    raw_type: None,
                    int_width: None,
                    float_width: None,
                })
                .collect(),
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        }
    }

    fn schemaless_collection(name: &str) -> CollectionInfo {
        CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentSchemaless,
            columns: Vec::new(),
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        }
    }

    fn scope_with(info: CollectionInfo) -> TableScope {
        let mut scope = TableScope::new();
        scope
            .add(ResolvedTable {
                name: info.name.clone(),
                alias: None,
                info,
            })
            .expect("add failed");
        scope
    }

    /// A double-quoted identifier resolves as a column name (case-preserved).
    /// `"userId"` is parsed by the SQL layer as `Expr::Identifier` with
    /// `quote_style = Some('"')` and `value = "userId"`.  At the
    /// `TableScope` level the column name arrives lowercase (strict schema
    /// columns are stored lowercase), so the resolved name is `"userid"`.
    ///
    /// This test confirms the resolution path, not just `convert_expr`.
    #[test]
    fn quoted_identifier_resolves_as_column() {
        let scope = scope_with(strict_collection("users", vec!["userid", "email"]));
        let (table, col) = scope
            .resolve_column(None, "userid")
            .expect("should resolve");
        assert_eq!(table, "users");
        assert_eq!(col, "userid");
    }

    /// An unrecognized column in a strict collection must yield
    /// `SqlError::UnknownColumn`, NOT `SqlError::Unsupported`.
    /// This verifies that a double-quoted identifier like `"ghost_col"`
    /// that maps to `SqlExpr::Column { name: "ghost_col" }` surfaces the
    /// right error variant when resolved against a strict schema.
    #[test]
    fn unknown_column_in_strict_collection_yields_unknown_column_error() {
        let scope = scope_with(strict_collection("users", vec!["id", "email"]));
        let err = scope
            .resolve_column(None, "ghost_col")
            .expect_err("should fail for unknown column");
        assert!(
            matches!(err, SqlError::UnknownColumn { ref column, .. } if column == "ghost_col"),
            "expected UnknownColumn(ghost_col), got {err:?}"
        );
        // Must NOT be Unsupported — that would be the wrong error variant.
        assert!(
            !matches!(err, SqlError::Unsupported { .. }),
            "must not surface Unsupported for a missing column"
        );
    }

    /// Schemaless collections accept any column, including ones that look
    /// like they could be misidentified double-quoted identifiers.
    #[test]
    fn any_column_accepted_in_schemaless_collection() {
        let scope = scope_with(schemaless_collection("events"));
        let (table, col) = scope
            .resolve_column(None, "ghost_col")
            .expect("schemaless should accept any column");
        assert_eq!(table, "events");
        assert_eq!(col, "ghost_col");
    }

    /// Qualified column reference: `"t"."col"` → table `t`, column `col`.
    #[test]
    fn qualified_column_resolves_correctly() {
        let scope = scope_with(strict_collection("t", vec!["col", "other"]));
        let (table, col) = scope
            .resolve_column(Some("t"), "col")
            .expect("qualified column should resolve");
        assert_eq!(table, "t");
        assert_eq!(col, "col");
    }

    /// Qualified reference to an unknown column in a strict collection must
    /// yield `SqlError::UnknownColumn`, not `Unsupported`.
    #[test]
    fn qualified_unknown_column_in_strict_collection() {
        let scope = scope_with(strict_collection("t", vec!["id"]));
        let err = scope
            .resolve_column(Some("t"), "missing")
            .expect_err("should fail");
        assert!(
            matches!(err, SqlError::UnknownColumn { .. }),
            "expected UnknownColumn, got {err:?}"
        );
    }
}
