// SPDX-License-Identifier: BUSL-1.1

//! Schema-inference utilities for `NodeDbQueryParser`.
//!
//! These free functions are called by `parser.rs` during Parse-message
//! handling to infer parameter and result-field types from SQL text and
//! catalog metadata.

use nodedb_types::DatabaseId;
use pgwire::api::Type;
use pgwire::api::results::FieldInfo;

/// Return true if `sql` starts with a DSL or DDL keyword that `plan_sql`
/// cannot parse and must be routed through `execute_sql` at Execute time.
///
/// Mirrors the prefix checks in `ddl/router/dsl.rs` so the extended-query
/// Parse handler can mark such statements as DSL passthroughs and route them
/// through the DSL dispatcher at Execute time.
///
/// NodeDB-specific DDL (`CREATE COLLECTION`, `DROP COLLECTION`, etc.) is also
/// included here because `execute_planned_sql_with_params` uses the standard
/// SQL planner (sqlparser) which does not recognise NodeDB extensions.
pub(super) fn is_dsl_statement(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    // `SEARCH ... USING VECTOR(...)` is preprocessor-rewritten into canonical
    // SELECT and goes through plan_sql like any other SELECT. Only the FUSION
    // form (and other SEARCH variants without a SELECT lowering) is a DSL
    // passthrough.
    if upper.starts_with("SEARCH ") && upper.contains("USING VECTOR") {
        return false;
    }
    // NodeDB DDL: `ddl_ast::parse` recognises these but `plan_sql` does not.
    // Route through `execute_sql` so the DDL router handles them. The full
    // parser tokenises and tries ~20 family dispatchers, so gate on the
    // first keyword first — most Parse messages carry plain SELECT/INSERT.
    let first_token = upper.split_whitespace().next().unwrap_or("");
    let may_be_ddl = matches!(
        first_token,
        "CREATE"
            | "DROP"
            | "ALTER"
            | "SHOW"
            | "DESCRIBE"
            | "GRANT"
            | "REVOKE"
            | "ANALYZE"
            | "COPY"
            | "BACKUP"
            | "RESTORE"
            | "UNDROP"
            | "REINDEX"
            | "REMOVE"
            | "REBALANCE"
            | "COMPACT"
    );
    if may_be_ddl && nodedb_sql::ddl_ast::parse(sql).is_some() {
        return true;
    }
    // Function, procedure, and aggregate DDL handled by the text-based DDL
    // router (function::dispatch) but not recognised by nodedb_sql::ddl_ast::parse.
    // Route through execute_sql so the DDL router intercepts them.
    if may_be_ddl
        && (upper.starts_with("CREATE OR REPLACE FUNCTION ")
            || upper.starts_with("CREATE FUNCTION ")
            || upper.starts_with("CREATE OR REPLACE AGGREGATE FUNCTION ")
            || upper.starts_with("CREATE AGGREGATE FUNCTION ")
            || upper.starts_with("CREATE OR REPLACE PROCEDURE ")
            || upper.starts_with("CREATE PROCEDURE ")
            || upper.starts_with("DROP FUNCTION ")
            || upper.starts_with("DROP PROCEDURE ")
            || upper.starts_with("ALTER FUNCTION ")
            || upper.starts_with("CALL "))
    {
        return true;
    }
    upper.starts_with("SEARCH ")
        || upper.starts_with("GRAPH ")
        || upper.starts_with("MATCH ")
        || upper.starts_with("OPTIONAL MATCH ")
        || upper.starts_with("CRDT MERGE ")
        || upper.starts_with("UPSERT INTO ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE FULLTEXT INDEX ")
        || upper.starts_with("CREATE SEARCH INDEX ")
        || upper.starts_with("CREATE SPARSE INDEX ")
}

/// Parse the top-level SELECT projection list with sqlparser. Returns
/// the list of (column_name, is_star) pairs, or `None` if the SQL isn't
/// a simple SELECT or parsing fails. A `Star` entry signals "all
/// columns from the target collection".
pub(super) enum ProjectionItem {
    Star,
    Named(String),
}

pub(super) fn parse_select_projection(sql: &str) -> Option<Vec<ProjectionItem>> {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let stmt = stmts.into_iter().next()?;
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    let mut out = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                out.push(ProjectionItem::Star);
            }
            SelectItem::UnnamedExpr(expr) => {
                out.push(ProjectionItem::Named(expr_column_name(expr)));
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                out.push(ProjectionItem::Named(alias.value.clone()));
            }
        }
    }
    Some(out)
}

/// Extract a reasonable column label from an expression. For a bare
/// identifier this is the identifier name; for compound identifiers,
/// the last segment; otherwise the stringified expression.
pub(super) fn expr_column_name(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| expr.to_string()),
        other => other.to_string(),
    }
}

/// Build `FieldInfo`s from a parsed SELECT projection. Star entries
/// expand against the collection's schema (if available); named entries
/// use the typed column when found, else default to TEXT.
pub(super) fn fields_from_projection(
    projection: &[ProjectionItem],
    plan: Option<&nodedb_sql::SqlPlan>,
    catalog: &dyn nodedb_sql::SqlCatalog,
) -> Vec<FieldInfo> {
    use pgwire::api::results::FieldFormat;

    let info = plan.and_then(|p| plan_collection(p)).and_then(|name| {
        catalog
            .get_collection(DatabaseId::DEFAULT, name)
            .ok()
            .flatten()
    });

    let mut fields = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            ProjectionItem::Star => {
                if let Some(info) = &info {
                    fields.extend(columns_to_field_info(&info.columns));
                }
            }
            ProjectionItem::Named(name) => {
                let pg_type = info
                    .as_ref()
                    .and_then(|info| info.columns.iter().find(|c| c.name == *name))
                    .map(|c| sql_data_type_to_pg(&c.data_type))
                    .unwrap_or(Type::TEXT);
                fields.push(FieldInfo::new(
                    name.clone(),
                    None,
                    None,
                    pg_type,
                    FieldFormat::Text,
                ));
            }
        }
    }
    fields
}

/// The collection targeted by a plan, if any. Used to pull typed schema
/// when building projection field info.
pub(super) fn plan_collection(plan: &nodedb_sql::SqlPlan) -> Option<&str> {
    use nodedb_sql::SqlPlan;
    match plan {
        SqlPlan::Scan { collection, .. }
        | SqlPlan::PointGet { collection, .. }
        | SqlPlan::DocumentIndexLookup { collection, .. } => Some(collection.as_str()),
        SqlPlan::Aggregate { input, .. } => plan_collection(input),
        SqlPlan::Join { left, .. } => plan_collection(left),
        _ => None,
    }
}

/// Replace each `$N` placeholder in `sql` with the literal `NULL`.
/// Used only for Parse-time schema inference — the real bound values
/// are substituted at Execute time.
pub(super) fn substitute_placeholders_with_null(sql: &str) -> String {
    let ranges = crate::control::server::shared::sql::placeholder::placeholder_ranges(sql);
    if ranges.is_empty() {
        return sql.to_owned();
    }
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;
    for (start, end, _idx) in ranges {
        out.push_str(&sql[cursor..start]);
        out.push_str("NULL");
        cursor = end;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Count $1, $2, ... placeholders in SQL text.
pub(super) fn count_placeholders(sql: &str) -> usize {
    let mut max_idx = 0usize;
    for (_, _, idx) in crate::control::server::shared::sql::placeholder::placeholder_ranges(sql) {
        if idx > max_idx {
            max_idx = max_idx.max(idx);
        }
    }
    max_idx
}

/// Build result `FieldInfo`s for a DML statement with a RETURNING clause.
///
/// Resolves the target collection from the DML plan, looks up its schema, and
/// projects the RETURNING spec onto it. Returns `None` if the plan isn't a
/// recognized DML type or the collection schema cannot be found.
pub(super) fn result_fields_for_returning(
    spec: &nodedb_physical::physical_plan::ReturningSpec,
    plan: Option<&nodedb_sql::SqlPlan>,
    catalog: &dyn nodedb_sql::SqlCatalog,
) -> Option<Vec<FieldInfo>> {
    use nodedb_physical::physical_plan::{ReturningColumns, ReturningItem};
    use pgwire::api::results::FieldFormat;

    let collection = match plan? {
        nodedb_sql::SqlPlan::Update { collection, .. } => collection.as_str(),
        nodedb_sql::SqlPlan::Delete { collection, .. } => collection.as_str(),
        _ => return None,
    };

    let info = catalog
        .get_collection(DatabaseId::DEFAULT, collection)
        .ok()
        .flatten()?;

    let fields = match &spec.columns {
        ReturningColumns::Star => columns_to_field_info(&info.columns),
        ReturningColumns::Named(items) => items
            .iter()
            .map(|item: &ReturningItem| {
                let display_name = item.alias.clone().unwrap_or_else(|| item.name.clone());
                let pg_type = info
                    .columns
                    .iter()
                    .find(|c| c.name == item.name)
                    .map(|c| sql_data_type_to_pg(&c.data_type))
                    .unwrap_or(Type::TEXT);
                FieldInfo::new(display_name, None, None, pg_type, FieldFormat::Text)
            })
            .collect(),
    };
    Some(fields)
}

/// Infer result FieldInfo from a SqlPlan by looking up collection schema.
pub(super) fn infer_result_fields(
    plan: &nodedb_sql::SqlPlan,
    catalog: &dyn nodedb_sql::SqlCatalog,
) -> Vec<FieldInfo> {
    use nodedb_sql::types::*;
    use pgwire::api::results::FieldFormat;

    let collection = match plan {
        SqlPlan::Scan { collection, .. } => collection,
        SqlPlan::PointGet { collection, .. } => collection,
        SqlPlan::DocumentIndexLookup { collection, .. } => collection,
        SqlPlan::ConstantResult { columns, .. } => {
            return columns
                .iter()
                .map(|name| FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text))
                .collect();
        }
        SqlPlan::Aggregate { aggregates, .. } => {
            return aggregates
                .iter()
                .map(|agg| {
                    FieldInfo::new(agg.alias.clone(), None, None, Type::TEXT, FieldFormat::Text)
                })
                .collect();
        }
        SqlPlan::Join { left, right, .. } => {
            let mut fields = infer_result_fields(left, catalog);
            fields.extend(infer_result_fields(right, catalog));
            return fields;
        }
        _ => return Vec::new(),
    };

    let info = match catalog.get_collection(DatabaseId::DEFAULT, collection) {
        Ok(Some(i)) => i,
        Ok(None) | Err(_) => return Vec::new(),
    };

    let projected_cols = match plan {
        SqlPlan::Scan { projection, .. } => projection,
        SqlPlan::DocumentIndexLookup { projection, .. } => projection,
        _ => return columns_to_field_info(&info.columns),
    };

    if projected_cols.is_empty() || projected_cols.iter().any(|p| matches!(p, Projection::Star)) {
        return columns_to_field_info(&info.columns);
    }

    projected_cols
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => {
                let col = info.columns.iter().find(|c| c.name == *name);
                let pg_type = col
                    .map(|c| sql_data_type_to_pg(&c.data_type))
                    .unwrap_or(Type::TEXT);
                Some(FieldInfo::new(
                    name.clone(),
                    None,
                    None,
                    pg_type,
                    FieldFormat::Text,
                ))
            }
            Projection::Computed { alias, .. } => Some(FieldInfo::new(
                alias.clone(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )),
            _ => None,
        })
        .collect()
}

pub(super) fn columns_to_field_info(columns: &[nodedb_sql::ColumnInfo]) -> Vec<FieldInfo> {
    use pgwire::api::results::FieldFormat;
    columns
        .iter()
        .map(|c| {
            FieldInfo::new(
                c.name.clone(),
                None,
                None,
                sql_data_type_to_pg(&c.data_type),
                FieldFormat::Text,
            )
        })
        .collect()
}

pub(super) fn sql_data_type_to_pg(dt: &nodedb_sql::SqlDataType) -> Type {
    use nodedb_sql::types::SqlDataType;
    match dt {
        SqlDataType::Int64 => Type::INT8,
        SqlDataType::Float64 => Type::FLOAT8,
        SqlDataType::String => Type::TEXT,
        SqlDataType::Bool => Type::BOOL,
        SqlDataType::Bytes => Type::BYTEA,
        SqlDataType::Timestamp => Type::TIMESTAMP,
        SqlDataType::Timestamptz => Type::TIMESTAMPTZ,
        SqlDataType::Decimal => Type::NUMERIC,
        SqlDataType::Uuid => Type::TEXT,
        SqlDataType::Vector(_) => Type::BYTEA,
        SqlDataType::Geometry => Type::BYTEA,
    }
}
