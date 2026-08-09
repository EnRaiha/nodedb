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
/// Mirrors the prefix checks in the protocol-neutral DDL router so the
/// extended-query Parse handler can mark such statements as DSL passthroughs
/// and route them through the DSL dispatcher at Execute time.
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
    // router but not recognised by nodedb_sql::ddl_ast::parse.
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
        // Kind-qualified index drops: recognized by the DDL router, rejected by
        // the SQL parser, so they must bypass Parse-time schema inference the
        // same way their CREATE counterparts do.
        || upper.starts_with("DROP VECTOR INDEX ")
        || upper.starts_with("DROP FULLTEXT INDEX ")
        || upper.starts_with("DROP SEARCH INDEX ")
        || upper.starts_with("DROP SPATIAL INDEX ")
        || upper.starts_with("DROP SPARSE INDEX ")
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
    use nodedb_sql::types::SqlDataType;
    use pgwire::api::results::FieldFormat;

    // Local, self-contained mapping from the planner's `SqlDataType` to a
    // pgwire `Type`; kept private to this function since RETURNING is the
    // only remaining caller after the DESCRIBE path moved to the planner's
    // authoritative `OutputSchema` (see `ddl_col_type_to_pg` in parser.rs).
    fn returning_col_type_to_pg(dt: &SqlDataType) -> Type {
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

    // Every write that can carry a RETURNING clause resolves its target here.
    // A write whose target is missed announces NO result columns while its
    // response still ships one field per stored column, which the client
    // cannot read against the RowDescription it was given — so the list is
    // the write plans, not just the two that first needed it.
    let collection = match plan? {
        nodedb_sql::SqlPlan::Update { collection, .. }
        | nodedb_sql::SqlPlan::UpdateFrom { collection, .. }
        | nodedb_sql::SqlPlan::Delete { collection, .. }
        | nodedb_sql::SqlPlan::Insert { collection, .. }
        | nodedb_sql::SqlPlan::KvInsert { collection, .. }
        | nodedb_sql::SqlPlan::Upsert { collection, .. }
        | nodedb_sql::SqlPlan::TimeseriesIngest { collection, .. }
        | nodedb_sql::SqlPlan::VectorPrimaryInsert { collection, .. } => collection.as_str(),
        nodedb_sql::SqlPlan::Merge { target, .. }
        | nodedb_sql::SqlPlan::InsertSelect { target, .. } => target.as_str(),
        _ => return None,
    };

    let info = catalog
        .get_collection(DatabaseId::DEFAULT, collection)
        .ok()
        .flatten()?;

    let columns_to_field_info = |columns: &[nodedb_sql::ColumnInfo]| -> Vec<FieldInfo> {
        columns
            .iter()
            .map(|c| {
                FieldInfo::new(
                    c.name.clone(),
                    None,
                    None,
                    returning_col_type_to_pg(&c.data_type),
                    FieldFormat::Text,
                )
            })
            .collect()
    };

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
                    .map(|c| returning_col_type_to_pg(&c.data_type))
                    .unwrap_or(Type::TEXT);
                FieldInfo::new(display_name, None, None, pg_type, FieldFormat::Text)
            })
            .collect(),
    };
    Some(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_placeholders_basic() {
        assert_eq!(count_placeholders("SELECT $1, $2, $3"), 3);
        assert_eq!(count_placeholders("SELECT 1"), 0);
        assert_eq!(count_placeholders("WHERE id = $1 AND name = $1"), 1);
    }

    #[test]
    fn count_placeholders_malformed_body_unaffected() {
        assert_eq!(count_placeholders("SELECT $"), 0);
        assert_eq!(count_placeholders("SELECT $abc"), 0);
    }

    #[test]
    fn count_placeholders_bounded_against_absurd_index() {
        // Must not attempt a `Vec` sized off an attacker-controlled index
        // downstream — the shared scanner refuses to track it at all.
        assert_eq!(count_placeholders("SELECT $99999999999999"), 0);
        assert_eq!(count_placeholders("SELECT $65536"), 0);
        assert_eq!(count_placeholders("SELECT $65535"), 65535);
    }
}
