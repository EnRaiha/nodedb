// SPDX-License-Identifier: BUSL-1.1

//! Pure JSON/plan projection and flattening helpers for SELECT responses.
//!
//! These operate purely on parsed SQL and `serde_json::Value` — no pgwire
//! wire types — so they are shared across any protocol-specific response
//! shaper. The pgwire-only encode glue that turns these into `DataRow`s
//! stays in `pgwire::handler::projection`.

/// Projection item from a parsed SELECT list.
#[derive(Clone)]
pub enum ProjectionItem {
    /// SELECT *
    Star,
    /// SELECT col  /  SELECT tbl.col  /  SELECT expr AS alias
    ///
    /// `lookup_key` is the key used to look up the value in the flat row
    /// object emitted by the Data Plane. For qualified references like
    /// `table.column` the Data Plane emits `"table.column"` as the key
    /// (prefix-merged by the join executor), so `lookup_key` preserves the
    /// full dot-joined form. `display_name` is the column label sent to the
    /// client (the last identifier segment, matching PostgreSQL behaviour).
    Named {
        lookup_key: String,
        display_name: String,
    },
}

/// Parse the SELECT projection list from `sql`. Returns `None` if the SQL is
/// not a simple SELECT or parsing fails; returns `Some([Star])` for `SELECT *`.
pub fn parse_select_projection(sql: &str) -> Option<Vec<ProjectionItem>> {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // NodeDB temporal clauses (`AS OF SYSTEM TIME`, `FOR SYSTEM_TIME`,
    // `AS OF VALID TIME`, ...) are extensions sqlparser cannot parse. Strip
    // them first — reusing the same preprocessing the planner uses — so the
    // SELECT list still reprojects into flat columns. Without this, a temporal
    // SELECT skips column projection and leaks the raw `{id,data}` envelope.
    let stripped = match nodedb_sql::parser::preprocess::temporal::extract(sql) {
        Ok(Some(extracted)) => extracted.sql,
        _ => sql.to_string(),
    };
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &stripped).ok()?;
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
                let (lookup_key, display_name) = expr_column_names(expr);
                out.push(ProjectionItem::Named {
                    lookup_key,
                    display_name,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                // The alias is the display name. The lookup key is the
                // underlying expression's full qualified name.
                let (lookup_key, _) = expr_column_names(expr);
                out.push(ProjectionItem::Named {
                    lookup_key,
                    display_name: alias.value.clone(),
                });
            }
        }
    }
    Some(out)
}

/// Returns `(lookup_key, display_name)` for an expression in the SELECT list.
///
/// For a plain `Identifier` both are the same bare column name.
/// For a `CompoundIdentifier` (e.g. `table.column`):
/// - `lookup_key` is the full dot-joined form (`"table.column"`) because the
///   join executor prefixes every key with its source collection name.
/// - `display_name` is the last segment (`"column"`) to match PostgreSQL
///   client expectations.
pub fn expr_column_names(expr: &sqlparser::ast::Expr) -> (String, String) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => {
            let name = id.value.clone();
            (name.clone(), name)
        }
        Expr::CompoundIdentifier(parts) => {
            let lookup_key = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            let display_name = parts
                .last()
                .map(|p| p.value.clone())
                .unwrap_or_else(|| lookup_key.clone());
            (lookup_key, display_name)
        }
        other => {
            // Normalize to lowercase so that aggregate functions like COUNT(*)
            // produce lookup keys ("count(*)") that match the canonical aggregate
            // key format used by the Data Plane response ("count(*)").
            let s = other.to_string().to_lowercase();
            (s.clone(), s)
        }
    }
}

/// Returns true when the projection list contains at least one non-Star named
/// column (i.e. we need to apply projection rather than pass through).
pub fn needs_projection(items: &[ProjectionItem]) -> bool {
    items
        .iter()
        .any(|i| matches!(i, ProjectionItem::Named { .. }))
}

/// Build the ordered list of lookup keys that correspond to `fields_for_projection`.
///
/// Callers pass this alongside `result_fields` to `reproject_response` so
/// that qualified column references (`table.column`) are resolved against the
/// join-prefixed keys the Data Plane emits.
pub fn lookup_keys_for_projection(items: &[ProjectionItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            ProjectionItem::Named { lookup_key, .. } => Some(lookup_key.clone()),
            ProjectionItem::Star => None,
        })
        .collect()
}

/// Convert a JSON scalar value to its PostgreSQL text-format string.
///
/// - `String` values are returned as-is (no extra quoting).
/// - `Bool` uses PostgreSQL text format: `t` for true, `f` for false.
/// - All other scalars (`Number`, `Array`, `Object`) use their JSON
///   `Display` representation; arrays/objects should not normally appear
///   as individual cell values but are rendered faithfully.
pub fn json_value_to_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        // PostgreSQL text format for boolean is `t`/`f`.
        serde_json::Value::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        other => other.to_string(),
    }
}

/// Flatten a parsed JSON value into row objects.
pub fn push_flat_rows(
    value: serde_json::Value,
    out: &mut Vec<serde_json::Map<String, serde_json::Value>>,
) {
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                push_flat_rows(item, out);
            }
        }
        serde_json::Value::Object(mut map) => {
            if is_scan_wrapper(&map)
                && let Some(serde_json::Value::Object(inner)) = map.remove("data")
            {
                out.push(inner);
                return;
            }
            out.push(map);
        }
        _ => {}
    }
}

/// The Data Plane's raw document-scan codec emits objects with exactly
/// the keys `id` (string) and `data` (object). This is the wire shape
/// we unwrap before column projection.
pub fn is_scan_wrapper(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.len() == 2
        && matches!(map.get("id"), Some(serde_json::Value::String(_)))
        && matches!(map.get("data"), Some(serde_json::Value::Object(_)))
}
