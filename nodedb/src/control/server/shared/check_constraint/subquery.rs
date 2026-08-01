// SPDX-License-Identifier: BUSL-1.1

//! Subquery CHECK constraint evaluation using a parsed, restricted AST shape.

use std::collections::HashMap;
use std::ops::ControlFlow;

use nodedb_types::DatabaseId;
use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, Value, VisitMut, VisitorMut,
};

use crate::control::security::catalog::types::CheckConstraintDef;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::enforce::ddl_err;
use super::simple::value_to_sql_literal;

const MAX_CHECK_RESPONSE_PAYLOAD_BYTES: usize = 64 * 1024;

/// Evaluate a CHECK constraint containing an `IN (SELECT ...)` expression.
pub(super) async fn enforce_subquery_check(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    constraint: &CheckConstraintDef,
    fields: &HashMap<String, nodedb_types::Value>,
) -> Result<(), DdlError> {
    validate_in_subquery_check(&constraint.check_sql)
        .map_err(|detail| evaluation_error(constraint, &detail))?;
    let mut statements = parse_check_wrapper(&constraint.check_sql, constraint)?;
    let expression = check_wrapper_expression_mut(&mut statements, constraint)?;
    bind_new_references(expression, fields, constraint)?;
    let check = parse_in_subquery_check(&statements, constraint)?;

    // SQL CHECK accepts UNKNOWN. An IN predicate with a NULL left operand is
    // UNKNOWN regardless of the subquery result, so do not issue an unnecessary
    // scan (and do not accidentally turn it into NOT IN success/failure).
    if is_sql_null(&check.lhs) {
        return Ok(());
    }

    let (tasks, _, _lease_scope) =
        plan_authorized_sql(state, identity, &check.match_sql, database_id).await?;
    if tasks.len() != 1 {
        return Err(evaluation_error(
            constraint,
            "CHECK match query did not produce exactly one task",
        ));
    }
    let task = tasks.into_tasks().into_iter().next().ok_or_else(|| {
        evaluation_error(
            constraint,
            "CHECK match query did not produce an executable task",
        )
    })?;
    let response = crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
        state,
        task,
        TraceId::ZERO,
    )
    .await
    .map_err(|error| evaluation_error(constraint, &error.to_string()))?;
    if response.status != crate::bridge::envelope::Status::Ok {
        return Err(evaluation_error(
            constraint,
            "CHECK match query Data Plane execution failed",
        ));
    }

    if response.payload.len() > MAX_CHECK_RESPONSE_PAYLOAD_BYTES {
        return Err(evaluation_error(
            constraint,
            "CHECK match query response exceeds the 64 KiB limit",
        ));
    }
    let payload = crate::data::executor::response_codec::decode_payload_to_json(&response.payload);
    let has_match =
        decode_match_count(&payload).map_err(|detail| evaluation_error(constraint, detail))? > 0;
    let passes = if check.negated { !has_match } else { has_match };
    if passes {
        Ok(())
    } else {
        Err(ddl_err(
            "23514",
            &format!(
                "CHECK constraint '{}' violated: {}",
                constraint.name, constraint.check_sql
            ),
        ))
    }
}

struct InSubqueryCheck {
    lhs: Expr,
    negated: bool,
    match_sql: String,
}

/// Parse an already-bound CHECK AST and serialize only its accepted nodes into
/// the restricted match query. No source fragments are sliced or spliced.
fn parse_in_subquery_check(
    statements: &[Statement],
    constraint: &CheckConstraintDef,
) -> Result<InSubqueryCheck, DdlError> {
    let [Statement::Query(query)] = statements else {
        return Err(evaluation_error(
            constraint,
            "CHECK expression is not one query",
        ));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(evaluation_error(
            constraint,
            "CHECK wrapper is not a SELECT",
        ));
    };
    let [SelectItem::ExprWithAlias { expr, alias }] = select.projection.as_slice() else {
        return Err(evaluation_error(
            constraint,
            "CHECK wrapper has an invalid projection",
        ));
    };
    if alias.value != "_check" {
        return Err(evaluation_error(
            constraint,
            "CHECK wrapper has an invalid result alias",
        ));
    }
    let Expr::InSubquery {
        expr: lhs,
        subquery,
        negated,
    } = strip_nested(expr)
    else {
        return Err(evaluation_error(
            constraint,
            "subquery CHECK must be a top-level IN (SELECT ...) or NOT IN (SELECT ...)",
        ));
    };
    let match_sql = build_match_query(lhs, subquery, *negated, constraint)?;
    Ok(InSubqueryCheck {
        lhs: lhs.as_ref().clone(),
        negated: *negated,
        match_sql,
    })
}

fn parse_check_wrapper(
    check_sql: &str,
    constraint: &CheckConstraintDef,
) -> Result<Vec<Statement>, DdlError> {
    let wrapper = format!("SELECT ({check_sql}) AS _check");
    // reconstructed-sql: parser-only validates and binds the stored CHECK expression AST
    nodedb_sql::parser::statement::parse_sql(&wrapper)
        .map_err(|error| evaluation_error(constraint, &error.to_string()))
}

fn check_wrapper_expression_mut<'a>(
    statements: &'a mut [Statement],
    constraint: &CheckConstraintDef,
) -> Result<&'a mut Expr, DdlError> {
    let [Statement::Query(query)] = statements else {
        return Err(evaluation_error(
            constraint,
            "CHECK expression is not one query",
        ));
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(evaluation_error(
            constraint,
            "CHECK wrapper is not a SELECT",
        ));
    };
    let [SelectItem::ExprWithAlias { expr, alias }] = select.projection.as_mut_slice() else {
        return Err(evaluation_error(
            constraint,
            "CHECK wrapper has an invalid projection",
        ));
    };
    if alias.value != "_check" {
        return Err(evaluation_error(
            constraint,
            "CHECK wrapper has an invalid result alias",
        ));
    }
    Ok(expr)
}

fn bind_new_references(
    expression: &mut Expr,
    fields: &HashMap<String, nodedb_types::Value>,
    constraint: &CheckConstraintDef,
) -> Result<(), DdlError> {
    let mut binder = NewReferenceBinder {
        fields,
        error: None,
    };
    let _ = expression.visit(&mut binder);
    match binder.error {
        Some(detail) => Err(evaluation_error(constraint, &detail)),
        None => Ok(()),
    }
}

struct NewReferenceBinder<'a> {
    fields: &'a HashMap<String, nodedb_types::Value>,
    error: Option<String>,
}

impl VisitorMut for NewReferenceBinder<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::CompoundIdentifier(parts) = expression else {
            return ControlFlow::Continue(());
        };
        if parts.len() != 2
            || parts.iter().any(|part| part.quote_style.is_some())
            || !parts[0].value.eq_ignore_ascii_case("NEW")
        {
            return ControlFlow::Continue(());
        }
        let field_name = parts[1].value.clone();
        let value = self
            .fields
            .iter()
            .find_map(|(name, value)| name.eq_ignore_ascii_case(&field_name).then_some(value))
            .cloned()
            .unwrap_or(nodedb_types::Value::Null);
        if !is_supported_bound_value(&value) {
            self.error = Some(format!(
                "CHECK subquery cannot bind non-scalar NEW field '{field_name}'"
            ));
            return ControlFlow::Break(());
        }
        match parse_literal_expression(&value) {
            Ok(literal) => *expression = literal,
            Err(detail) => {
                self.error = Some(detail);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

fn is_supported_bound_value(value: &nodedb_types::Value) -> bool {
    matches!(
        value,
        nodedb_types::Value::Null
            | nodedb_types::Value::Bool(_)
            | nodedb_types::Value::Integer(_)
            | nodedb_types::Value::Float(_)
            | nodedb_types::Value::String(_)
            | nodedb_types::Value::DateTime(_)
            | nodedb_types::Value::NaiveDateTime(_)
    )
}

fn parse_literal_expression(value: &nodedb_types::Value) -> Result<Expr, String> {
    let literal = value_to_sql_literal(value);
    // reconstructed-sql: parser-only converts one canonical Value literal into an Expr AST
    let statements = nodedb_sql::parser::statement::parse_sql(&format!("SELECT {literal}"))
        .map_err(|error| format!("failed to parse CHECK literal: {error}"))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err("CHECK literal did not produce exactly one query".to_string());
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err("CHECK literal query is not a SELECT".to_string());
    };
    match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(expression)] => Ok(expression.clone()),
        _ => Err("CHECK literal query did not produce exactly one expression".to_string()),
    }
}

/// Validate the one supported subquery CHECK form at DDL time and runtime.
pub(crate) fn validate_in_subquery_check(check_sql: &str) -> Result<(), String> {
    let constraint = CheckConstraintDef {
        name: "CHECK".to_string(),
        check_sql: check_sql.to_string(),
        has_subquery: true,
    };
    let statements = parse_check_wrapper(check_sql, &constraint).map_err(|error| error.message)?;
    let check = parse_in_subquery_check(&statements, &constraint).map_err(|error| error.message)?;
    if !is_supported_unbound_lhs(&check.lhs) {
        return Err(
            "subquery CHECK left operand must be a literal or an unquoted NEW.field reference"
                .to_string(),
        );
    }
    Ok(())
}

fn is_supported_unbound_lhs(expr: &Expr) -> bool {
    match strip_nested(expr) {
        Expr::Value(value) => !matches!(value.value, Value::Placeholder(_)),
        Expr::CompoundIdentifier(parts) => {
            parts.len() == 2
                && parts.iter().all(|part| part.quote_style.is_none())
                && parts[0].value.eq_ignore_ascii_case("NEW")
        }
        _ => false,
    }
}

fn strip_nested(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

fn build_match_query(
    lhs: &Expr,
    subquery: &Query,
    negated: bool,
    constraint: &CheckConstraintDef,
) -> Result<String, DdlError> {
    if subquery.with.is_some()
        || subquery.order_by.is_some()
        || subquery.limit_clause.is_some()
        || subquery.fetch.is_some()
        || !subquery.locks.is_empty()
    {
        return Err(evaluation_error(
            constraint,
            "IN subquery does not support WITH, ORDER BY, LIMIT, FETCH, or locks",
        ));
    }
    let SetExpr::Select(select) = subquery.body.as_ref() else {
        return Err(evaluation_error(
            constraint,
            "IN subquery must have a SELECT body",
        ));
    };
    validate_subquery_shape(select, constraint)?;
    let projection = match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(expr)] => expr,
        [SelectItem::ExprWithAlias { expr, .. }] => expr,
        _ => {
            return Err(evaluation_error(
                constraint,
                "IN subquery must select exactly one expression",
            ));
        }
    };

    let projection_sql = canonical_check_expr_sql(projection);
    let lhs_sql = canonical_check_expr_sql(lhs);
    let from_sql = canonical_check_from_sql(&select.from[0]);
    // IN is accepted by CHECK when a row matches or when a NULL row makes the
    // result UNKNOWN. NOT IN is violated only by an actual equal row; a NULL
    // without a match also yields UNKNOWN and therefore passes CHECK.
    match (negated, select.selection.as_ref()) {
        (true, Some(where_expr)) => Ok(format!(
            "SELECT COUNT(*) AS cnt FROM {from_sql} WHERE ({projection_sql}) = ({lhs_sql}) AND ({})",
            canonical_check_expr_sql(where_expr)
        )),
        (true, None) => Ok(format!(
            "SELECT COUNT(*) AS cnt FROM {from_sql} WHERE ({projection_sql}) = ({lhs_sql})"
        )),
        (false, Some(where_expr)) => Ok(format!(
            "SELECT COUNT(*) AS cnt FROM {from_sql} WHERE (({projection_sql}) = ({lhs_sql}) OR ({projection_sql}) IS NULL) AND ({})",
            canonical_check_expr_sql(where_expr)
        )),
        (false, None) => Ok(format!(
            "SELECT COUNT(*) AS cnt FROM {from_sql} WHERE (({projection_sql}) = ({lhs_sql}) OR ({projection_sql}) IS NULL)"
        )),
    }
}

fn canonical_check_expr_sql(expr: &Expr) -> String {
    expr.to_string()
}

fn canonical_check_from_sql(from: &sqlparser::ast::TableWithJoins) -> String {
    from.to_string()
}

fn validate_subquery_shape(
    select: &Select,
    constraint: &CheckConstraintDef,
) -> Result<(), DdlError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(evaluation_error(
            constraint,
            "IN subquery must have exactly one ordinary FROM relation without joins",
        ));
    }
    if !matches!(
        select.from[0].relation,
        sqlparser::ast::TableFactor::Table { args: None, .. }
    ) {
        return Err(evaluation_error(
            constraint,
            "IN subquery FROM must be an ordinary table relation",
        ));
    }
    let has_grouping = match &select.group_by {
        sqlparser::ast::GroupByExpr::All(_) => true,
        sqlparser::ast::GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
    };
    if select.having.is_some() || has_grouping || select.distinct.is_some() {
        return Err(evaluation_error(
            constraint,
            "IN subquery does not support grouping, HAVING, or DISTINCT",
        ));
    }
    Ok(())
}

fn is_sql_null(expr: &Expr) -> bool {
    matches!(strip_nested(expr), Expr::Value(value) if matches!(value.value, Value::Null))
}

fn decode_match_count(payload: &str) -> Result<u64, &'static str> {
    let value: serde_json::Value = crate::util::bounded_json::from_str(payload)
        .map_err(|_| "CHECK match query returned malformed JSON")?;
    let row = match value {
        serde_json::Value::Object(row) => row,
        serde_json::Value::Array(mut rows) if rows.len() == 1 => match rows.remove(0) {
            serde_json::Value::Object(row) => row,
            _ => return Err("CHECK match query did not return an object row"),
        },
        _ => return Err("CHECK match query did not return exactly one row"),
    };
    if row.len() != 1 {
        return Err("CHECK match query returned an ambiguous row");
    }
    row.get("cnt")
        .and_then(serde_json::Value::as_u64)
        .ok_or("CHECK match query did not return a nonnegative integer count")
}

fn evaluation_error(constraint: &CheckConstraintDef, detail: &str) -> DdlError {
    ddl_err(
        "23514",
        &format!(
            "CHECK constraint '{}' failed to evaluate: {detail}",
            constraint.name
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn constraint() -> CheckConstraintDef {
        CheckConstraintDef {
            name: "c".into(),
            check_sql: "unused".into(),
            has_subquery: true,
        }
    }

    #[test]
    fn match_query_uses_ast_serialization_for_quoted_identifiers_and_literals() {
        let statements = parse_check_wrapper(
            "'x' IN (SELECT \"value\" FROM \"odd FROM WHERE\" WHERE \"note\" = 'WHERE FROM')",
            &constraint(),
        )
        .expect("wrapper");
        let parsed =
            parse_in_subquery_check(&statements, &constraint()).expect("valid IN subquery");
        assert!(parsed.match_sql.contains("\"odd FROM WHERE\""));
        assert!(parsed.match_sql.contains("'WHERE FROM'"));
        assert!(!parsed.negated);
    }

    #[test]
    fn rejects_non_top_level_and_unsupported_subquery_shapes() {
        for expression in [
            "'x' IN (SELECT value FROM roles) OR true",
            "'x' IN (SELECT value FROM roles JOIN other ON true)",
            "'x' IN (SELECT value, other FROM roles)",
            "'x' IN (SELECT value FROM roles GROUP BY value)",
            "NULLIF(NEW.role, NEW.role) IN (SELECT value FROM roles)",
            "\"NEW\".role IN (SELECT value FROM roles)",
            "\"NEW\".\"role\" IN (SELECT value FROM roles)",
            "$1 IN (SELECT value FROM roles)",
            "? IN (SELECT value FROM roles)",
        ] {
            assert!(
                validate_in_subquery_check(expression).is_err(),
                "{expression}"
            );
        }
    }

    #[test]
    fn preserves_not_in_and_null_left_operand_semantics() {
        let not_in = parse_check_wrapper("'x' NOT IN (SELECT value FROM roles)", &constraint())
            .and_then(|statements| parse_in_subquery_check(&statements, &constraint()))
            .expect("NOT IN");
        assert!(not_in.negated);
        let null_lhs = parse_check_wrapper("NULL IN (SELECT value FROM roles)", &constraint())
            .and_then(|statements| parse_in_subquery_check(&statements, &constraint()))
            .expect("NULL IN");
        assert!(is_sql_null(&null_lhs.lhs));
    }

    #[test]
    fn ast_binding_replaces_only_new_compound_identifiers() {
        let mut fields = HashMap::new();
        fields.insert(
            "name".to_string(),
            nodedb_types::Value::String("admin".into()),
        );
        let mut statements = parse_check_wrapper(
            "NEW.name IN (SELECT name FROM roles WHERE note = 'NEW.name') -- NEW.name\n",
            &constraint(),
        )
        .expect("wrapper");
        let expression =
            check_wrapper_expression_mut(&mut statements, &constraint()).expect("expr");
        bind_new_references(expression, &fields, &constraint()).expect("bind");
        let rendered = statements[0].to_string();
        assert!(rendered.contains("'admin' IN"));
        assert!(rendered.contains("'NEW.name'"));
    }

    #[test]
    fn ast_binding_rejects_non_scalar_values_instead_of_binding_null() {
        let mut fields = HashMap::new();
        fields.insert(
            "role".to_string(),
            nodedb_types::Value::Array(vec![nodedb_types::Value::String("admin".into())]),
        );
        let mut statements =
            parse_check_wrapper("NEW.role IN (SELECT name FROM roles)", &constraint())
                .expect("wrapper");
        let expression =
            check_wrapper_expression_mut(&mut statements, &constraint()).expect("expr");
        assert!(bind_new_references(expression, &fields, &constraint()).is_err());
    }

    #[test]
    fn response_payload_limit_is_small_and_explicit() {
        assert_eq!(MAX_CHECK_RESPONSE_PAYLOAD_BYTES, 64 * 1024);
    }

    #[test]
    fn count_decoder_requires_one_nonnegative_integer_count() {
        assert_eq!(decode_match_count("{\"cnt\":0}"), Ok(0));
        assert_eq!(decode_match_count("[{\"cnt\":2}]"), Ok(2));
        for malformed in ["[]", "{\"cnt\":-1}", "{\"cnt\":1.0}", "{\"other\":1}"] {
            assert!(decode_match_count(malformed).is_err(), "{malformed}");
        }
    }
}
