// SPDX-License-Identifier: BUSL-1.1

//! Typeguard field definitions turned into strict-schema columns.
//!
//! `CONVERT COLLECTION <name> TO document_strict` with no explicit column list
//! reads the collection's active typeguards instead. With or without a list,
//! each guard's `DEFAULT` / `VALUE` is carried onto the converted column it
//! names.

use nodedb_types::TypeGuardFieldDef;
use nodedb_types::columnar::ColumnDef;

use super::super::super::result::DdlError;
use super::super::column_default::{DeclaredColumn, validate_carried_default};
use super::support::err;
use super::type_map::typeguard_type_to_column_type;

/// Convert typeguard field definitions to strict schema column definitions.
///
/// Each typeguard field becomes a column. The type expression is mapped to
/// ColumnType, and REQUIRED fields become NOT NULL. Dot-path fields are
/// skipped: a strict schema is flat. The key column and the guard clauses are
/// added by the caller.
pub(super) fn typeguards_to_column_defs(
    guards: &[TypeGuardFieldDef],
) -> Result<Vec<ColumnDef>, DdlError> {
    let mut columns: Vec<ColumnDef> = Vec::new();

    for guard in guards {
        if guard.field.contains('.') || columns.iter().any(|c| c.name == guard.field) {
            continue;
        }

        let ct = typeguard_type_to_column_type(&guard.type_expr).map_err(|e| {
            err(
                &e.sqlstate,
                format!("field '{}': {}", guard.field, e.message),
            )
        })?;
        let col = if guard.required {
            ColumnDef::required(guard.field.clone(), ct)
        } else {
            ColumnDef::nullable(guard.field.clone(), ct)
        }
        .with_declared_width(&guard.type_expr);
        columns.push(col);
    }

    Ok(columns)
}

/// Carry each guard's `DEFAULT` / `VALUE` onto the converted column it names.
///
/// A guard `VALUE` evaluates on every write and always overwrites, which is a
/// stored generated column. A guard `DEFAULT` becomes a column `DEFAULT`,
/// which is evaluated with no row in scope, so one that reads another column
/// is refused. A `DEFAULT` the column list declares itself wins over a guard
/// `DEFAULT`.
///
/// `generated_allowed` is false for a KV target, whose write path computes no
/// generated column.
///
/// Returns each generated column with the expression text it was declared
/// with.
pub(super) fn carry_guard_clauses(
    columns: &mut [ColumnDef],
    guards: &[TypeGuardFieldDef],
    generated_allowed: bool,
) -> Result<Vec<(String, String)>, DdlError> {
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut generated = Vec::new();
    for guard in guards {
        let Some(column) = columns.iter_mut().find(|c| c.name == guard.field) else {
            continue;
        };
        if let Some(expr) = guard.value_expr.as_deref() {
            carry_value(column, expr, &names, generated_allowed)?;
            generated.push((column.name.clone(), expr.to_string()));
        } else if let Some(expr) = guard.default_expr.as_deref()
            && column.default.is_none()
        {
            validate_carried_default(
                "typeguard DEFAULT",
                &DeclaredColumn {
                    name: &column.name,
                    declared_type: &column.declared_type_name(),
                    primary_key: column.primary_key,
                },
                expr,
            )?;
            column.default = Some(expr.to_string());
        }
    }
    Ok(generated)
}

/// Turn `column` into the generated column a guard `VALUE` declares.
///
/// The expression must read only columns of the converted schema, must not
/// read its own column, and cannot land on the primary key or on a column the
/// list gives a `DEFAULT`.
fn carry_value(
    column: &mut ColumnDef,
    expr: &str,
    names: &[String],
    generated_allowed: bool,
) -> Result<(), DdlError> {
    let field = column.name.clone();
    if !generated_allowed {
        return Err(err(
            "0A000",
            format!(
                "field '{field}': typeguard VALUE has no equivalent on a kv collection; drop the \
                 guard VALUE before CONVERT"
            ),
        ));
    }
    if column.primary_key {
        return Err(err(
            "0A000",
            format!("field '{field}': typeguard VALUE cannot carry onto the primary key column"),
        ));
    }
    if column.default.is_some() {
        return Err(err(
            "42601",
            format!(
                "field '{field}': the column list declares a DEFAULT and the typeguard declares \
                 VALUE"
            ),
        ));
    }
    let (parsed, deps) = nodedb_query::expr_parse::parse_generated_expr(expr).map_err(|e| {
        err(
            "42601",
            format!("field '{field}': typeguard VALUE '{expr}' is invalid: {e}"),
        )
    })?;
    if deps.contains(&field) {
        return Err(err(
            "0A000",
            format!(
                "field '{field}': typeguard VALUE reads its own field and has no generated-column \
                 equivalent"
            ),
        ));
    }
    if let Some(missing) = deps.iter().find(|dep| !names.contains(dep)) {
        return Err(err(
            "42703",
            format!(
                "field '{field}': typeguard VALUE reads '{missing}', which is not a column of the \
                 converted schema"
            ),
        ));
    }
    let serialized = sonic_rs::to_string(&parsed)
        .map_err(|e| DdlError::internal(format!("generated expression serialization: {e}")))?;
    column.generated_expr = Some(serialized);
    column.generated_deps = deps;
    column.nullable = true;
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnType;

    use super::*;

    fn guard(field: &str, type_expr: &str) -> TypeGuardFieldDef {
        TypeGuardFieldDef {
            field: field.to_string(),
            type_expr: type_expr.to_string(),
            required: false,
            check_expr: None,
            default_expr: None,
            value_expr: None,
        }
    }

    /// An unresolvable guard type stops CONVERT instead of storing a column
    /// type the guard never declared.
    #[test]
    fn typeguards_to_column_defs_refuses_an_unresolvable_guard_type() {
        let guards = vec![guard("gadget", "WIDGET")];
        let error =
            typeguards_to_column_defs(&guards).expect_err("an unresolvable guard type must refuse");
        assert_eq!(error.sqlstate, "42601");
        assert!(
            error.message.contains("gadget"),
            "the error must name the field: {}",
            error.message
        );
    }

    /// A guard DEFAULT the column path can evaluate reaches the column.
    #[test]
    fn typeguards_to_column_defs_carry_a_literal_default() {
        let mut status = guard("status", "STRING");
        status.default_expr = Some("'draft'".to_string());
        let guards = [status];
        let mut columns = typeguards_to_column_defs(&guards).expect("the guard converts");
        carry_guard_clauses(&mut columns, &guards, true).expect("a literal DEFAULT carries");
        assert_eq!(columns[0].name, "status");
        assert_eq!(columns[0].default.as_deref(), Some("'draft'"));
    }

    /// A guard VALUE becomes a generated column, so it keeps overwriting on
    /// every write instead of filling only an omitted field.
    #[test]
    fn typeguards_to_column_defs_carry_a_value_expression() {
        let mut computed = guard("computed", "STRING");
        computed.value_expr = Some("'server'".to_string());
        let guards = [computed];
        let mut columns = typeguards_to_column_defs(&guards).expect("the guard converts");
        let generated = carry_guard_clauses(&mut columns, &guards, true).expect("a VALUE carries");
        assert!(columns[0].generated_expr.is_some());
        assert!(columns[0].default.is_none());
        assert_eq!(
            generated,
            [("computed".to_string(), "'server'".to_string())]
        );
    }

    /// A carried-over expression naming an unregistered function stops CONVERT
    /// with the SQLSTATE every other DEFAULT source raises for it.
    #[test]
    fn typeguards_to_column_defs_refuse_an_unevaluable_default() {
        let mut status = guard("status", "STRING");
        status.default_expr = Some("no_such_function()".to_string());
        let guards = [status];
        let mut columns = typeguards_to_column_defs(&guards).expect("the guard converts");
        let error = carry_guard_clauses(&mut columns, &guards, true)
            .expect_err("an unevaluable carried-over DEFAULT must refuse");
        assert_eq!(error.sqlstate, "42883");
    }

    /// A guard DEFAULT that reads another field has no column-DEFAULT
    /// equivalent, so CONVERT refuses it, naming field and clause.
    #[test]
    fn a_cross_field_guard_default_is_refused() {
        let mut lowered = guard("lowered", "STRING");
        lowered.default_expr = Some("LOWER(status)".to_string());
        let guards = [guard("status", "STRING"), lowered];
        let mut columns = typeguards_to_column_defs(&guards).expect("the guards convert");
        let error = carry_guard_clauses(&mut columns, &guards, true)
            .expect_err("a cross-field DEFAULT must refuse");
        assert_eq!(error.sqlstate, "0A000");
        assert!(error.message.contains("lowered"), "{}", error.message);
        assert!(
            error.message.contains("typeguard DEFAULT"),
            "{}",
            error.message
        );
        assert!(error.message.contains("status"), "{}", error.message);
    }

    /// A guard VALUE that reads a field the converted schema lacks would read
    /// NULL on every write, so CONVERT refuses it.
    #[test]
    fn a_guard_value_reading_a_missing_column_is_refused() {
        let mut lowered = guard("lowered", "STRING");
        lowered.value_expr = Some("LOWER(status)".to_string());
        let guards = [lowered];
        let mut columns = typeguards_to_column_defs(&guards).expect("the guard converts");
        let error = carry_guard_clauses(&mut columns, &guards, true)
            .expect_err("a missing dependency must refuse");
        assert_eq!(error.sqlstate, "42703");
    }

    /// A KV write path computes no generated column, so a guard VALUE has no
    /// equivalent there.
    #[test]
    fn a_kv_target_refuses_a_guard_value() {
        let mut computed = guard("computed", "STRING");
        computed.value_expr = Some("'server'".to_string());
        let guards = [computed];
        let mut columns = typeguards_to_column_defs(&guards).expect("the guard converts");
        let error = carry_guard_clauses(&mut columns, &guards, false)
            .expect_err("a kv target must refuse VALUE");
        assert_eq!(error.sqlstate, "0A000");
    }

    /// A DEFAULT the column list declares wins over the guard's.
    #[test]
    fn a_listed_default_wins_over_a_guard_default() {
        let mut status = guard("status", "STRING");
        status.default_expr = Some("'draft'".to_string());
        let mut columns =
            vec![ColumnDef::nullable("status", ColumnType::String).with_default("'x'")];
        carry_guard_clauses(&mut columns, &[status], true).expect("the list wins");
        assert_eq!(columns[0].default.as_deref(), Some("'x'"));
    }
}
