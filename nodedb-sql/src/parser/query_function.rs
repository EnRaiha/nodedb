// SPDX-License-Identifier: Apache-2.0

//! Recognition of the Origin query functions from the parsed statement.
//!
//! `VERIFY_AUDIT_CHAIN`, `VERIFY_HASH_CHAIN`, `BALANCE_AS_OF`,
//! `TEMPORAL_LOOKUP`, `VERIFY_BALANCE` and `CONVERT_CURRENCY_LOOKUP` each run
//! as a whole statement: `SELECT <FUNC>(<literal>, ...)`. A lexer gate keeps
//! the extra parse off ordinary statements. It passes only a `SELECT` whose
//! plain text (outside literals, quoted identifiers and comments) names one of
//! the functions. The parsed AST then decides.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, Statement,
    UnaryOperator, Value,
};

use crate::error::SqlError;
use crate::parser::preprocess::lex;
use crate::parser::statement::parse_sql;

/// One query function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryFunction {
    VerifyAuditChain,
    VerifyHashChain,
    BalanceAsOf,
    TemporalLookup,
    VerifyBalance,
    ConvertCurrencyLookup,
}

impl QueryFunction {
    pub const ALL: [Self; 6] = [
        Self::VerifyAuditChain,
        Self::VerifyHashChain,
        Self::BalanceAsOf,
        Self::TemporalLookup,
        Self::VerifyBalance,
        Self::ConvertCurrencyLookup,
    ];

    /// The function name as SQL spells it.
    pub fn name(self) -> &'static str {
        match self {
            Self::VerifyAuditChain => "VERIFY_AUDIT_CHAIN",
            Self::VerifyHashChain => "VERIFY_HASH_CHAIN",
            Self::BalanceAsOf => "BALANCE_AS_OF",
            Self::TemporalLookup => "TEMPORAL_LOOKUP",
            Self::VerifyBalance => "VERIFY_BALANCE",
            Self::ConvertCurrencyLookup => "CONVERT_CURRENCY_LOOKUP",
        }
    }

    /// The argument list, as error messages show it.
    fn signature(self) -> &'static str {
        match self {
            Self::VerifyAuditChain => "([from_seq [, to_seq]])",
            Self::VerifyHashChain => "(collection)",
            Self::BalanceAsOf => "(collection, key, column, timestamp)",
            Self::TemporalLookup => "(table, key_value, as_of, key_column, time_column)",
            Self::VerifyBalance => "(collection, column)",
            Self::ConvertCurrencyLookup => {
                "(amount, from, to, rate_table, as_of, key_column, rate_column, time_column, precision)"
            }
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|function| function.name().eq_ignore_ascii_case(name))
    }
}

/// `VERIFY_AUDIT_CHAIN([from_seq [, to_seq]])`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyAuditChainArgs {
    pub from_seq: Option<String>,
    pub to_seq: Option<String>,
}

/// `VERIFY_HASH_CHAIN(collection)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyHashChainArgs {
    pub collection: String,
}

/// `BALANCE_AS_OF(collection, key, column, timestamp)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceAsOfArgs {
    pub collection: String,
    pub key: String,
    pub column: String,
    pub timestamp: String,
}

/// `TEMPORAL_LOOKUP(table, key_value, as_of, key_column, time_column)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalLookupArgs {
    pub table: String,
    pub key_value: String,
    pub as_of: String,
    pub key_column: String,
    pub time_column: String,
}

/// `VERIFY_BALANCE(collection, column)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyBalanceArgs {
    pub collection: String,
    pub column: String,
}

/// `CONVERT_CURRENCY_LOOKUP(amount, from, to, rate_table, as_of, key_column,
/// rate_column, time_column, precision)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvertCurrencyLookupArgs {
    pub amount: String,
    pub from: String,
    pub to: String,
    pub rate_table: String,
    pub as_of: String,
    pub key_column: String,
    pub rate_column: String,
    pub time_column: String,
    pub precision: String,
}

/// One recognized call. Each argument holds the literal's text: a string's
/// content (quotes removed, escapes resolved) or a number as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryFunctionCall {
    VerifyAuditChain(VerifyAuditChainArgs),
    VerifyHashChain(VerifyHashChainArgs),
    BalanceAsOf(BalanceAsOfArgs),
    TemporalLookup(TemporalLookupArgs),
    VerifyBalance(VerifyBalanceArgs),
    ConvertCurrencyLookup(ConvertCurrencyLookupArgs),
}

/// Recognize a query-function statement.
///
/// `None` means the statement is not a query-function call. `Some(Err)` means
/// it names a query function in a form the function does not accept.
pub fn parse_query_function(sql: &str) -> Option<Result<QueryFunctionCall, SqlError>> {
    if !lex::first_sql_word(sql)?.eq_ignore_ascii_case("SELECT") {
        return None;
    }
    if !QueryFunction::ALL
        .iter()
        .any(|function| lex::keyword_position_outside_literals(sql, function.name()).is_some())
    {
        return None;
    }
    // A statement sqlparser refuses goes to the planner, which reports the
    // parse error itself.
    let statements = parse_sql(sql).ok()?;
    if let [statement] = statements.as_slice()
        && let Some((function, args)) = standalone_call(statement)
    {
        return Some(bind(function, args));
    }
    statements
        .iter()
        .find_map(named_function)
        .map(|function| Err(misplaced(function)))
}

/// The call when `statement` is exactly `SELECT <FUNC>(...)`.
fn standalone_call(statement: &Statement) -> Option<(QueryFunction, &[FunctionArg])> {
    let Statement::Query(query) = statement else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let [SelectItem::UnnamedExpr(expr)] = select.projection.as_slice() else {
        return None;
    };
    let Expr::Function(func) = expr else {
        return None;
    };
    // Any other clause (FROM, WHERE, LIMIT, WITH, DISTINCT, ...) changes the
    // rendered statement.
    if statement.to_string() != format!("SELECT {expr}") {
        return None;
    }
    if func.over.is_some()
        || func.filter.is_some()
        || !func.within_group.is_empty()
        || func.null_treatment.is_some()
        || func.uses_odbc_syntax
        || !matches!(func.parameters, FunctionArguments::None)
    {
        return None;
    }
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    let function = QueryFunction::from_name(&func.name.to_string())?;
    Some((function, list.args.as_slice()))
}

/// The first query function the statement calls anywhere.
fn named_function(statement: &Statement) -> Option<QueryFunction> {
    let found = sqlparser::ast::visit_expressions(statement, |expr| match expr {
        Expr::Function(func) => match QueryFunction::from_name(&func.name.to_string()) {
            Some(function) => ControlFlow::Break(function),
            None => ControlFlow::Continue(()),
        },
        _ => ControlFlow::Continue(()),
    });
    match found {
        ControlFlow::Break(function) => Some(function),
        ControlFlow::Continue(()) => None,
    }
}

fn bind(function: QueryFunction, args: &[FunctionArg]) -> Result<QueryFunctionCall, SqlError> {
    let values = args
        .iter()
        .enumerate()
        .map(|(index, arg)| literal(function, index + 1, arg))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(match function {
        QueryFunction::VerifyAuditChain => {
            if values.len() > 2 {
                return Err(arity(function));
            }
            let mut values = values.into_iter();
            QueryFunctionCall::VerifyAuditChain(VerifyAuditChainArgs {
                from_seq: values.next(),
                to_seq: values.next(),
            })
        }
        QueryFunction::VerifyHashChain => {
            let [collection] = exact(function, values)?;
            QueryFunctionCall::VerifyHashChain(VerifyHashChainArgs { collection })
        }
        QueryFunction::BalanceAsOf => {
            let [collection, key, column, timestamp] = exact(function, values)?;
            QueryFunctionCall::BalanceAsOf(BalanceAsOfArgs {
                collection,
                key,
                column,
                timestamp,
            })
        }
        QueryFunction::TemporalLookup => {
            let [table, key_value, as_of, key_column, time_column] = exact(function, values)?;
            QueryFunctionCall::TemporalLookup(TemporalLookupArgs {
                table,
                key_value,
                as_of,
                key_column,
                time_column,
            })
        }
        QueryFunction::VerifyBalance => {
            let [collection, column] = exact(function, values)?;
            QueryFunctionCall::VerifyBalance(VerifyBalanceArgs { collection, column })
        }
        QueryFunction::ConvertCurrencyLookup => {
            let [
                amount,
                from,
                to,
                rate_table,
                as_of,
                key_column,
                rate_column,
                time_column,
                precision,
            ] = exact(function, values)?;
            QueryFunctionCall::ConvertCurrencyLookup(ConvertCurrencyLookupArgs {
                amount,
                from,
                to,
                rate_table,
                as_of,
                key_column,
                rate_column,
                time_column,
                precision,
            })
        }
    })
}

fn exact<const N: usize>(
    function: QueryFunction,
    values: Vec<String>,
) -> Result<[String; N], SqlError> {
    <[String; N]>::try_from(values).map_err(|_| arity(function))
}

/// The text of one literal argument.
fn literal(
    function: QueryFunction,
    position: usize,
    arg: &FunctionArg,
) -> Result<String, SqlError> {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
        return Err(not_literal(function, position, &arg.to_string()));
    };
    let refused = || not_literal(function, position, &expr.to_string());
    match expr {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::EscapedStringLiteral(text) => Ok(text.clone()),
            Value::Number(number, _) => Ok(number.clone()),
            _ => Err(refused()),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            Expr::Value(value) => match &value.value {
                Value::Number(number, _) => Ok(format!("-{number}")),
                _ => Err(refused()),
            },
            _ => Err(refused()),
        },
        _ => Err(refused()),
    }
}

fn arity(function: QueryFunction) -> SqlError {
    SqlError::Parse {
        detail: format!("{} requires {}", function.name(), function.signature()),
    }
}

fn not_literal(function: QueryFunction, position: usize, found: &str) -> SqlError {
    SqlError::Parse {
        detail: format!(
            "{} argument {position} must be a string or number literal — found {found}",
            function.name()
        ),
    }
}

fn misplaced(function: QueryFunction) -> SqlError {
    let name = function.name();
    SqlError::Unsupported {
        detail: format!(
            "{name}(...) runs only as a whole statement: SELECT {name}{}",
            function.signature()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(sql: &str) -> QueryFunctionCall {
        parse_query_function(sql)
            .expect("statement is a query-function call")
            .expect("call is well-formed")
    }

    fn refusal(sql: &str) -> SqlError {
        parse_query_function(sql)
            .expect("statement names a query function")
            .expect_err("call must be refused")
    }

    #[test]
    fn routes_standalone_call() {
        let expected = QueryFunctionCall::VerifyBalance(VerifyBalanceArgs {
            collection: "c".into(),
            column: "col".into(),
        });
        assert_eq!(call("SELECT VERIFY_BALANCE('c','col')"), expected);
        assert_eq!(call("select verify_balance('c', 'col')"), expected);
        assert_eq!(call("/* x */ SELECT VERIFY_BALANCE('c', 'col');"), expected);
    }

    #[test]
    fn ignores_name_in_literal_comment_or_identifier() {
        for sql in [
            "INSERT INTO t (id) VALUES ('tmp_verify_balance_x')",
            "SELECT 'verify_balance' FROM t",
            "SELECT 1 -- verify_balance note",
            "SELECT \"verify_balance\" FROM t",
            "SELECT verify_balance FROM t",
        ] {
            assert!(parse_query_function(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn comma_and_escaped_quote_inside_literal() {
        match call("SELECT TEMPORAL_LOOKUP('t', 'a,b', '2024-01-01', 'k', 'ts')") {
            QueryFunctionCall::TemporalLookup(args) => {
                assert_eq!(args.table, "t");
                assert_eq!(args.key_value, "a,b");
                assert_eq!(args.time_column, "ts");
            }
            other => panic!("expected TemporalLookup, got {other:?}"),
        }
        assert_eq!(
            call("SELECT VERIFY_HASH_CHAIN('it''s')"),
            QueryFunctionCall::VerifyHashChain(VerifyHashChainArgs {
                collection: "it's".into()
            })
        );
    }

    #[test]
    fn arity_is_exact() {
        for sql in [
            "SELECT VERIFY_BALANCE('c')",
            "SELECT VERIFY_BALANCE('a', 'b', 'c')",
        ] {
            let error = refusal(sql);
            assert!(
                error
                    .to_string()
                    .contains("VERIFY_BALANCE requires (collection, column)"),
                "{sql}: {error}"
            );
        }
        assert!(matches!(
            refusal("SELECT VERIFY_AUDIT_CHAIN(1, 2, 3)"),
            SqlError::Parse { .. }
        ));
    }

    #[test]
    fn audit_chain_without_args_defaults() {
        assert_eq!(
            call("SELECT VERIFY_AUDIT_CHAIN()"),
            QueryFunctionCall::VerifyAuditChain(VerifyAuditChainArgs {
                from_seq: None,
                to_seq: None,
            })
        );
        assert_eq!(
            call("SELECT VERIFY_AUDIT_CHAIN(1, 100)"),
            QueryFunctionCall::VerifyAuditChain(VerifyAuditChainArgs {
                from_seq: Some("1".into()),
                to_seq: Some("100".into()),
            })
        );
    }

    #[test]
    fn misplaced_call_is_refused() {
        for sql in [
            "SELECT id, VERIFY_BALANCE('c', 'col') FROM t",
            "SELECT VERIFY_BALANCE('c', 'col') AS r",
            "SELECT VERIFY_BALANCE('c', 'col') FROM t",
        ] {
            let error = refusal(sql);
            assert!(
                matches!(error, SqlError::Unsupported { .. }),
                "{sql}: {error}"
            );
            assert!(error.to_string().contains("whole statement"), "{error}");
        }
    }

    #[test]
    fn non_literal_argument_is_refused() {
        let error = refusal("SELECT VERIFY_HASH_CHAIN(upper('x'))");
        assert!(error.to_string().contains("argument 1"), "{error}");
    }

    #[test]
    fn negative_number_keeps_its_sign() {
        match call(
            "SELECT CONVERT_CURRENCY_LOOKUP(-5, 'USD', 'EUR', 'rates', '2024-01-01', \
             'pair', 'rate', 'ts', 2)",
        ) {
            QueryFunctionCall::ConvertCurrencyLookup(args) => {
                assert_eq!(args.amount, "-5");
                assert_eq!(args.precision, "2");
            }
            other => panic!("expected ConvertCurrencyLookup, got {other:?}"),
        }
    }
}
