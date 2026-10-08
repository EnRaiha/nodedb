// SPDX-License-Identifier: Apache-2.0

//! Arguments of the `graph_score(node_id_col, 'seed', depth => N, label => 'edge')`
//! leg of a three-source `rrf_score(...)`.
//!
//! Two positional arguments: the node-id column (the executor resolves
//! surrogates from the collection) and the seed node id, a string literal.
//! The options are a closed set, each named with `=>`: `depth` (a positive
//! integer, default 1) and `label` (a string literal, default every label).
//! A third positional argument, an unknown or repeated option, `=` in place
//! of `=>`, and a value of the wrong type are typed errors.

use sqlparser::ast::{self, FunctionArgOperator};

use super::super::helpers::extract_string_literal;
use crate::error::{Result, SqlError};

/// The options `graph_score` accepts.
const OPTION_NAMES: &str = "depth, label";

/// The BFS spec of a `graph_score(...)` leg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GraphScoreArgs {
    pub seed_id: String,
    pub depth: usize,
    pub edge_label: Option<String>,
}

/// Parse the argument list of a `graph_score(...)` call.
pub(super) fn parse_graph_score_args(func: &ast::Function) -> Result<GraphScoreArgs> {
    let ast::FunctionArguments::List(list) = &func.args else {
        return Err(arity_error());
    };
    let mut positional: Vec<&ast::Expr> = Vec::new();
    let mut depth: Option<usize> = None;
    let mut edge_label: Option<String> = None;
    for arg in &list.args {
        let (key, value, operator) = match arg {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                if positional.len() >= 2 {
                    return Err(invalid(format!(
                        "graph_score() takes two positional arguments (node_id_col, 'seed'); \
                         name the options: {OPTION_NAMES} (e.g. depth => 2), got {expr}"
                    )));
                }
                positional.push(expr);
                continue;
            }
            ast::FunctionArg::Unnamed(other) => {
                return Err(invalid(format!(
                    "graph_score() expects a value expression, got {other}"
                )));
            }
            ast::FunctionArg::Named {
                name,
                arg,
                operator,
            } => (name.value.to_ascii_lowercase(), arg, operator),
            ast::FunctionArg::ExprNamed {
                name: ast::Expr::Identifier(ident),
                arg,
                operator,
            } => (ident.value.to_ascii_lowercase(), arg, operator),
            ast::FunctionArg::ExprNamed { name, .. } => {
                return Err(invalid(format!(
                    "graph_score(): option name {name} is not an identifier; \
                     name an option: {OPTION_NAMES}"
                )));
            }
        };
        if *operator != FunctionArgOperator::RightArrow {
            return Err(invalid(format!(
                "graph_score(): use '=>' not '{operator}' for option '{key}'"
            )));
        }
        let ast::FunctionArgExpr::Expr(value) = value else {
            return Err(invalid(format!(
                "graph_score(): option '{key}' expects a value, got {value}"
            )));
        };
        match key.as_str() {
            "depth" => {
                if depth.is_some() {
                    return Err(repeated(&key));
                }
                depth = Some(parse_depth(value)?);
            }
            "label" => {
                if edge_label.is_some() {
                    return Err(repeated(&key));
                }
                edge_label = Some(extract_string_literal(value).map_err(|_| {
                    invalid(format!(
                        "graph_score(): option 'label' expects a string literal, got {value}"
                    ))
                })?);
            }
            _ => {
                return Err(invalid(format!(
                    "graph_score(): unknown option '{key}'; the options are {OPTION_NAMES}"
                )));
            }
        }
    }
    let [_, seed] = positional.as_slice() else {
        return Err(arity_error());
    };
    let seed_id = extract_string_literal(seed).map_err(|_| {
        invalid(format!(
            "graph_score(): the seed node id must be a string literal, got {seed}"
        ))
    })?;
    Ok(GraphScoreArgs {
        seed_id,
        depth: depth.unwrap_or(1),
        edge_label,
    })
}

/// A `depth => N` value: a positive integer literal.
fn parse_depth(value: &ast::Expr) -> Result<usize> {
    let error = || {
        invalid(format!(
            "graph_score(): option 'depth' expects a positive integer, got {value}"
        ))
    };
    let ast::Expr::Value(v) = value else {
        return Err(error());
    };
    let ast::Value::Number(n, _) = &v.value else {
        return Err(error());
    };
    match n.parse::<usize>() {
        Ok(depth) if depth > 0 => Ok(depth),
        _ => Err(error()),
    }
}

fn arity_error() -> SqlError {
    SqlError::Arity {
        detail: "graph_score() takes (node_id_col, 'seed', [depth => N, label => 'edge'])".into(),
    }
}

fn repeated(key: &str) -> SqlError {
    invalid(format!("graph_score(): option '{key}' is given twice"))
}

fn invalid(detail: String) -> SqlError {
    SqlError::InvalidFunction { detail }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::*;

    fn parse(call: &str) -> Result<GraphScoreArgs> {
        let sql = format!("SELECT {call}");
        let statements = Parser::parse_sql(&GenericDialect {}, &sql).expect("parses");
        let ast::Statement::Query(query) = &statements[0] else {
            panic!("expected a query");
        };
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a select");
        };
        let ast::SelectItem::UnnamedExpr(ast::Expr::Function(func)) = &select.projection[0] else {
            panic!("expected a function call");
        };
        parse_graph_score_args(func)
    }

    #[test]
    fn named_options_reach_the_spec() {
        assert_eq!(
            parse("graph_score(id, 'n1', depth => 2, label => 'hop')").expect("valid"),
            GraphScoreArgs {
                seed_id: "n1".into(),
                depth: 2,
                edge_label: Some("hop".into()),
            }
        );
        assert_eq!(
            parse("graph_score(id, 'n1')").expect("valid"),
            GraphScoreArgs {
                seed_id: "n1".into(),
                depth: 1,
                edge_label: None,
            }
        );
    }

    #[test]
    fn malformed_arguments_are_typed_errors() {
        for call in [
            "graph_score(id, 7)",
            "graph_score(id, 'n1', depth => 0)",
            "graph_score(id, 'n1', depth => 1.5)",
            "graph_score(id, 'n1', depth => 'x')",
            "graph_score(id, 'n1', label => 3)",
            "graph_score(id, 'n1', hops => 2)",
            "graph_score(id, 'n1', depth => 1, depth => 2)",
            "graph_score(id, 'n1', 2)",
        ] {
            let err = parse(call).expect_err(call);
            assert!(
                matches!(err, SqlError::InvalidFunction { .. }),
                "{call}: {err:?}"
            );
        }
        assert!(matches!(
            parse("graph_score(id)").expect_err("one argument"),
            SqlError::Arity { .. }
        ));
    }
}
