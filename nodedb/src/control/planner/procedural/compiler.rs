// SPDX-License-Identifier: BUSL-1.1

//! Procedural SQL → expression compiler.
//!
//! Compiles a `ProceduralBlock` AST into an equivalent SQL expression string.
//! The output is a single SQL expression (not a statement) that can be registered
//! as a DataFusion ScalarUDF body.
//!
//! **Compilation rules:**
//! - `IF/ELSIF/ELSE` → `CASE WHEN ... THEN ... END`
//! - `DECLARE x := expr` → inlined at each use site
//! - `FOR i IN 1..N LOOP` → unrolled CASE WHEN chain
//! - `RETURN expr` → the expression value at that branch
//! - Subqueries in conditions → passed through (DataFusion plans as semi-joins)

use std::collections::HashMap;

use super::ast::*;
use super::error::ProceduralError;
use super::validate::MAX_LOOP_UNROLL;

/// Compile a procedural block into a single SQL expression string.
///
/// The resulting expression can be used as the body of a DataFusion ScalarUDF.
/// Variables declared with DECLARE are inlined at each use site.
///
/// Returns the compiled SQL expression, or an error if the block cannot be
/// compiled to a single expression.
pub fn compile_to_sql(block: &ProceduralBlock) -> Result<String, ProceduralError> {
    let mut ctx = CompileContext::new();
    compile_statements(&block.statements, &mut ctx)
}

/// Tracks variable bindings for inlining DECLARE/ASSIGN.
struct CompileContext {
    /// Variable name → SQL expression for inlining.
    variables: HashMap<String, String>,
}

impl CompileContext {
    fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    /// Substitute known variables in a SQL expression.
    fn substitute(&self, sql: &str) -> String {
        substitute_identifiers(sql, &self.variables)
    }
}

/// Compile a list of statements into a single SQL expression.
///
/// The last RETURN in the control flow determines the expression's value.
/// If no RETURN is reached, the expression evaluates to NULL.
fn compile_statements(
    stmts: &[Statement],
    ctx: &mut CompileContext,
) -> Result<String, ProceduralError> {
    // Strategy: process statements sequentially. DECLARE and ASSIGN update
    // the variable context. IF and FOR produce CASE WHEN expressions.
    // RETURN sets the final expression value.
    //
    // For a sequence like:
    //   DECLARE x INT := 1;
    //   x := x + 1;
    //   IF x > 1 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF;
    //
    // This compiles to:
    //   CASE WHEN ((1) + 1) > 1 THEN 'yes' ELSE 'no' END

    for stmt in stmts {
        match stmt {
            Statement::Declare { name, default, .. } => {
                let expr = match default {
                    Some(e) => ctx.substitute(&e.sql),
                    None => "NULL".into(),
                };
                ctx.variables.insert(name.to_lowercase(), expr);
            }
            Statement::Assign { target, expr } => {
                let substituted = ctx.substitute(&expr.sql);
                ctx.variables.insert(target.to_lowercase(), substituted);
            }
            Statement::If {
                condition,
                then_block,
                elsif_branches,
                else_block,
            } => {
                return compile_if(condition, then_block, elsif_branches, else_block, ctx);
            }
            Statement::Return { expr } => {
                return Ok(ctx.substitute(&expr.sql));
            }
            Statement::For {
                var,
                start,
                end,
                reverse,
                body,
            } => {
                return compile_for(var, start, end, *reverse, body, ctx);
            }
            Statement::Raise {
                level: RaiseLevel::Exception,
                ..
            } => {
                // RAISE EXCEPTION in a branch terminates that path.
                // We can't represent this directly in a CASE WHEN expression.
                // For now, raise in a non-branching position → compile error.
                return Err(ProceduralError::compile(
                    "RAISE EXCEPTION outside an IF branch cannot be compiled to an expression. \
                     Move it inside an IF block or use CREATE PROCEDURE",
                ));
            }
            Statement::ReturnQuery { query } => {
                // RETURN QUERY compiles to a subquery expression.
                // This enables table-valued functions: the function body becomes
                // `(SELECT ... FROM ... WHERE ...)` which DataFusion can inline.
                return Ok(format!("({})", ctx.substitute(query)));
            }
            // LOOP/WHILE rejected by validator for function bodies.
            Statement::Loop { .. } | Statement::While { .. } => {
                return Err(ProceduralError::compile(
                    "LOOP/WHILE cannot be compiled to an expression. \
                     Use FOR with literal bounds or CREATE PROCEDURE",
                ));
            }
            // Break/Continue should not appear outside loops (validator catches this).
            Statement::Break | Statement::Continue => {
                return Err(ProceduralError::compile("BREAK/CONTINUE outside of a loop"));
            }
            // Raise NOTICE/WARNING — informational, skip (no expression effect).
            Statement::Raise { .. } => {}
            // Side-effecting SQL / transaction control — should be caught by validator.
            Statement::Sql { .. }
            | Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint { .. }
            | Statement::RollbackTo { .. }
            | Statement::ReleaseSavepoint { .. } => {
                return Err(ProceduralError::compile(
                    "DML/transaction control not allowed in function bodies",
                ));
            }
        }
    }

    // If we reach here with no RETURN, the function returns NULL.
    Ok("NULL".into())
}

/// Compile IF/ELSIF/ELSE to CASE WHEN expression.
fn compile_if(
    condition: &SqlExpr,
    then_block: &[Statement],
    elsif_branches: &[ElsIfBranch],
    else_block: &Option<Vec<Statement>>,
    ctx: &mut CompileContext,
) -> Result<String, ProceduralError> {
    let mut parts = Vec::new();

    // WHEN condition THEN result
    let cond_sql = ctx.substitute(&condition.sql);
    let then_sql = compile_statements(then_block, &mut ctx.clone_vars())?;
    parts.push(format!("WHEN {cond_sql} THEN {then_sql}"));

    // ELSIF branches.
    for branch in elsif_branches {
        let cond = ctx.substitute(&branch.condition.sql);
        let body = compile_statements(&branch.body, &mut ctx.clone_vars())?;
        parts.push(format!("WHEN {cond} THEN {body}"));
    }

    // ELSE clause.
    let else_sql = if let Some(else_stmts) = else_block {
        compile_statements(else_stmts, &mut ctx.clone_vars())?
    } else {
        "NULL".into()
    };
    parts.push(format!("ELSE {else_sql}"));

    Ok(format!("CASE {} END", parts.join(" ")))
}

/// Compile FOR loop by unrolling into a nested CASE WHEN chain.
///
/// `FOR i IN 1..3 LOOP RETURN i * 2; END LOOP;`
/// compiles to: `CASE WHEN true THEN (1) * 2 END`
///
/// Since function FOR loops must contain a RETURN (otherwise value is undefined),
/// and the first iteration's RETURN determines the value, unrolling just evaluates
/// the body with each iteration's variable binding.
///
/// For FOR loops with conditional RETURNs:
/// `FOR i IN 1..3 LOOP IF arr[i] > threshold THEN RETURN i; END IF; END LOOP;`
/// → unrolled as nested CASE WHEN for each iteration.
fn compile_for(
    var: &str,
    start: &SqlExpr,
    end: &SqlExpr,
    reverse: bool,
    body: &[Statement],
    ctx: &mut CompileContext,
) -> Result<String, ProceduralError> {
    let start_val = start.sql.trim().parse::<i64>().map_err(|_| {
        ProceduralError::compile("FOR loop start must be integer literal in function bodies")
    })?;
    let end_val = end.sql.trim().parse::<i64>().map_err(|_| {
        ProceduralError::compile("FOR loop end must be integer literal in function bodies")
    })?;

    let iterations: Vec<i64> = if reverse {
        (end_val..=start_val).rev().collect()
    } else {
        (start_val..=end_val).collect()
    };

    if iterations.len() as u64 > MAX_LOOP_UNROLL {
        return Err(ProceduralError::compile(format!(
            "FOR loop has {} iterations, exceeds unrolling threshold ({MAX_LOOP_UNROLL})",
            iterations.len()
        )));
    }

    if iterations.is_empty() {
        return Ok("NULL".into());
    }

    // Unroll: for each iteration, bind the variable and compile the body.
    // Use nested CASE WHEN to try each iteration in order.
    // The first RETURN hit determines the value.
    let mut case_parts = Vec::new();
    for val in &iterations {
        let mut iter_ctx = ctx.clone_vars();
        iter_ctx
            .variables
            .insert(var.to_lowercase(), val.to_string());
        let body_sql = compile_statements(body, &mut iter_ctx)?;
        // Each iteration becomes a WHEN clause. If the body is unconditional
        // (always returns), it's `WHEN true THEN <body>`. If conditional,
        // the inner CASE WHEN handles it.
        if body_sql != "NULL" {
            case_parts.push(format!("WHEN true THEN {body_sql}"));
            // For unconditional returns, first iteration wins.
            if !body.iter().any(|s| matches!(s, Statement::If { .. })) {
                break;
            }
        }
    }

    if case_parts.is_empty() {
        return Ok("NULL".into());
    }

    case_parts.push("ELSE NULL".into());
    Ok(format!("CASE {} END", case_parts.join(" ")))
}

impl CompileContext {
    /// Create a snapshot of the current variable bindings.
    /// Used for branches (IF/ELSE) that shouldn't leak assignments to siblings.
    fn clone_vars(&self) -> Self {
        Self {
            variables: self.variables.clone(),
        }
    }
}

/// Substitute all known variables in one lexical pass. Replacement text is
/// copied, never revisited, so binding expansion is deterministic.
fn substitute_identifiers(sql: &str, variables: &HashMap<String, String>) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut cursor = 0;
    while cursor < sql.len() {
        let end = opaque_end(sql, cursor);
        if end > cursor {
            output.push_str(&sql[cursor..end]);
            cursor = end;
        } else if let Some(ch) = sql[cursor..].chars().next() {
            if is_identifier_start(ch) {
                let end = consume_identifier(sql, cursor);
                let token = &sql[cursor..end];
                if let Some(value) = variables.get(&token.to_lowercase()) {
                    output.push('(');
                    output.push_str(value);
                    output.push(')');
                } else {
                    output.push_str(token);
                }
                cursor = end;
            } else {
                output.push(ch);
                cursor += ch.len_utf8();
            }
        }
    }
    output
}

fn opaque_end(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    match bytes[start] {
        b'\'' | b'"' => consume_quoted(sql, start, bytes[start]),
        b'-' if bytes.get(start + 1) == Some(&b'-') => sql[start..]
            .find('\n')
            .map_or(sql.len(), |offset| start + offset),
        b'/' if bytes.get(start + 1) == Some(&b'*') => consume_block_comment(sql, start),
        b'$' => consume_dollar_quote(sql, start).unwrap_or(start),
        _ => start,
    }
}

fn consume_quoted(sql: &str, start: usize, quote: u8) -> usize {
    let bytes = sql.as_bytes();
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        if bytes[cursor] == quote {
            cursor += 1;
            if bytes.get(cursor) != Some(&quote) {
                return cursor;
            }
            cursor += 1;
        } else {
            cursor += sql[cursor..].chars().next().map_or(1, char::len_utf8);
        }
    }
    sql.len()
}

fn consume_block_comment(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut cursor = start + 2;
    let mut depth = 1usize;
    while cursor + 1 < bytes.len() {
        match (bytes[cursor], bytes[cursor + 1]) {
            (b'/', b'*') => {
                depth += 1;
                cursor += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                cursor += 2;
                if depth == 0 {
                    return cursor;
                }
            }
            _ => cursor += sql[cursor..].chars().next().map_or(1, char::len_utf8),
        }
    }
    sql.len()
}

fn consume_dollar_quote(sql: &str, start: usize) -> Option<usize> {
    let rest = &sql[start..];
    let close = rest[1..].find('$')? + 1;
    let tag = &rest[..=close];
    let tag_body = &tag[1..tag.len() - 1];
    if !tag_body.is_empty()
        && (!tag_body
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_alphabetic())
            || !tag_body.chars().all(|ch| ch == '_' || ch.is_alphanumeric()))
    {
        return None;
    }
    let body_start = start + tag.len();
    Some(
        sql[body_start..]
            .find(tag)
            .map_or(sql.len(), |offset| body_start + offset + tag.len()),
    )
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_alphabetic()
}
fn is_identifier_continue(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_alphanumeric()
}
fn consume_identifier(sql: &str, start: usize) -> usize {
    sql[start..]
        .char_indices()
        .find_map(|(offset, ch)| {
            (offset > 0 && !is_identifier_continue(ch)).then_some(start + offset)
        })
        .unwrap_or(sql.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block(stmts: Vec<Statement>) -> ProceduralBlock {
        ProceduralBlock {
            statements: stmts,
            exception_handlers: Vec::new(),
        }
    }

    #[test]
    fn compile_simple_return() {
        let block = make_block(vec![Statement::Return {
            expr: SqlExpr::new("42"),
        }]);
        assert_eq!(compile_to_sql(&block).unwrap(), "42");
    }

    #[test]
    fn compile_if_else() {
        let block = make_block(vec![Statement::If {
            condition: SqlExpr::new("x > 0"),
            then_block: vec![Statement::Return {
                expr: SqlExpr::new("'positive'"),
            }],
            elsif_branches: vec![],
            else_block: Some(vec![Statement::Return {
                expr: SqlExpr::new("'non-positive'"),
            }]),
        }]);
        let sql = compile_to_sql(&block).unwrap();
        assert!(sql.contains("CASE"));
        assert!(sql.contains("WHEN x > 0 THEN 'positive'"));
        assert!(sql.contains("ELSE 'non-positive'"));
    }

    #[test]
    fn compile_if_elsif_else() {
        let block = make_block(vec![Statement::If {
            condition: SqlExpr::new("score > 0.9"),
            then_block: vec![Statement::Return {
                expr: SqlExpr::new("'critical'"),
            }],
            elsif_branches: vec![
                ElsIfBranch {
                    condition: SqlExpr::new("score > 0.7"),
                    body: vec![Statement::Return {
                        expr: SqlExpr::new("'high'"),
                    }],
                },
                ElsIfBranch {
                    condition: SqlExpr::new("score > 0.4"),
                    body: vec![Statement::Return {
                        expr: SqlExpr::new("'medium'"),
                    }],
                },
            ],
            else_block: Some(vec![Statement::Return {
                expr: SqlExpr::new("'low'"),
            }]),
        }]);
        let sql = compile_to_sql(&block).unwrap();
        assert_eq!(
            sql,
            "CASE WHEN score > 0.9 THEN 'critical' \
             WHEN score > 0.7 THEN 'high' \
             WHEN score > 0.4 THEN 'medium' \
             ELSE 'low' END"
        );
    }

    #[test]
    fn compile_declare_and_inline() {
        let block = make_block(vec![
            Statement::Declare {
                name: "threshold".into(),
                data_type: "FLOAT".into(),
                default: Some(SqlExpr::new("0.5")),
            },
            Statement::If {
                condition: SqlExpr::new("score > threshold"),
                then_block: vec![Statement::Return {
                    expr: SqlExpr::new("'high'"),
                }],
                elsif_branches: vec![],
                else_block: Some(vec![Statement::Return {
                    expr: SqlExpr::new("'low'"),
                }]),
            },
        ]);
        let sql = compile_to_sql(&block).unwrap();
        // threshold should be inlined as (0.5)
        assert!(sql.contains("score > (0.5)"));
    }

    #[test]
    fn compile_variable_reassign() {
        let block = make_block(vec![
            Statement::Declare {
                name: "x".into(),
                data_type: "INT".into(),
                default: Some(SqlExpr::new("1")),
            },
            Statement::Assign {
                target: "x".into(),
                expr: SqlExpr::new("x + 1"),
            },
            Statement::Return {
                expr: SqlExpr::new("x"),
            },
        ]);
        let sql = compile_to_sql(&block).unwrap();
        // x starts as 1, then x := x+1 = (1)+1, then RETURN x = ((1) + 1)
        assert!(sql.contains("(1) + 1"));
    }

    #[test]
    fn compile_for_loop_unrolled() {
        let block = make_block(vec![Statement::For {
            var: "i".into(),
            start: SqlExpr::new("1"),
            end: SqlExpr::new("3"),
            reverse: false,
            body: vec![Statement::Return {
                expr: SqlExpr::new("i * 2"),
            }],
        }]);
        let sql = compile_to_sql(&block).unwrap();
        // First iteration: i=1, body = "(1) * 2" (variable inlined with parens).
        assert!(sql.contains("(1) * 2"), "actual: {sql}");
    }

    #[test]
    fn replaces_only_whole_identifier_tokens() {
        let variables = HashMap::from([("x".to_string(), "5".to_string())]);
        assert_eq!(substitute_identifiers("x + 1", &variables), "(5) + 1");
        assert_eq!(substitute_identifiers("max(x)", &variables), "max((5))");
        assert_eq!(substitute_identifiers("text", &variables), "text");
    }

    #[test]
    fn replacement_is_one_pass_and_respects_unicode_dollar_boundaries() {
        let mut variables = HashMap::new();
        variables.insert("x".into(), "y".into());
        variables.insert("y".into(), "9".into());
        variables.insert("é".into(), "11".into());
        assert_eq!(
            substitute_identifiers("x y αx xβ x$ É é", &variables),
            "(y) (9) αx xβ x$ (11) (11)"
        );
    }

    #[test]
    fn replacement_preserves_unicode_and_opaque_sql_regions() {
        let variables = HashMap::from([("x".to_string(), "5".to_string())]);
        let sql = "x + '雪x' + \"x\" -- x 雪\n/* outer x /* inner x */ 雪 */ + x";
        assert_eq!(
            substitute_identifiers(sql, &variables),
            "(5) + '雪x' + \"x\" -- x 雪\n/* outer x /* inner x */ 雪 */ + (5)"
        );
    }

    #[test]
    fn no_return_gives_null() {
        let block = make_block(vec![Statement::Declare {
            name: "x".into(),
            data_type: "INT".into(),
            default: Some(SqlExpr::new("1")),
        }]);
        assert_eq!(compile_to_sql(&block).unwrap(), "NULL");
    }
}
