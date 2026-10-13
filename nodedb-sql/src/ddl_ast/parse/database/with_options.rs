// SPDX-License-Identifier: Apache-2.0

//! Shared `WITH (key = value, ...)` reader for database DDL.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::error::SqlError;

/// Read an optional `WITH (key = value, ...)` at the cursor. A value is a
/// quoted string or a run of tokens such as `eu-west` or `1.5`. Returns an
/// empty vec when the next token is not `WITH`.
pub(super) fn parse_with_options(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<Vec<(String, String)>, SqlError> {
    if !c.eat_keyword("WITH") {
        return Ok(Vec::new());
    }
    c.paren_list("WITH options", |c| {
        let key = c.word("option name")?.to_string();
        c.expect_punct('=')?;
        let value = match c.string(&key) {
            Ok(value) => value,
            Err(_) => {
                let raw = c.raw_run(&key)?;
                raw.trim_matches('"').to_string()
            }
        };
        Ok((key, value))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::clause::tokenize;

    fn options(sql: &str) -> Result<Vec<(String, String)>, SqlError> {
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "CREATE DATABASE");
        parse_with_options(&mut c)
    }

    #[test]
    fn with_clause_values_keep_their_text() {
        assert_eq!(
            options("WITH (region='eu', tier = gold)").expect("parses"),
            vec![
                ("region".to_string(), "eu".to_string()),
                ("tier".to_string(), "gold".to_string())
            ]
        );
    }

    #[test]
    fn a_pair_without_equals_is_refused() {
        assert!(options("WITH (region)").is_err());
    }
}
