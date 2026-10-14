// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP MATERIALIZED VIEW + CREATE/DROP CONTINUOUS AGGREGATE.

use super::clause::{ClauseCursor, Tok, Token, tokenize};
use super::helpers::if_exists_name;
use crate::ddl_ast::statement::{NodedbStatement, StreamViewStmt};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("SHOW MATERIALIZED VIEWS") {
        return Some(Ok(NodedbStatement::StreamView(
            StreamViewStmt::ShowMaterializedViews,
        )));
    }
    if upper.starts_with("SHOW CONTINUOUS AGGREGATES") {
        return Some(Ok(NodedbStatement::StreamView(
            StreamViewStmt::ShowContinuousAggregates,
        )));
    }
    let known = [
        "CREATE MATERIALIZED VIEW ",
        "DROP MATERIALIZED VIEW ",
        "CREATE CONTINUOUS AGGREGATE ",
        "DROP CONTINUOUS AGGREGATE ",
        "REFRESH CONTINUOUS AGGREGATE",
    ];
    if !known.iter().any(|p| upper.starts_with(p)) {
        return None;
    }
    Some(parse(trimmed).map(NodedbStatement::StreamView))
}

fn parse(sql: &str) -> Result<StreamViewStmt, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "MATERIALIZED VIEW");
    if c.peek_keywords(&["CREATE", "MATERIALIZED", "VIEW"]) {
        return parse_create_mv(sql, &toks);
    }
    if c.eat_keywords(&["DROP", "MATERIALIZED", "VIEW"]) {
        let (if_exists, name) = if_exists_name(&mut c, "view name")?;
        c.finish()?;
        return Ok(StreamViewStmt::DropMaterializedView { name, if_exists });
    }
    if c.eat_keywords(&["CREATE", "CONTINUOUS", "AGGREGATE"]) {
        return parse_create_continuous_aggregate(&mut c);
    }
    if c.eat_keywords(&["DROP", "CONTINUOUS", "AGGREGATE"]) {
        let (if_exists, name) = if_exists_name(&mut c, "aggregate name")?;
        c.finish()?;
        return Ok(StreamViewStmt::DropContinuousAggregate { name, if_exists });
    }
    c.expect_keywords(&["REFRESH", "CONTINUOUS", "AGGREGATE"])?;
    if c.at_end() {
        return Err(c.error(
            "REFRESH CONTINUOUS AGGREGATE is missing the aggregate name; \
             write REFRESH CONTINUOUS AGGREGATE <name>",
        ));
    }
    let name = c.ident("aggregate name")?;
    if !c.at_end() {
        return Err(c.error(format!(
            "REFRESH CONTINUOUS AGGREGATE {name} takes no clause after the name"
        )));
    }
    Ok(StreamViewStmt::RefreshContinuousAggregate { name })
}

/// Index of the `WITH` token of a trailing `WITH ( ... )` options clause —
/// one that ends the statement — or `None`. A `WITH` that opens a CTE or sits
/// inside the query is not it.
fn trailing_with_options(toks: &[Token<'_>]) -> Option<usize> {
    let mut end = toks.len();
    if matches!(
        toks.get(end.checked_sub(1)?).map(|t| &t.tok),
        Some(Tok::Punct(';'))
    ) {
        end -= 1;
    }
    let last = end.checked_sub(1)?;
    if toks[last].tok != Tok::Punct(')') {
        return None;
    }
    let mut depth = 0i32;
    for i in (0..=last).rev() {
        match toks[i].tok {
            Tok::Punct(')') => depth += 1,
            Tok::Punct('(') => {
                depth -= 1;
                if depth == 0 {
                    let with = i.checked_sub(1)?;
                    return matches!(&toks[with].tok, Tok::Word(w) if w.eq_ignore_ascii_case("WITH"))
                        .then_some(with);
                }
            }
            _ => {}
        }
    }
    None
}

/// Read `CREATE MATERIALIZED VIEW <name> [ON <source>] [STREAMING] AS
/// <select> [WITH (REFRESH = <mode>)]`.
///
/// A periodic view requires both `ON <source>` and `AS <select>`; a missing
/// clause is a syntax error here rather than a catalog lookup on an empty
/// name downstream. A STREAMING view sources from a change stream named in
/// its own FROM clause, so it takes no `ON` collection.
fn parse_create_mv<'a>(sql: &'a str, toks: &[Token<'a>]) -> Result<StreamViewStmt, SqlError> {
    let with_at = trailing_with_options(toks);
    let head = &toks[..with_at.unwrap_or(toks.len())];
    let mut c = ClauseCursor::new(sql, head, "CREATE MATERIALIZED VIEW");
    c.expect_keywords(&["CREATE", "MATERIALIZED", "VIEW"])?;
    if c.peek_keyword("ON") || c.peek_keyword("AS") || c.at_end() {
        return Err(c.error(
            "missing the view name; write CREATE MATERIALIZED VIEW <name> ON \
             <source_collection> AS <select>",
        ));
    }
    let name = c.name("view name")?;
    let source = if c.eat_keyword("ON") {
        c.name("source collection")?
    } else {
        String::new()
    };
    let streaming_keyword = c.eat_keyword("STREAMING");
    if !c.eat_keyword("AS") {
        return Err(c.error(format!(
            "{name} is missing the required AS <select> clause; write CREATE MATERIALIZED \
             VIEW {name} ON <source_collection> AS <select>"
        )));
    }
    let query_sql = c.rest().to_string();
    if query_sql.is_empty() {
        return Err(c.error(format!(
            "{name} is missing the required AS <select> clause; write CREATE MATERIALIZED \
             VIEW {name} ON <source_collection> AS <select>"
        )));
    }

    let mut refresh_mode = if streaming_keyword {
        "STREAMING"
    } else {
        "FULL"
    }
    .to_string();
    if let Some(at) = with_at {
        let mut w = ClauseCursor::new(sql, &toks[at..], "CREATE MATERIALIZED VIEW");
        w.expect_keyword("WITH")?;
        let options = w.paren_list("WITH options", |w| {
            let key = w.word("option name")?.to_ascii_uppercase();
            w.expect_punct('=')?;
            let value = w.raw_run("option value")?;
            Ok((
                key,
                value.trim_matches('\'').trim_matches('"').to_uppercase(),
            ))
        })?;
        w.finish()?;
        for (key, value) in options {
            if key != "REFRESH" {
                return Err(w.error(format!("unknown option '{key}' (expected REFRESH)")));
            }
            refresh_mode = value;
        }
    }

    if !refresh_mode.eq_ignore_ascii_case("STREAMING") && source.is_empty() {
        return Err(c.error(format!(
            "{name} is missing the required ON <source_collection> clause; write CREATE \
             MATERIALIZED VIEW {name} ON <source_collection> AS <select>"
        )));
    }

    Ok(StreamViewStmt::CreateMaterializedView {
        name,
        source,
        query_sql,
        refresh_mode,
    })
}

/// Read `<name> ON <source> BUCKET '<interval>' AGGREGATE <exprs>
/// [GROUP BY <cols>] [WITH (...)]` after `CREATE CONTINUOUS AGGREGATE`.
fn parse_create_continuous_aggregate(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<StreamViewStmt, SqlError> {
    let name = c.name("aggregate name")?;
    c.expect_keyword("ON")?;
    let source = c.name("source collection")?;
    c.expect_keyword("BUCKET")?;
    let bucket_raw = match c.string("BUCKET") {
        Ok(bucket) => bucket,
        Err(_) => c.raw_run("BUCKET")?.to_string(),
    };
    c.expect_keyword("AGGREGATE")?;
    let aggregate_exprs_raw = c
        .raw_until("AGGREGATE", &[&["GROUP", "BY"], &["WITH"]])?
        .to_string();
    let group_by = if c.eat_keywords(&["GROUP", "BY"]) {
        let mut cols = Vec::new();
        loop {
            cols.push(c.name("GROUP BY column")?);
            if !c.eat_punct(',') {
                break;
            }
        }
        cols
    } else {
        Vec::new()
    };
    let with_clause_raw = if c.eat_keyword("WITH") {
        c.raw_paren("WITH")?.to_string()
    } else {
        String::new()
    };
    c.finish()?;
    Ok(StreamViewStmt::CreateContinuousAggregate {
        name,
        source,
        bucket_raw,
        aggregate_exprs_raw,
        group_by,
        with_clause_raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_sql(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&sql.to_uppercase(), &parts, sql)
    }

    fn mv(sql: &str) -> Result<StreamViewStmt, SqlError> {
        let toks = tokenize(sql);
        parse_create_mv(sql, &toks)
    }

    #[test]
    fn parse_mv_unicode_name_preserves_original_offsets() {
        let sql = "CREATE MATERIALIZED VIEW summaryﬀﬀ ON orders AS SELECT count(*) FROM orders";
        let StreamViewStmt::CreateMaterializedView {
            name,
            source,
            query_sql,
            ..
        } = mv(sql).expect("valid CREATE MATERIALIZED VIEW")
        else {
            panic!("expected CreateMaterializedView");
        };
        assert_eq!(name, "summaryﬀﬀ");
        assert_eq!(source, "orders");
        assert_eq!(query_sql, "SELECT count(*) FROM orders");
    }

    #[test]
    fn parse_mv_basic() {
        let sql = "CREATE MATERIALIZED VIEW summary ON orders AS SELECT count(*) FROM orders";
        let StreamViewStmt::CreateMaterializedView {
            name,
            source,
            query_sql,
            refresh_mode,
        } = mv(sql).expect("valid CREATE MATERIALIZED VIEW")
        else {
            panic!("expected CreateMaterializedView");
        };
        assert_eq!(name, "summary");
        assert_eq!(source, "orders");
        assert!(query_sql.contains("count(*)"));
        assert_eq!(refresh_mode, "FULL");
    }

    #[test]
    fn join_on_inside_the_query_is_not_the_source_clause() {
        let sql = "CREATE MATERIALIZED VIEW v ON a AS SELECT a.id FROM a JOIN b ON a.id = b.id";
        let StreamViewStmt::CreateMaterializedView {
            source, query_sql, ..
        } = mv(sql).expect("parses")
        else {
            panic!("expected CreateMaterializedView");
        };
        assert_eq!(source, "a");
        assert_eq!(query_sql, "SELECT a.id FROM a JOIN b ON a.id = b.id");
        // Without ON the join's ON is never read as the source.
        assert!(mv("CREATE MATERIALIZED VIEW v AS SELECT * FROM a JOIN b ON a.id = b.id").is_err());
    }

    #[test]
    fn streaming_keyword_sets_the_mode() {
        let sql = "CREATE MATERIALIZED VIEW s ON o STREAMING AS SELECT count(*) FROM o";
        let StreamViewStmt::CreateMaterializedView { refresh_mode, .. } = mv(sql).expect("parses")
        else {
            panic!("expected CreateMaterializedView");
        };
        assert_eq!(refresh_mode, "STREAMING");
    }

    #[test]
    fn streaming_inside_the_query_sets_nothing() {
        let sql = "CREATE MATERIALIZED VIEW s ON o AS SELECT 'STREAMING' AS label FROM o";
        let StreamViewStmt::CreateMaterializedView { refresh_mode, .. } = mv(sql).expect("parses")
        else {
            panic!("expected CreateMaterializedView");
        };
        assert_eq!(refresh_mode, "FULL");
    }

    #[test]
    fn parse_continuous_aggregate_unicode_name_preserves_original_offsets() {
        let sql = "CREATE CONTINUOUS AGGREGATE hourlyﬀﬀ ON metrics BUCKET '1h' AGGREGATE avg(cpu) AS mean_cpu";
        let Some(Ok(NodedbStatement::StreamView(StreamViewStmt::CreateContinuousAggregate {
            name,
            source,
            bucket_raw,
            aggregate_exprs_raw,
            ..
        }))) = try_sql(sql)
        else {
            panic!("expected CreateContinuousAggregate");
        };
        assert_eq!(name, "hourlyﬀﬀ");
        assert_eq!(source, "metrics");
        assert_eq!(bucket_raw, "1h");
        assert_eq!(aggregate_exprs_raw, "avg(cpu) AS mean_cpu");
    }

    #[test]
    fn continuous_aggregate_group_by_and_with() {
        let sql = "CREATE CONTINUOUS AGGREGATE c ON m BUCKET '5m' AGGREGATE SUM(value) AS t \
                   GROUP BY host, region WITH (refresh_policy = 'manual')";
        let Some(Ok(NodedbStatement::StreamView(StreamViewStmt::CreateContinuousAggregate {
            group_by,
            with_clause_raw,
            ..
        }))) = try_sql(sql)
        else {
            panic!("expected CreateContinuousAggregate");
        };
        assert_eq!(group_by, vec!["host", "region"]);
        assert_eq!(with_clause_raw, "refresh_policy = 'manual'");
    }

    #[test]
    fn continuous_aggregate_trailing_tokens_are_refused() {
        let sql = "CREATE CONTINUOUS AGGREGATE c ON m BUCKET '5m' AGGREGATE SUM(v) AS t WITH (a = 1) EXTRA";
        assert!(matches!(try_sql(sql), Some(Err(SqlError::Parse { .. }))));
    }

    #[test]
    fn parse_refresh_continuous_aggregate() {
        let stmt = try_sql("REFRESH CONTINUOUS AGGREGATE hourly_avg")
            .expect("claimed")
            .expect("parses");
        assert_eq!(
            stmt,
            NodedbStatement::StreamView(StreamViewStmt::RefreshContinuousAggregate {
                name: "hourly_avg".into()
            })
        );
    }

    #[test]
    fn parse_refresh_continuous_aggregate_without_name_is_a_syntax_error() {
        assert!(matches!(
            try_sql("REFRESH CONTINUOUS AGGREGATE"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }

    #[test]
    fn parse_refresh_continuous_aggregate_with_trailing_clause_is_a_syntax_error() {
        assert!(matches!(
            try_sql("REFRESH CONTINUOUS AGGREGATE hourly_avg WITH DATA"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }

    #[test]
    fn parse_continuous_aggregate_basic() {
        let sql = "CREATE CONTINUOUS AGGREGATE hourly_avg ON metrics BUCKET '1h' AGGREGATE avg(cpu) AS mean_cpu";
        let Some(Ok(NodedbStatement::StreamView(StreamViewStmt::CreateContinuousAggregate {
            name,
            source,
            bucket_raw,
            aggregate_exprs_raw,
            ..
        }))) = try_sql(sql)
        else {
            panic!("expected CreateContinuousAggregate");
        };
        assert_eq!(name, "hourly_avg");
        assert_eq!(source, "metrics");
        assert_eq!(bucket_raw, "1h");
        assert!(aggregate_exprs_raw.to_uppercase().contains("AVG(CPU)"));
    }

    #[test]
    fn parse_mv_without_on_clause_is_a_syntax_error() {
        let err = mv("CREATE MATERIALIZED VIEW summary AS SELECT count(*) FROM orders")
            .expect_err("ON is required for a periodic view");
        let SqlError::Parse { detail } = err else {
            panic!("expected a parse error");
        };
        assert!(detail.contains("ON <source_collection>"), "{detail}");
        assert!(detail.contains("summary"), "{detail}");
    }

    #[test]
    fn parse_streaming_mv_needs_no_on_clause() {
        let sql = "CREATE MATERIALIZED VIEW live AS SELECT count(*) FROM orders WITH (refresh='STREAMING')";
        let StreamViewStmt::CreateMaterializedView {
            name,
            source,
            refresh_mode,
            query_sql,
        } = mv(sql).expect("streaming view takes no ON clause")
        else {
            panic!("expected CreateMaterializedView");
        };
        assert_eq!(name, "live");
        assert!(source.is_empty());
        assert_eq!(refresh_mode, "STREAMING");
        assert_eq!(query_sql, "SELECT count(*) FROM orders");
    }
}
