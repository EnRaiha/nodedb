// SPDX-License-Identifier: Apache-2.0

//! Parse maintenance: ANALYZE, COMPACT, SHOW COMPACTION STATUS, SHOW STORAGE.

use crate::ddl_ast::statement::{ClusterStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    _trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Option<NodedbStatement> {
        // `ANALYZE`/`COMPACT` are keyword-boundary matched: bare `ANALYZE`
        // (whole statement, no target) or `ANALYZE <name>`. A bare prefix
        // match would absorb statements like `ANALYZE users(id)` — actually
        // `ANALYZE` followed by a parenthesised payload — into the collection
        // name. PostgreSQL has no `ANALYZE ... (cols)` form; any trailing
        // `(...)` after the collection name is stripped so the name stays
        // clean.
        if parts.first().is_some_and(|p| p.eq_ignore_ascii_case("ANALYZE")) {
            let collection = parts
                .get(1)
                .map(|s| s.split('(').next().unwrap_or(s).trim().to_string());
            return Some(NodedbStatement::Cluster(ClusterStmt::Analyze {
                collection,
            }));
        }
        if parts.first().is_some_and(|p| p.eq_ignore_ascii_case("COMPACT")) {
            let collection = parts
                .get(1)?
                .split('(')
                .next()
                .unwrap_or(parts.get(1)?)
                .trim()
                .to_string();
            return Some(NodedbStatement::Cluster(ClusterStmt::Compact {
                collection,
            }));
        }
        if upper.starts_with("SHOW COMPACTION ST") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowCompactionStatus));
        }
        if upper.starts_with("SHOW STORAGE") {
            let collection = parts.get(2).map(|s| s.to_string());
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowStorage {
                collection,
            }));
        }
        None
    })()
    .map(Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s2_analyze_parenthesized_payload_absorbs_token() {
        // CLAIM S2: starts_with("ANALYZE") + raw parts[1] absorbs "(id)".
        let sql = "ANALYZE users(id)";
        let out = try_parse(
            sql.to_uppercase().as_str(),
            &sql.split_whitespace().collect::<Vec<_>>(),
            sql,
        );
        let Some(Ok(NodedbStatement::Cluster(ClusterStmt::Analyze { collection }))) = out else {
            panic!("expected Analyze statement, got {out:?}");
        };
        assert_eq!(
            collection.as_deref(),
            Some("users"),
            "S2: ANALYZE users(id) must yield collection 'users', not 'users(id)'"
        );
    }

    #[test]
    fn s2_analyze_tab_whitespace_is_accepted() {
        let sql = "ANALYZE\tusers(id)";
        let out = try_parse(
            &sql.to_uppercase().replace('\t', " "),
            &sql.split_whitespace().collect::<Vec<_>>(),
            sql,
        );
        let Some(Ok(NodedbStatement::Cluster(ClusterStmt::Analyze { collection }))) = out else {
            panic!("expected Analyze statement, got {out:?}");
        };
        assert_eq!(collection.as_deref(), Some("users"));
    }

    #[test]
    fn s2_bare_analyze_has_no_collection() {
        let sql = "ANALYZE";
        let out = try_parse(
            sql.to_uppercase().as_str(),
            &sql.split_whitespace().collect::<Vec<_>>(),
            sql,
        );
        let Some(Ok(NodedbStatement::Cluster(ClusterStmt::Analyze { collection }))) = out else {
            panic!("expected Analyze statement, got {out:?}");
        };
        assert_eq!(collection, None);
    }

    #[test]
    fn s2_plain_analyze_parses_cleanly() {
        let sql = "ANALYZE users";
        let out = try_parse(
            sql.to_uppercase().as_str(),
            &sql.split_whitespace().collect::<Vec<_>>(),
            sql,
        );
        let Some(Ok(NodedbStatement::Cluster(ClusterStmt::Analyze { collection }))) = out else {
            panic!("expected Analyze statement, got {out:?}");
        };
        assert_eq!(collection.as_deref(), Some("users"));
    }
}
