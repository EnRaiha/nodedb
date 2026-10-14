// SPDX-License-Identifier: Apache-2.0

//! `MIRROR DATABASE <local_name> FROM <source_cluster>.<source_database> [MODE = sync | async]`
//! and `SHOW DATABASE MIRROR STATUS [FOR <name>]`.

use nodedb_types::MirrorMode;

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;

/// Read `<local> FROM <cluster>.<database> [MODE = sync | async]` after
/// `MIRROR DATABASE`.
pub(super) fn parse_mirror_database(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    // The source is specified as `<cluster_id>.<database_name>`, allowing the
    // handler to set up a cross-cluster QUIC link to the correct source cluster.
    if c.at_end() {
        return Err(c.error("MIRROR DATABASE requires a local name"));
    }
    let local_name = db_name(c, "local name")?;
    if !c.eat_keyword("FROM") {
        return Err(c.error("MIRROR DATABASE requires FROM <source_cluster>.<source_database>"));
    }
    if c.at_end() {
        return Err(c.error("MIRROR DATABASE FROM requires <source_cluster>.<source_database>"));
    }
    let source_token = c.raw_run("source")?.trim_matches('"');

    // Split on the first '.' to extract cluster.database. If there is no dot
    // the whole token is treated as the source_cluster and source_database
    // defaults to the same identifier (same-name convention for local testing).
    let (source_cluster, source_database) = match source_token.find('.') {
        Some(dot_pos) => {
            let cluster = source_token[..dot_pos].trim_matches('"').to_string();
            let database = source_token[dot_pos + 1..].trim_matches('"').to_string();
            if cluster.is_empty() || database.is_empty() {
                return Err(SqlError::Parse {
                    detail: format!(
                        "MIRROR DATABASE FROM: invalid source '{source_token}'; \
                         expected <source_cluster>.<source_database>"
                    ),
                });
            }
            (cluster, database)
        }
        None => {
            let name = source_token.to_string();
            (name.clone(), name)
        }
    };

    // MODE = sync | async (optional; default async)
    let mode = if c.eat_keyword("MODE") {
        c.expect_punct('=')?;
        let value = c.word("MODE")?.to_uppercase();
        match value.as_str() {
            "SYNC" => MirrorMode::Sync,
            "ASYNC" => MirrorMode::Async,
            other => {
                return Err(SqlError::Parse {
                    detail: format!(
                        "MIRROR DATABASE MODE: expected 'sync' or 'async', got '{other}'"
                    ),
                });
            }
        }
    } else {
        MirrorMode::Async
    };
    c.finish()?;

    Ok(NodedbStatement::Database(DatabaseStmt::MirrorDatabase {
        local_name,
        source_cluster,
        source_database,
        mode,
    }))
}

/// Read `[FOR <name>]` after `SHOW DATABASE MIRROR STATUS`.
pub(super) fn parse_show_database_mirror_status(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    let name = if c.eat_keyword("FOR") {
        if c.at_end() {
            return Err(c.error("SHOW DATABASE MIRROR STATUS FOR requires a database name"));
        }
        Some(db_name(c, "database name")?)
    } else {
        None
    };
    c.finish()?;
    Ok(NodedbStatement::Database(
        DatabaseStmt::ShowDatabaseMirrorStatus { name },
    ))
}

#[cfg(test)]
mod tests {
    use super::super::dispatch::try_parse;
    use super::*;

    fn ok(sql: &str) -> NodedbStatement {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
            .expect("expected Some")
            .expect("expected Ok")
    }

    #[test]
    fn parse_mirror_database_dotted_source_async_default() {
        let stmt = ok("MIRROR DATABASE replica FROM prod-us.mydb");
        match stmt {
            NodedbStatement::Database(DatabaseStmt::MirrorDatabase {
                local_name,
                source_cluster,
                source_database,
                mode,
            }) => {
                assert_eq!(local_name, "replica");
                assert_eq!(source_cluster, "prod-us");
                assert_eq!(source_database, "mydb");
                assert_eq!(mode, MirrorMode::Async);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_mirror_database_sync_mode() {
        let stmt = ok("MIRROR DATABASE replica FROM prod-us.mydb MODE = sync");
        match stmt {
            NodedbStatement::Database(DatabaseStmt::MirrorDatabase { mode, .. }) => {
                assert_eq!(mode, MirrorMode::Sync);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_mirror_database_invalid_mode_errors() {
        let sql = "MIRROR DATABASE replica FROM prod-us.mydb MODE = invalid";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(
                    detail.contains("async") || detail.contains("sync"),
                    "{detail}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_show_database_mirror_status_all() {
        let stmt = ok("SHOW DATABASE MIRROR STATUS");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::ShowDatabaseMirrorStatus { name: None })
        );
    }

    #[test]
    fn parse_show_database_mirror_status_for_name() {
        let stmt = ok("SHOW DATABASE MIRROR STATUS FOR replica");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::ShowDatabaseMirrorStatus {
                name: Some("replica".into())
            })
        );
    }

    #[test]
    fn parse_show_database_mirror_status_missing_name_errors() {
        let sql = "SHOW DATABASE MIRROR STATUS FOR";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(detail.contains("requires a database name"), "{detail}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
