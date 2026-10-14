// SPDX-License-Identifier: Apache-2.0

//! Parse `ALTER COLLECTION <name> SET ON CONFLICT <policy> FOR <kind>`
//! and `SHOW CONFLICT POLICY ON <name>`.

use super::clause::{ClauseCursor, Token, tokenize};
use crate::ddl_ast::alter_ops::{ConflictPolicyKind, ConstraintKindKeyword};
use crate::ddl_ast::statement::{AlterCollectionOp, CollectionStmt, NodedbStatement, PolicyStmt};
use crate::error::SqlError;

/// Try to parse conflict-policy DDL statements.
///
/// Handles:
/// - `ALTER COLLECTION <name> SET ON CONFLICT <policy> FOR <kind>`
/// - `SHOW CONFLICT POLICY ON <name>`
pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("SHOW CONFLICT POLICY ") {
        return Some(parse_show_conflict_policy(trimmed));
    }
    // Runs before the collection family, so `ALTER COLLECTION <name> SET ON
    // CONFLICT` is claimed here. The clause must follow the name directly;
    // the same words inside a value are not this statement.
    if upper.starts_with("ALTER COLLECTION ") {
        let toks = tokenize(trimmed);
        let mut probe = ClauseCursor::new(trimmed, &toks, "ALTER COLLECTION");
        let is_set_on_conflict = probe.eat_keywords(&["ALTER", "COLLECTION"])
            && probe.name("collection").is_ok()
            && probe.peek_keywords(&["SET", "ON", "CONFLICT"]);
        if is_set_on_conflict {
            return Some(parse_alter_set_on_conflict(trimmed, &toks));
        }
    }
    None
}

/// `SHOW CONFLICT POLICY ON <name>`.
fn parse_show_conflict_policy(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "SHOW CONFLICT POLICY");
    c.expect_keywords(&["SHOW", "CONFLICT", "POLICY", "ON"])?;
    let collection = c.name("collection")?;
    c.finish()?;
    Ok(NodedbStatement::Policy(PolicyStmt::ShowConflictPolicy {
        collection,
    }))
}

/// `ALTER COLLECTION <name> SET ON CONFLICT <policy> FOR <kind>`.
fn parse_alter_set_on_conflict<'a>(
    sql: &'a str,
    toks: &[Token<'a>],
) -> Result<NodedbStatement, SqlError> {
    let mut c = ClauseCursor::new(sql, toks, "ALTER COLLECTION SET ON CONFLICT");
    c.expect_keywords(&["ALTER", "COLLECTION"])?;
    let name = c.name("collection")?;
    c.expect_keywords(&["SET", "ON", "CONFLICT"])?;
    let policy = parse_policy_keyword(c.word("conflict policy")?)?;
    c.expect_keyword("FOR")?;
    let constraint_kind = parse_constraint_kind(c.word("constraint kind")?)?;
    c.finish()?;

    Ok(NodedbStatement::Collection(
        CollectionStmt::AlterCollection {
            name,
            operation: AlterCollectionOp::SetOnConflict {
                policy,
                constraint_kind,
            },
        },
    ))
}

fn parse_policy_keyword(s: &str) -> Result<ConflictPolicyKind, SqlError> {
    match s.to_uppercase().as_str() {
        "LAST_WRITER_WINS" => Ok(ConflictPolicyKind::LastWriterWins),
        "RENAME_SUFFIX" | "RENAME_APPEND_SUFFIX" => Ok(ConflictPolicyKind::RenameSuffix),
        "CASCADE_DEFER" => Ok(ConflictPolicyKind::CascadeDefer),
        "ESCALATE_TO_DLQ" => Ok(ConflictPolicyKind::EscalateToDlq),
        "CUSTOM" => Err(SqlError::Parse {
            detail: "CUSTOM conflict policy is not supported via SQL DDL; \
                     use the native NodeDB protocol (TextFields.policy) to configure webhook-based policies"
                .to_string(),
        }),
        other => Err(SqlError::Parse {
            detail: format!(
                "unknown conflict policy keyword '{other}'; \
                 valid options: LAST_WRITER_WINS, RENAME_SUFFIX, CASCADE_DEFER, ESCALATE_TO_DLQ"
            ),
        }),
    }
}

fn parse_constraint_kind(s: &str) -> Result<ConstraintKindKeyword, SqlError> {
    match s.to_uppercase().as_str() {
        "UNIQUE" => Ok(ConstraintKindKeyword::Unique),
        "FOREIGN_KEY" => Ok(ConstraintKindKeyword::ForeignKey),
        "NOT_NULL" => Ok(ConstraintKindKeyword::NotNull),
        "CHECK" => Ok(ConstraintKindKeyword::Check),
        other => Err(SqlError::Parse {
            detail: format!(
                "unknown constraint kind '{other}'; \
                 valid options: UNIQUE, FOREIGN_KEY, NOT_NULL, CHECK"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn alter_last_writer_wins_unique() {
        let stmt = ok("ALTER COLLECTION agents SET ON CONFLICT LAST_WRITER_WINS FOR UNIQUE");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::AlterCollection { name, operation }) => {
                assert_eq!(name, "agents");
                assert_eq!(
                    operation,
                    AlterCollectionOp::SetOnConflict {
                        policy: ConflictPolicyKind::LastWriterWins,
                        constraint_kind: ConstraintKindKeyword::Unique,
                    }
                );
            }
            other => panic!("expected AlterCollection, got {other:?}"),
        }
    }

    #[test]
    fn alter_cascade_defer_foreign_key() {
        let stmt = ok("ALTER COLLECTION posts SET ON CONFLICT CASCADE_DEFER FOR FOREIGN_KEY");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::AlterCollection { name, operation }) => {
                assert_eq!(name, "posts");
                assert_eq!(
                    operation,
                    AlterCollectionOp::SetOnConflict {
                        policy: ConflictPolicyKind::CascadeDefer,
                        constraint_kind: ConstraintKindKeyword::ForeignKey,
                    }
                );
            }
            other => panic!("expected AlterCollection, got {other:?}"),
        }
    }

    #[test]
    fn alter_rename_suffix_legacy_keyword() {
        let stmt = ok("ALTER COLLECTION x SET ON CONFLICT RENAME_APPEND_SUFFIX FOR NOT_NULL");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::AlterCollection { operation, .. }) => {
                assert_eq!(
                    operation,
                    AlterCollectionOp::SetOnConflict {
                        policy: ConflictPolicyKind::RenameSuffix,
                        constraint_kind: ConstraintKindKeyword::NotNull,
                    }
                );
            }
            other => panic!("expected AlterCollection, got {other:?}"),
        }
    }

    #[test]
    fn alter_escalate_to_dlq_check() {
        let stmt = ok("ALTER COLLECTION x SET ON CONFLICT ESCALATE_TO_DLQ FOR CHECK");
        match stmt {
            NodedbStatement::Collection(CollectionStmt::AlterCollection { operation, .. }) => {
                assert_eq!(
                    operation,
                    AlterCollectionOp::SetOnConflict {
                        policy: ConflictPolicyKind::EscalateToDlq,
                        constraint_kind: ConstraintKindKeyword::Check,
                    }
                );
            }
            other => panic!("expected AlterCollection, got {other:?}"),
        }
    }

    #[test]
    fn show_conflict_policy() {
        let stmt = ok("SHOW CONFLICT POLICY ON mydb");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::ShowConflictPolicy {
                collection: "mydb".to_string()
            })
        );
    }

    #[test]
    fn custom_policy_rejected() {
        let e = err("ALTER COLLECTION x SET ON CONFLICT CUSTOM FOR UNIQUE");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("native NodeDB protocol"));
    }

    #[test]
    fn unknown_policy_rejected() {
        let e = err("ALTER COLLECTION x SET ON CONFLICT FOOBAR FOR UNIQUE");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn unknown_constraint_kind_rejected() {
        let e = err("ALTER COLLECTION x SET ON CONFLICT LAST_WRITER_WINS FOR BADKIND");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn trailing_tokens_are_refused() {
        let e = err("ALTER COLLECTION t SET ON CONFLICT LAST_WRITER_WINS FOR UNIQUE extra");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn set_on_conflict_inside_a_value_is_not_this_statement() {
        let stmt = parse("ALTER COLLECTION t ADD COLUMN m TEXT DEFAULT ' SET ON CONFLICT '");
        assert!(
            !matches!(
                stmt,
                Some(Ok(NodedbStatement::Collection(
                    CollectionStmt::AlterCollection {
                        operation: AlterCollectionOp::SetOnConflict { .. },
                        ..
                    }
                )))
            ),
            "got {stmt:?}"
        );
    }
}
