// SPDX-License-Identifier: BUSL-1.1

//! `CREATE TEMPORARY TABLE` / `CREATE TEMP TABLE` DDL handler.
//!
//! Temporary tables are session-local DataFusion MemTables. They shadow
//! permanent tables with the same name for the creating session. Auto-dropped
//! on session disconnect.

use std::sync::Arc;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::temp_tables::{OnCommitAction, TempTableEntry};
use crate::control::server::shared::session::{SessionId, SessionStore};

/// Handle `CREATE TEMPORARY TABLE name (col1 type1, ...) [ON COMMIT ...]`.
///
/// Also handles `CREATE TEMP TABLE` (alias).
/// Creates a DataFusion MemTable and stores it in the session for query access.
pub fn create_temp_table(
    sessions: &SessionStore,
    identity: &AuthenticatedIdentity,
    session_id: SessionId,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    let parts: Vec<&str> = sql.split_whitespace().collect();
    if !upper.starts_with("CREATE TEMPORARY TABLE ") && !upper.starts_with("CREATE TEMP TABLE ") {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "syntax: CREATE TEMPORARY TABLE <name> (columns...)".to_owned(),
        ))));
    }

    let name = parts
        .get(3)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "CREATE TEMPORARY TABLE requires a name".to_owned(),
            )))
        })?
        .to_lowercase();

    if sessions.has_temp_table(session_id, &name) {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P07".to_owned(),
            format!("temporary table \"{name}\" already exists in this session"),
        ))));
    }

    let on_commit = if upper.contains("ON COMMIT DROP") {
        OnCommitAction::Drop
    } else if upper.contains("ON COMMIT DELETE ROWS") {
        OnCommitAction::DeleteRows
    } else {
        OnCommitAction::PreserveRows
    };

    // Parse schema from column definitions.
    let schema = Arc::new(parse_temp_table_schema(sql)?);

    sessions.register_temp_table(
        session_id,
        name.clone(),
        TempTableEntry {
            schema,
            on_commit,
            batches: Vec::new(),
        },
    );

    tracing::info!(table = %name, user = %identity.username, "created temporary table");
    Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))])
}

/// Handle `DROP TABLE name` for temp tables.
///
/// Returns `Some(response)` if the table was a temp table, `None` otherwise.
pub fn drop_temp_table_if_exists(
    sessions: &SessionStore,
    session_id: SessionId,
    name: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    if sessions.has_temp_table(session_id, name) {
        sessions.remove_temp_table(session_id, name);
        Some(Ok(vec![Response::Execution(Tag::new("DROP TABLE"))]))
    } else {
        None
    }
}

/// Parse column definitions from `CREATE TEMPORARY TABLE name (col1 type1, ...)`.
fn parse_temp_table_schema(sql: &str) -> PgWireResult<arrow::datatypes::Schema> {
    use arrow::datatypes::{Field, Schema};

    let paren_start = sql.find('(').ok_or_else(|| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "CREATE TEMPORARY TABLE requires column definitions in parentheses".to_owned(),
        )))
    })?;

    let inner = &sql[paren_start + 1..];
    let paren_end = inner.rfind(')').unwrap_or(inner.len());
    let cols_str = &inner[..paren_end];

    let mut fields = Vec::new();
    for col_def in cols_str.split(',') {
        let col_def = col_def.trim();
        if col_def.is_empty() {
            continue;
        }
        let mut tokens = col_def.split_whitespace();
        let col_name = match tokens.next() {
            Some(n) => n,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("missing column name in definition: '{col_def}'"),
                ))));
            }
        };
        let type_str = match tokens.next() {
            Some(t) => t.to_uppercase(),
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("missing type for column '{col_name}'"),
                ))));
            }
        };

        // Reject table-level constraint keywords appearing as the "column name" token.
        // "PRIMARY" here means `PRIMARY KEY (col)` table-level form — not supported for
        // temp tables either; inline `col TYPE PRIMARY KEY` passes through fine.
        let col_upper = col_name.to_uppercase();
        if matches!(
            col_upper.as_str(),
            "PRIMARY" | "UNIQUE" | "CHECK" | "FOREIGN" | "REFERENCES" | "CONSTRAINT"
        ) {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "0A000".to_owned(),
                format!(
                    "unsupported constraint: {col_upper}; \
                     use NodeDB-native enforcement (indexes, typeguards)"
                ),
            ))));
        }

        // Reject inline constraint keywords appearing after the type token.
        // "PRIMARY" (inline `col TYPE PRIMARY KEY`) is intentionally allowed through here —
        // temp tables are DataFusion MemTables and ignore the PK marker, but accepting the
        // syntax avoids breaking CREATE TEMPORARY TABLE statements that carry it.
        for tok in tokens.by_ref() {
            let upper_tok = tok.to_uppercase();
            match upper_tok.as_str() {
                "UNIQUE" => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(),
                        "unsupported constraint: UNIQUE constraint; \
                         use a UNIQUE secondary index: \
                         CREATE INDEX ... ON collection (field) UNIQUE"
                            .to_owned(),
                    ))));
                }
                "CHECK" => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(),
                        "unsupported constraint: CHECK constraint; \
                         CHECK constraints are unsupported; enforce in application code \
                         or use a typed function in INSERT"
                            .to_owned(),
                    ))));
                }
                "FOREIGN" | "REFERENCES" => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(),
                        "unsupported constraint: FOREIGN KEY constraint; \
                         FOREIGN KEY enforcement is unsupported; enforce in application code"
                            .to_owned(),
                    ))));
                }
                _ => {}
            }
        }

        let data_type = sql_type_to_arrow(&type_str);
        let nullable = !col_def.to_uppercase().contains("NOT NULL");
        fields.push(Field::new(col_name, data_type, nullable));
    }

    if fields.is_empty() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "temporary table must have at least one column".to_owned(),
        ))));
    }

    Ok(Schema::new(fields))
}

/// Map SQL type name to Arrow DataType.
fn sql_type_to_arrow(type_str: &str) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    match type_str {
        "INT" | "INT4" | "INTEGER" => DataType::Int32,
        "BIGINT" | "INT8" => DataType::Int64,
        "SMALLINT" | "INT2" => DataType::Int16,
        "FLOAT" | "FLOAT4" | "REAL" => DataType::Float32,
        "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => DataType::Float64,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TEXT" | "VARCHAR" | "STRING" => DataType::Utf8,
        "BYTEA" | "BYTES" => DataType::Binary,
        "TIMESTAMP" | "TIMESTAMPTZ" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        "DATE" => DataType::Date32,
        _ => DataType::Utf8,
    }
}
