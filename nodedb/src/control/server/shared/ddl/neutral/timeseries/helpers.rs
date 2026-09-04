// SPDX-License-Identifier: BUSL-1.1

//! Internal helpers for the protocol-neutral timeseries DDL handlers.

use nodedb_sql::parser::preprocess::lex::find_ascii_keyword;

use crate::control::server::shared::ddl::sql_parse::parse_ident_token;

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Parse a `WITH (key = 'value', ...)` clause from a split DDL statement.
///
/// Returns `None` if no WITH clause is present or if the clause is empty.
pub(super) fn parse_with_clause(parts: &[&str]) -> Option<String> {
    let sql = parts.join(" ");
    let with_pos = find_ascii_keyword(&sql, "WITH")?;
    let after_with = sql[with_pos + 4..].trim();

    let open = after_with.find('(')?;
    let close = after_with.rfind(')')?;
    if close <= open {
        return None;
    }
    let inner = &after_with[open + 1..close];

    let mut config = serde_json::Map::new();
    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some(eq) = pair.find('=') {
            let key = pair[..eq].trim().to_lowercase();
            let val = pair[eq + 1..].trim().trim_matches('\'').trim_matches('"');
            config.insert(key, serde_json::Value::String(val.to_string()));
        }
    }

    if config.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(config).to_string())
    }
}

/// Parse column definitions from `CREATE TIMESERIES name (col TYPE, ...)`.
///
/// Looks for parenthesized column definitions between the name and `WITH`.
/// The `(...)` must appear before any `WITH` keyword. Returns `None` if
/// no column definitions are present (falls back to default schema).
///
/// Returns an error when a column name is not a usable SQL identifier.
///
/// Supported types: TIMESTAMP, FLOAT, FLOAT8, INT, INTEGER, VARCHAR, TEXT, BOOLEAN.
pub(super) fn parse_column_defs(parts: &[&str]) -> Result<Option<Vec<(String, String)>>, DdlError> {
    let sql = parts.join(" ");
    // Column defs start after name (index 2 in parts) and before WITH.
    // Find the first `(` that is NOT part of a WITH clause.
    let with_pos = find_ascii_keyword(&sql, "WITH").unwrap_or(sql.len());
    let before_with = &sql[..with_pos];

    let (Some(open), Some(close)) = (before_with.find('('), before_with.rfind(')')) else {
        return Ok(None);
    };
    if close <= open {
        return Ok(None);
    }

    let inner = &before_with[open + 1..close];
    let mut fields = Vec::new();

    for col_def in inner.split(',') {
        let col_def = col_def.trim();
        if col_def.is_empty() {
            continue;
        }
        let col_parts: Vec<&str> = col_def.split_whitespace().collect();
        if col_parts.len() < 2 {
            continue;
        }
        let col_name = parse_ident_token(col_parts[0])?;
        let col_type = col_parts[1].to_uppercase();
        fields.push((col_name, col_type));
    }

    if fields.is_empty() {
        Ok(None)
    } else {
        Ok(Some(fields))
    }
}

/// Format a byte count into a human-readable string.
pub(super) fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_clause_basic() {
        let parts: Vec<&str> =
            "CREATE TIMESERIES metrics WITH (partition_by = '3d', retention_period = '30d')"
                .split_whitespace()
                .collect();
        let config = parse_with_clause(&parts).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(parsed["partition_by"], "3d");
        assert_eq!(parsed["retention_period"], "30d");
    }

    #[test]
    fn parse_with_clause_none() {
        let parts: Vec<&str> = "CREATE TIMESERIES metrics".split_whitespace().collect();
        assert!(parse_with_clause(&parts).is_none());
    }

    #[test]
    fn parse_column_defs_basic() {
        let parts: Vec<&str> =
            "CREATE TIMESERIES metrics (timestamp TIMESTAMP, host VARCHAR, cpu FLOAT)"
                .split_whitespace()
                .collect();
        let cols = parse_column_defs(&parts).unwrap().unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], ("timestamp".into(), "TIMESTAMP".into()));
        assert_eq!(cols[1], ("host".into(), "VARCHAR".into()));
        assert_eq!(cols[2], ("cpu".into(), "FLOAT".into()));
    }

    #[test]
    fn parse_column_defs_with_with_clause() {
        let parts: Vec<&str> =
            "CREATE TIMESERIES metrics (ts TIMESTAMP, val FLOAT) WITH (partition_by = '1d')"
                .split_whitespace()
                .collect();
        let cols = parse_column_defs(&parts).unwrap().unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].0, "ts");
        assert_eq!(cols[1].0, "val");
    }

    #[test]
    fn parse_column_defs_none_when_absent() {
        let parts: Vec<&str> = "CREATE TIMESERIES metrics".split_whitespace().collect();
        assert!(parse_column_defs(&parts).unwrap().is_none());
    }

    #[test]
    fn parse_column_defs_none_for_with_only() {
        let parts: Vec<&str> = "CREATE TIMESERIES metrics WITH (partition_by = '1d')"
            .split_whitespace()
            .collect();
        // The `(...)` belongs to WITH, not column defs.
        assert!(parse_column_defs(&parts).unwrap().is_none());
    }

    #[test]
    fn format_bytes_test() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1500), "1.5 KB");
        assert_eq!(format_bytes(1_500_000), "1.4 MB");
        assert_eq!(format_bytes(2_000_000_000), "1.9 GB");
    }
}
