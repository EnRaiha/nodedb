// SPDX-License-Identifier: BUSL-1.1

//! CSV import for `COPY FROM`.
//!
//! Relocated verbatim from the pgwire `ddl::collection::copy_from::csv_import`
//! module (now deleted). `plan_and_dispatch` returns the protocol-neutral
//! [`DdlError`] directly (it is the neutral collection-DML helper), so this
//! module's own file-read/parse errors are built as `DdlError` at their call
//! sites to keep one error type end to end.

use crate::control::server::shared::ddl::neutral::collection::dml::{
    fields_to_insert_sql, plan_and_dispatch,
};
use crate::control::server::shared::ddl::result::DdlError;

use super::entry::wrap_row_error;
use super::import_ctx::ImportCtx;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// CSV-specific parsing options.
#[derive(Clone, Copy, Debug)]
pub(super) struct CsvOptions {
    pub(super) delimiter: char,
    pub(super) has_header: bool,
}

/// Import from a CSV file (header row → column names; each subsequent row → INSERT).
pub(super) async fn import_csv(
    ctx: &ImportCtx<'_>,
    collection: &str,
    path: &str,
    opts: CsvOptions,
) -> Result<usize, DdlError> {
    let CsvOptions {
        delimiter,
        has_header,
    } = opts;
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| ddl_err("58030", format!("COPY: cannot read '{path}': {e}")))?;

    let content = std::str::from_utf8(&bytes).map_err(|e| {
        ddl_err(
            "22021",
            format!("COPY: file '{path}' is not valid UTF-8: {e}"),
        )
    })?;

    let mut lines = content.lines();

    // If HEADER true (default), first non-empty line is the header.
    let headers: Vec<String> = if has_header {
        match lines.next() {
            None => return Ok(0),
            Some(h) => parse_csv_row(h, delimiter),
        }
    } else {
        Vec::new()
    };

    // Parse phase: collect all rows, validate column counts.
    let mut parsed: Vec<(
        usize,
        std::collections::HashMap<String, nodedb_types::Value>,
    )> = Vec::new();
    let mut line_no = if has_header { 2usize } else { 1usize };

    for line in lines {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() {
            line_no += 1;
            continue;
        }
        let values = parse_csv_row(line, delimiter);
        let mut fields: std::collections::HashMap<String, nodedb_types::Value> = if has_header {
            if values.len() != headers.len() {
                return Err(ddl_err(
                    "22P02",
                    format!(
                        "COPY: row {line_no} has {} columns, header has {}",
                        values.len(),
                        headers.len()
                    ),
                ));
            }
            headers
                .iter()
                .zip(values.iter())
                .map(|(h, v)| (h.clone(), coerce_csv_value(v)))
                .collect()
        } else {
            values
                .iter()
                .enumerate()
                .map(|(i, v)| (format!("col_{i}"), coerce_csv_value(v)))
                .collect()
        };
        // Inject a unique row number as id if the field map has no id.
        // This prevents duplicate-key errors on schemaless collections
        // where all rows would otherwise receive the same empty-string id.
        if !fields.contains_key("id") {
            fields.insert(
                "id".to_string(),
                nodedb_types::Value::Integer(line_no as i64),
            );
        }
        parsed.push((line_no, fields));
        line_no += 1;
    }

    // Insert phase.
    for (ln, fields) in &parsed {
        let sql = fields_to_insert_sql(collection, fields);
        plan_and_dispatch(
            ctx.state,
            ctx.identity,
            ctx.tenant_id,
            ctx.database_id,
            &sql,
            ctx.txn_ctx,
        )
        .await
        .map_err(|e| wrap_row_error(e, *ln, "CSV"))?;
    }

    Ok(parsed.len())
}

/// Parse a single CSV row respecting double-quote quoting and escaped quotes (`""`).
fn parse_csv_row(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next(); // consume second "
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == delimiter {
            fields.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    fields.push(current.trim().to_string());
    fields
}

/// Coerce a CSV string value to a `nodedb_types::Value`.
///
/// Type precedence: integer → float → bool → string. This is the natural
/// order for untyped CSV where numeric values are the most common case.
fn coerce_csv_value(s: &str) -> nodedb_types::Value {
    let trimmed = s.trim();
    if let Ok(i) = trimmed.parse::<i64>() {
        return nodedb_types::Value::Integer(i);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return nodedb_types::Value::Float(f);
    }
    match trimmed.to_lowercase().as_str() {
        "true" => return nodedb_types::Value::Bool(true),
        "false" => return nodedb_types::Value::Bool(false),
        _ => {}
    }
    nodedb_types::Value::String(s.to_string())
}
