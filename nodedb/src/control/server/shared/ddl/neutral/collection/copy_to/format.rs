// SPDX-License-Identifier: BUSL-1.1

//! Format serializers for COPY TO: NDJSON, JSON array, CSV.
//!
//! Relocated verbatim from the pgwire `ddl::collection::copy_to::format`
//! module (now deleted).

use nodedb_sql::ddl_ast::statement::CopyFormat;
use sonic_rs;

use crate::control::server::shared::ddl::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Serialize a slice of JSON object values to bytes in the requested format.
///
/// `rows` is a `Vec<serde_json::Value>` where each element is an Object.
/// Returns the serialized bytes on success.
pub(super) fn serialize_rows(
    rows: &[serde_json::Value],
    format: &CopyFormat,
    delimiter: char,
    header: bool,
) -> Result<Vec<u8>, DdlError> {
    match format {
        CopyFormat::Ndjson => serialize_ndjson(rows),
        CopyFormat::JsonArray => serialize_json_array(rows),
        CopyFormat::Csv => serialize_csv(rows, delimiter, header),
    }
}

fn serialize_ndjson(rows: &[serde_json::Value]) -> Result<Vec<u8>, DdlError> {
    let mut out = Vec::with_capacity(rows.len() * 64);
    for row in rows {
        let line = sonic_rs::to_vec(row)
            .map_err(|e| ddl_err("XX000", format!("COPY TO: JSON serialization error: {e}")))?;
        out.extend_from_slice(&line);
        out.push(b'\n');
    }
    Ok(out)
}

fn serialize_json_array(rows: &[serde_json::Value]) -> Result<Vec<u8>, DdlError> {
    // Build a serde_json::Value::Array and serialize once.
    let arr = serde_json::Value::Array(rows.to_vec());
    let bytes = sonic_rs::to_vec(&arr).map_err(|e| {
        ddl_err(
            "XX000",
            format!("COPY TO: JSON array serialization error: {e}"),
        )
    })?;
    Ok(bytes)
}

fn serialize_csv(
    rows: &[serde_json::Value],
    delimiter: char,
    header: bool,
) -> Result<Vec<u8>, DdlError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Collect column names from the first row.
    let columns: Vec<String> = match &rows[0] {
        serde_json::Value::Object(map) => {
            let mut cols: Vec<String> = map.keys().cloned().collect();
            cols.sort();
            cols
        }
        _ => {
            return Err(ddl_err(
                "22P02",
                "COPY TO CSV: expected JSON objects as rows",
            ));
        }
    };

    let mut out = Vec::with_capacity(rows.len() * columns.len() * 16);

    if header {
        let hdr = csv_row(
            &columns.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            delimiter,
        );
        out.extend_from_slice(hdr.as_bytes());
        out.push(b'\n');
    }

    for (idx, row) in rows.iter().enumerate() {
        let obj = match row.as_object() {
            Some(m) => m,
            None => {
                return Err(ddl_err(
                    "22P02",
                    format!("COPY TO CSV: row {idx} is not a JSON object"),
                ));
            }
        };
        let values: Vec<String> = columns
            .iter()
            .map(|col| json_value_to_csv_field(obj.get(col).unwrap_or(&serde_json::Value::Null)))
            .collect();
        let line = csv_row(
            &values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            delimiter,
        );
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }

    Ok(out)
}

/// Format one CSV row with RFC 4180 quoting.
fn csv_row(fields: &[&str], delimiter: char) -> String {
    let parts: Vec<String> = fields.iter().map(|f| csv_quote(f, delimiter)).collect();
    parts.join(&delimiter.to_string())
}

/// RFC 4180 CSV field quoting: quote if field contains delimiter, `"`, `\r`, or `\n`.
fn csv_quote(s: &str, delimiter: char) -> String {
    if s.contains(delimiter) || s.contains('"') || s.contains('\r') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Convert a JSON value to a CSV field string (without quoting — handled by csv_quote).
fn json_value_to_csv_field(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            sonic_rs::to_string(val).unwrap_or_default()
        }
    }
}
