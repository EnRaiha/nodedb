// SPDX-License-Identifier: Apache-2.0

/// Source for `COPY ... TO`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyToSource {
    /// `COPY <collection> TO '<path>'` — export from a named collection.
    Collection(String),
    /// `COPY (SELECT ...) TO '<path>'` — export from an arbitrary query.
    Query(String),
}

/// Row format of `COPY ... FROM STDIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyStdinFormat {
    /// PostgreSQL text format: tab-delimited by default, `\N` is NULL, and a
    /// backslash escapes the next character.
    Text,
    /// CSV: comma-delimited by default, `"` quotes a field, and an unquoted
    /// empty field is NULL.
    Csv,
}

/// Format for `COPY ... FROM` bulk import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyFormat {
    /// One JSON object per line (`.ndjson` / `.jsonl`).
    Ndjson,
    /// A JSON array of objects (`.json`).
    JsonArray,
    /// CSV with an optional header row (`.csv`).
    Csv,
}
