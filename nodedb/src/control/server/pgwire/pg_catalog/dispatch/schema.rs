// SPDX-License-Identifier: BUSL-1.1

//! Single source of truth for catalog-table schemas, plus the cheap
//! relation-name detector used at Parse/Describe time.

use pgwire::api::results::FieldInfo;

use crate::control::server::pgwire::pg_catalog::tables::{
    catalog_misc, pg_attribute, pg_class, pg_index, pg_type,
};
use crate::control::server::pgwire::pg_catalog::vquery::value::{VColumn, VType};
use crate::control::server::pgwire::pg_catalog::vquery::{VTable, parse_select};
use crate::control::server::pgwire::types::{bool_field, int4_field, int8_field, text_field};

/// Canonical names of every relation served by the catalog evaluator.
pub const KNOWN_TABLES: &[&str] = &[
    "pg_database",
    "pg_namespace",
    "pg_type",
    "pg_class",
    "pg_attribute",
    "pg_index",
    "pg_authid",
    "_system.audit_log",
    "_system.dropped_collections",
    "_system.l2_cleanup_queue",
];

/// Map a parsed relation key to its canonical static name.
pub fn known_table(name: &str) -> Option<&'static str> {
    KNOWN_TABLES
        .iter()
        .copied()
        .find(|t| t.eq_ignore_ascii_case(name))
}

/// Column list for a known relation — the schema source of truth shared by
/// materialization (via `tables::*::columns`) and Describe.
pub fn columns_for(table: &str) -> Option<Vec<VColumn>> {
    Some(match table {
        "pg_database" => catalog_misc::pg_database_columns(),
        "pg_namespace" => catalog_misc::pg_namespace_columns(),
        "pg_type" => pg_type::columns(),
        "pg_class" => pg_class::columns(),
        "pg_attribute" => pg_attribute::columns(),
        "pg_index" => pg_index::columns(),
        "pg_authid" => catalog_misc::pg_authid_columns(),
        "_system.audit_log" => cols(&[
            ("seq", VType::Int8),
            ("timestamp_us", VType::Int8),
            ("event", VType::Text),
            ("tenant_id", VType::Int8),
            ("source", VType::Text),
            ("detail", VType::Text),
            ("prev_hash", VType::Text),
        ]),
        "_system.dropped_collections" => cols(&[
            ("tenant_id", VType::Int8),
            ("name", VType::Text),
            ("owner", VType::Text),
            ("engine_type", VType::Text),
            ("deactivated_at_ns", VType::Int8),
            ("retention_expires_at_ns", VType::Int8),
            ("size_bytes_estimate", VType::Int8),
        ]),
        "_system.l2_cleanup_queue" => cols(&[
            ("tenant_id", VType::Int8),
            ("name", VType::Text),
            ("purge_lsn", VType::Int8),
            ("enqueued_at_ns", VType::Int8),
            ("bytes_pending", VType::Int8),
            ("last_error", VType::Text),
            ("attempts", VType::Int4),
        ]),
        _ => return None,
    })
}

fn cols(spec: &[(&'static str, VType)]) -> Vec<VColumn> {
    spec.iter().map(|&(n, ty)| VColumn::new(n, ty)).collect()
}

fn to_field(col: &VColumn) -> FieldInfo {
    match col.ty {
        VType::Bool => bool_field(col.name),
        VType::Int4 => int4_field(col.name),
        VType::Int8 => int8_field(col.name),
        VType::Text => text_field(col.name),
    }
}

/// Full single-table schema (every column). Used at Describe time when the
/// projected schema cannot be computed.
pub fn pg_catalog_schema(table: &str) -> Option<Vec<FieldInfo>> {
    columns_for(table).map(|cols| cols.iter().map(to_field).collect())
}

/// Parse `sql` and compute the schema of the response the Execute path will
/// produce — including joins and projected columns. The relation set is taken
/// from the parsed FROM clause, so no table hint is needed. Falls back to
/// `None` if parsing or schema inference fails so the caller can use the full
/// schema.
pub fn pg_catalog_projected_schema(sql: &str) -> Option<Vec<FieldInfo>> {
    use crate::control::server::pgwire::pg_catalog::vquery::{CatalogResolver, EvalCtx, execute};

    let select = parse_select(sql).ok()?;
    let template = schema_only_table(&select.from)?;
    let resolver = CatalogResolver::default();
    let search_path = ["public".to_string()];
    let ctx = EvalCtx {
        resolver: &resolver,
        username: "",
        database: "nodedb",
        search_path: &search_path,
    };
    let result = execute(&select, template, &ctx).ok()?;
    Some(result.columns.iter().map(field_from_out).collect())
}

fn field_from_out(
    col: &crate::control::server::pgwire::pg_catalog::vquery::exec::OutColumn,
) -> FieldInfo {
    match col.ty {
        VType::Bool => bool_field(&col.name),
        VType::Int4 => int4_field(&col.name),
        VType::Int8 => int8_field(&col.name),
        VType::Text => text_field(&col.name),
    }
}

/// Build a row-less combined table with the schema of every relation in the
/// FROM clause (alias-qualified), for schema-only inference at Parse time.
fn schema_only_table(
    from: &crate::control::server::pgwire::pg_catalog::vquery::select::FromClause,
) -> Option<VTable> {
    let mut columns: Vec<VColumn> = Vec::new();
    for rel in from.relations() {
        let rel_cols = columns_for(&rel.table)?;
        for c in &rel_cols {
            columns.push(c.qualified(&rel.alias));
        }
    }
    Some(VTable {
        columns,
        rows: Vec::new(),
    })
}

/// Extract the first `pg_catalog.<table>` / bare `pg_<table>` / `_system.<t>`
/// reference from an uppercased SQL string. Matches on token boundaries so
/// identifiers that merely *contain* a virtual table name (e.g. a user
/// collection `pg_class_count_a`) are not mis-routed.
pub fn extract_pg_catalog_table(upper: &str) -> Option<&'static str> {
    if contains_word(upper, "_SYSTEM.AUDIT_LOG") {
        return Some("_system.audit_log");
    }
    if contains_word(upper, "_SYSTEM.DROPPED_COLLECTIONS") {
        return Some("_system.dropped_collections");
    }
    if contains_word(upper, "_SYSTEM.L2_CLEANUP_QUEUE") {
        return Some("_system.l2_cleanup_queue");
    }
    for table in [
        "pg_database",
        "pg_namespace",
        "pg_type",
        "pg_class",
        "pg_attribute",
        "pg_index",
        "pg_authid",
    ] {
        let qualified = format!("PG_CATALOG.{}", table.to_uppercase());
        let bare = table.to_uppercase();
        if contains_word(upper, &qualified) || contains_word(upper, &bare) {
            return known_table(table);
        }
    }
    None
}

fn contains_word(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let nlen = needle.len();
    let mut start = 0usize;
    while let Some(rel) = haystack[start..].find(needle) {
        let pos = start + rel;
        let before_ok = pos == 0 || !is_ident_char(bytes[pos - 1]);
        let after = pos + nlen;
        let after_ok = after == bytes.len() || !is_ident_char(bytes[after]);
        if before_ok && after_ok {
            return true;
        }
        start = pos + 1;
        if start >= bytes.len() {
            break;
        }
    }
    false
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_qualified_table() {
        let sql = "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_class")
        );
    }

    #[test]
    fn extracts_bare_table() {
        let sql = "SELECT oid, typname FROM pg_type";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_type")
        );
    }

    #[test]
    fn no_match_for_regular_query() {
        let sql = "SELECT * FROM users WHERE id = 1";
        assert_eq!(extract_pg_catalog_table(&sql.to_uppercase()), None);
    }

    #[test]
    fn handles_join_with_pg_catalog() {
        let sql =
            "SELECT c.oid FROM pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_namespace")
        );
    }
}
