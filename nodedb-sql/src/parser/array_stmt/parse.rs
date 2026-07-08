// SPDX-License-Identifier: Apache-2.0

//! Recursive-descent parser for the four `ARRAY` statements.
//!
//! `try_parse_array_statement` returns `Ok(None)` for any SQL that does
//! not begin with one of the array prefixes; this lets the caller fall
//! through to the standard sqlparser path. When the prefix matches, the
//! parser commits — any further error is surfaced as `SqlError::Parse`.

#[path = "parse_impl/mod.rs"]
mod parse_impl;

use super::ast::ArrayStatement;
use super::lexer::tokenize;
use crate::error::Result;
use parse_impl::Parser;

/// Top-level entry. Returns `Ok(None)` if the SQL doesn't start with an
/// array statement keyword sequence.
pub fn try_parse_array_statement(sql: &str) -> Result<Option<ArrayStatement>> {
    let trimmed = sql.trim_start();
    let upper: String = trimmed
        .chars()
        .take(40)
        .collect::<String>()
        .to_ascii_uppercase();

    if upper.starts_with("CREATE ARRAY ") || upper == "CREATE ARRAY" {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("CREATE")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Create(p.parse_create()?)));
    }
    if upper.starts_with("DROP ARRAY ") || upper == "DROP ARRAY" {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("DROP")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Drop(p.parse_drop()?)));
    }
    if upper.starts_with("INSERT INTO ARRAY ") {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("INSERT")?;
        p.expect_kw("INTO")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Insert(p.parse_insert()?)));
    }
    if upper.starts_with("DELETE FROM ARRAY ") {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("DELETE")?;
        p.expect_kw("FROM")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Delete(p.parse_delete()?)));
    }
    if upper.starts_with("ALTER ARRAY ") || upper == "ALTER ARRAY" {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("ALTER")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Alter(p.parse_alter()?)));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::super::ast::ArrayStatement;
    use super::*;
    use crate::types_array::ArrayCellOrderAst;

    #[test]
    fn passthrough_non_array_sql() {
        assert!(
            try_parse_array_statement("SELECT * FROM t")
                .unwrap()
                .is_none()
        );
        assert!(
            try_parse_array_statement("CREATE TABLE t (x INT)")
                .unwrap()
                .is_none()
        );
        assert!(
            try_parse_array_statement("INSERT INTO foo VALUES (1)")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parse_create_array_full() {
        let sql = "CREATE ARRAY genome \
                   DIMS (chrom INT64 [1..23], pos INT64 [0..300000000]) \
                   ATTRS (variant STRING, qual FLOAT64) \
                   TILE_EXTENTS (1, 1000000) \
                   CELL_ORDER HILBERT";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Create(c) => {
                assert_eq!(c.name, "genome");
                assert_eq!(c.dims.len(), 2);
                assert_eq!(c.attrs.len(), 2);
                assert_eq!(c.tile_extents, vec![1, 1_000_000]);
                assert_eq!(c.cell_order, ArrayCellOrderAst::Hilbert);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_drop_array_if_exists() {
        let stmt = try_parse_array_statement("DROP ARRAY IF EXISTS g")
            .unwrap()
            .unwrap();
        match stmt {
            ArrayStatement::Drop(d) => {
                assert!(d.if_exists);
                assert_eq!(d.name, "g");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_insert_multi_row() {
        let sql = "INSERT INTO ARRAY g \
                   COORDS (1, 100) VALUES ('SNP', 99.5), \
                   COORDS (1, 200) VALUES ('INS', 88.0)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Insert(i) => {
                assert_eq!(i.name, "g");
                assert_eq!(i.rows.len(), 2);
                assert_eq!(i.rows[0].coords.len(), 2);
                assert_eq!(i.rows[0].attrs.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_delete_coords_in() {
        let sql = "DELETE FROM ARRAY g WHERE COORDS IN ((1, 100), (1, 200))";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Delete(d) => {
                assert_eq!(d.name, "g");
                assert_eq!(d.coords.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn create_rejects_unknown_dim_type() {
        let sql = "CREATE ARRAY g DIMS (x BOGUS [0..10]) ATTRS (v INT64) TILE_EXTENTS (1)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_create_array_with_audit_retain() {
        let sql = "CREATE ARRAY g \
                   DIMS (x INT64 [0..100]) \
                   ATTRS (v INT64) \
                   TILE_EXTENTS (10) \
                   WITH (audit_retain_ms = 86400000)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Create(c) => {
                assert_eq!(c.audit_retain_ms, Some(86_400_000));
                assert_eq!(c.minimum_audit_retain_ms, None);
                assert_eq!(c.prefix_bits, 8);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_create_array_with_all_retention_keys() {
        let sql = "CREATE ARRAY genomes \
                   DIMS (variant_id INT64 [0..1000000000], sample_id INT64 [0..100000]) \
                   ATTRS (gt INT64, dp INT64) \
                   TILE_EXTENTS (1024, 256) \
                   WITH (prefix_bits = 8, audit_retain_ms = 86400000, minimum_audit_retain_ms = 3600000)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Create(c) => {
                assert_eq!(c.prefix_bits, 8);
                assert_eq!(c.audit_retain_ms, Some(86_400_000));
                assert_eq!(c.minimum_audit_retain_ms, Some(3_600_000));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_create_array_unknown_with_key_rejected() {
        let sql = "CREATE ARRAY g \
                   DIMS (x INT64 [0..10]) \
                   ATTRS (v INT64) \
                   TILE_EXTENTS (1) \
                   WITH (bogus_key = 42)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_create_array_negative_retain_rejected() {
        let sql = "CREATE ARRAY g \
                   DIMS (x INT64 [0..10]) \
                   ATTRS (v INT64) \
                   TILE_EXTENTS (1) \
                   WITH (audit_retain_ms = -1)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_alter_array_single_key() {
        let sql = "ALTER ARRAY my_array SET (audit_retain_ms = 86400000)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Alter(a) => {
                assert_eq!(a.name, "my_array");
                assert_eq!(a.set.len(), 1);
                assert_eq!(a.set[0].0, "audit_retain_ms");
                assert_eq!(a.set[0].1, Some(86_400_000));
            }
            _ => panic!("expected Alter variant"),
        }
    }

    #[test]
    fn parse_alter_array_null_value() {
        let sql = "ALTER ARRAY my_array SET (audit_retain_ms = NULL)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Alter(a) => {
                assert_eq!(a.set[0].0, "audit_retain_ms");
                assert_eq!(a.set[0].1, None);
            }
            _ => panic!("expected Alter variant"),
        }
    }

    #[test]
    fn parse_alter_array_multi_key() {
        let sql = "ALTER ARRAY arr SET (audit_retain_ms = 5000, minimum_audit_retain_ms = 1000)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Alter(a) => {
                assert_eq!(a.set.len(), 2);
                assert!(
                    a.set
                        .iter()
                        .any(|(k, v)| k == "audit_retain_ms" && *v == Some(5000))
                );
                assert!(
                    a.set
                        .iter()
                        .any(|(k, v)| k == "minimum_audit_retain_ms" && *v == Some(1000))
                );
            }
            _ => panic!("expected Alter variant"),
        }
    }

    #[test]
    fn parse_alter_array_unknown_key_rejected() {
        let sql = "ALTER ARRAY arr SET (bogus_key = 42)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_alter_array_minimum_null_rejected() {
        let sql = "ALTER ARRAY arr SET (minimum_audit_retain_ms = NULL)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_alter_not_matched_for_other_sql() {
        assert!(
            try_parse_array_statement("ALTER TABLE foo ADD COLUMN x INT")
                .unwrap()
                .is_none()
        );
    }
}
