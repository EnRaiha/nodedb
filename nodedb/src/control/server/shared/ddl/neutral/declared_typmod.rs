// SPDX-License-Identifier: BUSL-1.1

//! DDL-time gate for a declared `DECIMAL(p,s)` typmod.
//!
//! A schemaless or key-value collection keeps each declared column type as
//! text. Without this gate, `DECIMAL(1001,0)` is accepted there and the
//! column then reads as an unknown type. The gate runs on every engine's
//! `CREATE` path and on `ALTER ADD COLUMN`, so all engines refuse the same
//! declarations.
//!
//! An out-of-range precision or scale is refused with SQLSTATE `22023`, as
//! PostgreSQL refuses it. A malformed parameter list is refused with `42601`.

use nodedb_types::columnar::{ColumnType, ColumnTypeParseError};

use super::super::result::DdlError;

/// Refuse every column whose declared type is a `DECIMAL` or `NUMERIC` with
/// an invalid typmod.
///
/// Each pair carries a column name and its declared type text, modifiers
/// included.
pub(super) fn validate_declared_typmods(columns: &[(String, String)]) -> Result<(), DdlError> {
    for (column, declared_type) in columns {
        validate_declared_typmod(column, declared_type)?;
    }
    Ok(())
}

/// Refuse one column whose declared type is a `DECIMAL` or `NUMERIC` with an
/// invalid typmod.
///
/// Every other parse error is left to the engine's own schema builder. A
/// schemaless column can name a custom type this parser does not know.
pub(super) fn validate_declared_typmod(column: &str, declared_type: &str) -> Result<(), DdlError> {
    let Err(error) = ColumnType::parse_declared_type(declared_type) else {
        return Ok(());
    };
    if matches!(
        error,
        ColumnTypeParseError::InvalidDecimalTypmod(_)
            | ColumnTypeParseError::InvalidDecimalParams(_)
    ) {
        return Err(DdlError::new(
            error.sqlstate(),
            format!("column '{column}': {error}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declared(name: &str, declared_type: &str) -> (String, String) {
        (name.to_string(), declared_type.to_string())
    }

    #[test]
    fn valid_and_plain_decimals_pass() {
        let columns = [
            declared("a", "DECIMAL(5,2)"),
            declared("b", "NUMERIC(28, 28) NOT NULL"),
            declared("c", "DECIMAL"),
            declared("d", "NUMERIC DEFAULT 1.5"),
            declared("e", "TEXT"),
            declared("f", "my_custom_type"),
        ];
        assert!(validate_declared_typmods(&columns).is_ok());
    }

    #[test]
    fn out_of_range_typmod_is_22023() {
        for declared_type in [
            "DECIMAL(1001,0)",
            "DECIMAL(0)",
            "NUMERIC(5,6)",
            "DECIMAL(29,0) NOT NULL",
            "DECIMAL(38, 10)",
        ] {
            let error = validate_declared_typmods(&[declared("v", declared_type)])
                .expect_err(declared_type);
            assert_eq!(error.sqlstate, "22023", "{declared_type}");
            assert!(
                error.message.contains("'v'"),
                "{declared_type}: {}",
                error.message
            );
        }
    }

    #[test]
    fn malformed_typmod_is_42601() {
        for declared_type in ["DECIMAL(a,2)", "NUMERIC(5,2,1)", "DECIMAL()"] {
            let error = validate_declared_typmods(&[declared("v", declared_type)])
                .expect_err(declared_type);
            assert_eq!(error.sqlstate, "42601", "{declared_type}");
        }
    }
}
