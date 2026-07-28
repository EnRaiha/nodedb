// SPDX-License-Identifier: Apache-2.0

//! `StrictSchema` construction for the strict-document engine.

use std::str::FromStr;

use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

use super::type_str::parse_column_type_str_full;

/// Build a `StrictSchema` from pre-extracted `(name, type_str)` column pairs.
///
/// Auto-inserts a `_rowid INT64 PRIMARY KEY` if no PRIMARY KEY is declared.
/// Returns `Err` if `columns` is empty or any type string is unknown.
pub(crate) fn build_strict_schema(
    columns: &[(String, String)],
    bitemporal: bool,
) -> Result<StrictSchema, SqlError> {
    if columns.is_empty() {
        return Err(SqlError::Parse {
            detail: "document_strict requires at least one column".to_string(),
        });
    }

    let mut col_defs: Vec<ColumnDef> = Vec::with_capacity(columns.len());
    for (name, type_str) in columns {
        let (bare_type, is_pk, is_not_null, default_expr) = parse_column_type_str_full(type_str);
        let column_type = ColumnType::from_str(&bare_type).map_err(
            |e: nodedb_types::columnar::ColumnTypeParseError| SqlError::Parse {
                detail: e.to_string(),
            },
        )?;
        let nullable = !is_not_null && !is_pk;
        let mut col = if nullable {
            ColumnDef::nullable(name.clone(), column_type)
        } else {
            ColumnDef::required(name.clone(), column_type)
        };
        if is_pk {
            col = col.with_primary_key();
        }
        if let Some(expr) = default_expr {
            col = col.with_default(expr);
        }

        // GENERATED ALWAYS AS: extract and store the expression when present in type_str.
        let gen_kw = ["GENERATED ALWAYS AS", "GENERATED AS"]
            .into_iter()
            .find_map(|keyword| {
                find_ascii_case_insensitive(type_str, keyword).map(|position| (position, keyword))
            });
        if let Some((gen_pos, kw)) = gen_kw {
            let after_gen = type_str.get(gen_pos + kw.len()..).unwrap_or("").trim();
            if after_gen.starts_with('(') {
                let mut depth = 0usize;
                let mut end = 0usize;
                for (i, ch) in after_gen.char_indices() {
                    match ch {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                end = i;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                if end > 1 {
                    let expr_text = after_gen
                        .strip_prefix('(')
                        .and_then(|body| end.checked_sub(1).and_then(|last| body.get(..last)))
                        .unwrap_or("");
                    match nodedb_query::expr_parse::parse_generated_expr(expr_text) {
                        Ok((parsed_expr, deps)) => {
                            if let Ok(expr_json) = sonic_rs::to_string(&parsed_expr) {
                                col.generated_expr = Some(expr_json);
                                col.generated_deps = deps;
                                // Generated columns are nullable (computed value may be null).
                                col.nullable = true;
                            }
                        }
                        Err(e) => {
                            return Err(SqlError::Parse {
                                detail: format!("invalid GENERATED expression: {e}"),
                            });
                        }
                    }
                }
            }
        }

        col_defs.push(col);
    }

    if !col_defs.iter().any(|c| c.primary_key) {
        col_defs.insert(
            0,
            ColumnDef::required("_rowid", ColumnType::Int64).with_primary_key(),
        );
    }

    if bitemporal {
        StrictSchema::new_bitemporal(col_defs).map_err(|e| SqlError::Parse {
            detail: e.to_string(),
        })
    } else {
        StrictSchema::new(col_defs).map_err(|e| SqlError::Parse {
            detail: e.to_string(),
        })
    }
}
