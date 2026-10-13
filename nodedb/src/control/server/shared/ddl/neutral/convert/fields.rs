// SPDX-License-Identifier: BUSL-1.1

//! The catalog's declared-field record for a converted schema.
//!
//! `DESCRIBE`, enum-label validation and auto-index derivation read
//! `StoredCollection::fields`. A conversion rewrites it from the converted
//! columns, in the spelling `CREATE COLLECTION` stores.

use nodedb_types::columnar::ColumnDef;

/// The declared text each converted column records in `coll.fields`.
///
/// `generated_sources` pairs a generated column with the expression text it
/// was declared with.
pub(super) fn converted_fields(
    columns: &[ColumnDef],
    generated_sources: &[(String, String)],
) -> Vec<(String, String)> {
    columns
        .iter()
        .map(|column| {
            let mut text = column.declared_type_name();
            if !column.nullable {
                text.push_str(" NOT NULL");
            }
            if column.primary_key {
                text.push_str(" PRIMARY KEY");
            }
            if let Some(default) = &column.default {
                text.push_str(" DEFAULT ");
                text.push_str(default);
            }
            if let Some((_, source)) = generated_sources
                .iter()
                .find(|(name, _)| *name == column.name)
            {
                text.push_str(" GENERATED ALWAYS AS (");
                text.push_str(source);
                text.push(')');
            }
            (column.name.clone(), text)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnType;

    use super::*;

    /// Each clause a column carries lands in its declared text.
    #[test]
    fn every_clause_reaches_the_declared_text() {
        let id = ColumnDef::required("id", ColumnType::String).with_primary_key();
        let status = ColumnDef::nullable("status", ColumnType::String).with_default("'new'");
        let mut lowered = ColumnDef::nullable("lowered", ColumnType::String);
        lowered.generated_expr = Some("{}".to_string());
        let fields = converted_fields(
            &[id, status, lowered],
            &[("lowered".to_string(), "LOWER(status)".to_string())],
        );
        assert_eq!(fields[0].1, "TEXT NOT NULL PRIMARY KEY");
        assert_eq!(fields[1].1, "TEXT DEFAULT 'new'");
        assert_eq!(fields[2].1, "TEXT GENERATED ALWAYS AS (LOWER(status))");
    }
}
