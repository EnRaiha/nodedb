// SPDX-License-Identifier: BUSL-1.1

//! The columns a collection computes on every write, and the refusal of a
//! write that supplies one.
//!
//! A generated column's value is computed by the engine, so a value a write
//! supplies would never be stored. Every write form refuses it with SQLSTATE
//! `428C9`, as an UPDATE of the column is refused.

use nodedb_types::columnar::ColumnDef;
use nodedb_types::{CollectionType, DocumentMode};

use super::types::StoredCollection;

impl StoredCollection {
    /// Every column this collection computes on write: a strict-schema
    /// `GENERATED ALWAYS AS` column, or a generated schemaless field.
    pub fn generated_columns(&self) -> Vec<String> {
        let strict: &[ColumnDef] = match &self.collection_type {
            CollectionType::Document(DocumentMode::Strict(schema)) => &schema.columns,
            CollectionType::Document(DocumentMode::Schemaless)
            | CollectionType::KeyValue(_)
            | CollectionType::Columnar(_) => &[],
        };
        strict
            .iter()
            .filter(|c| c.generated_expr.is_some())
            .map(|c| c.name.clone())
            .chain(
                self.field_defs
                    .iter()
                    .filter(|f| f.is_generated)
                    .map(|f| f.name.clone()),
            )
            .collect()
    }
}

/// Refuse a write that supplies a value for a generated column.
///
/// `supplied` names every column the write supplies. The refusal is SQLSTATE
/// `428C9` and names the column.
pub fn refuse_generated_write<'a>(
    collection: &str,
    generated: &[String],
    supplied: impl IntoIterator<Item = &'a str>,
) -> crate::Result<()> {
    if generated.is_empty() {
        return Ok(());
    }
    match supplied
        .into_iter()
        .find(|name| generated.iter().any(|g| g == name))
    {
        Some(column) => Err(crate::Error::RejectedConstraint {
            collection: collection.to_string(),
            constraint: "generated_always".to_string(),
            detail: format!("cannot insert a value into generated column '{column}'"),
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A write that names a generated column is refused, naming it. A write
    /// that omits it passes.
    #[test]
    fn a_supplied_generated_column_is_refused() {
        let generated = vec!["tax".to_string()];
        let error =
            refuse_generated_write("g", &generated, ["id", "tax"]).expect_err("tax is generated");
        match error {
            crate::Error::RejectedConstraint {
                constraint, detail, ..
            } => {
                assert_eq!(constraint, "generated_always");
                assert!(detail.contains("'tax'"), "{detail}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        refuse_generated_write("g", &generated, ["id", "price"]).expect("tax is omitted");
    }
}
