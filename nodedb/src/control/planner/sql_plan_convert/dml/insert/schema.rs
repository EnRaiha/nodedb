// SPDX-License-Identifier: BUSL-1.1

pub(crate) use nodedb_types::DEFAULT_IDENTITY_COLUMN;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

/// Build a `ColumnarSchema` from raw catalog column-type strings.
///
/// `column_schema` is the list of `(column_name, type_str)` pairs from the
/// DDL catalog (`stored.fields`). Unknown type strings are treated as
/// `ColumnType::String` (matching the memtable's existing fallback).
///
/// `identity_column` names the column that carries the row's identity: the
/// DDL-declared `PRIMARY KEY` when one exists, else the engine's resolved
/// primary key. The column of that name is the schema primary key. When no
/// column carries that name, a required `String` column is synthesized under
/// it.
///
/// Returns `None` when `column_schema` is empty, meaning no catalog schema is
/// available, or when the resulting schema fails validation.
///
/// This is the single source of truth for turning a catalog's raw
/// `(name, type_str)` field list into a typed `ColumnarSchema` — shared by
/// the live SQL insert path (via [`build_schema_bytes`]) and
/// `bootstrap::data_plane::load_columnar_schema_seed`, which pre-registers
/// each columnar-family collection's real schema before WAL replay so a
/// fresh `MutationEngine` never falls back to type-lossy inference.
pub(crate) fn build_columnar_schema(
    column_schema: &[(String, String)],
    identity_column: &str,
) -> Option<ColumnarSchema> {
    if column_schema.is_empty() {
        return None;
    }
    let mut cols = Vec::with_capacity(column_schema.len());
    let mut has_id = false;
    for (name, type_str) in column_schema {
        // `type_str` carries SQL modifiers such as `NOT NULL` or `PRIMARY KEY`
        // ("BIGINT NOT NULL", "DECIMAL(10, 2) NOT NULL"). The declared-type
        // resolver reads the leading type token and keeps a spaced parameter
        // list whole.
        let col_type = ColumnType::from_declared_type(type_str).unwrap_or(ColumnType::String);
        let col = if name == identity_column {
            has_id = true;
            ColumnDef::required(name.clone(), col_type).with_primary_key()
        } else {
            ColumnDef::nullable(name.clone(), col_type)
        };
        cols.push(col.with_declared_width(type_str));
    }
    // No column carries the identity: synthesize it.
    if !has_id {
        cols.insert(
            0,
            ColumnDef::required(identity_column, ColumnType::String).with_primary_key(),
        );
    }
    ColumnarSchema::new(cols).ok()
}

/// Build a `ColumnarSchema` from raw catalog column-type strings, then
/// serialize it as MessagePack for the `ColumnarOp::Insert::schema_bytes` field.
///
/// `identity_column` is forwarded to [`build_columnar_schema`] unchanged.
///
/// Returns an empty `Vec` when `column_schema` is empty or fails validation
/// — see [`build_columnar_schema`] for the typed builder this wraps.
pub(in super::super) fn build_schema_bytes(
    column_schema: &[(String, String)],
    identity_column: &str,
) -> Vec<u8> {
    build_columnar_schema(column_schema, identity_column)
        .map(|schema| zerompk::to_msgpack_vec(&schema).unwrap_or_default())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{DecimalTypmod, FloatWidth, IntWidth};

    use super::*;

    #[test]
    fn columnar_schema_keeps_declared_widths_and_typmods() {
        let fields: Vec<(String, String)> = [
            ("id", "BIGINT PRIMARY KEY"),
            ("s", "SMALLINT NOT NULL"),
            ("r", "REAL"),
            ("d", "DECIMAL(10, 2) NOT NULL"),
        ]
        .into_iter()
        .map(|(name, declared)| (name.to_string(), declared.to_string()))
        .collect();
        let schema = build_columnar_schema(&fields, "id").expect("valid schema");
        let column = |name: &str| {
            schema
                .columns
                .iter()
                .find(|c| c.name == name)
                .cloned()
                .expect("column present")
        };
        assert_eq!(column("id").int_width, Some(IntWidth::I64));
        assert_eq!(column("s").int_width, Some(IntWidth::I16));
        assert_eq!(column("r").float_width, Some(FloatWidth::F32));
        assert_eq!(
            column("d").column_type,
            ColumnType::Decimal(Some(DecimalTypmod::new(10, 2).expect("valid typmod")))
        );
    }
}
