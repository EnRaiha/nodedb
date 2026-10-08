// SPDX-License-Identifier: BUSL-1.1

//! The pgwire wire type of each result column type: its PostgreSQL type,
//! and the format its cells travel in.
//!
//! Every result path builds its RowDescription and its cell bytes from these
//! functions, so a column's advertised format always matches its bytes.

use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo};

use crate::control::server::response_shape::types::DdlColType;

/// The result formats of a statement that requests text for every column,
/// as the simple-query protocol does.
pub static TEXT_RESULTS: Format = Format::UnifiedText;

/// The PostgreSQL type a column of type `ct` advertises.
pub fn pg_type(ct: DdlColType) -> Type {
    match ct {
        DdlColType::Text => Type::TEXT,
        DdlColType::Int8 => Type::INT8,
        DdlColType::Int4 => Type::INT4,
        DdlColType::Int2 => Type::INT2,
        DdlColType::Float8 => Type::FLOAT8,
        DdlColType::Float4 => Type::FLOAT4,
        DdlColType::Bool => Type::BOOL,
        DdlColType::Bytea => Type::BYTEA,
        DdlColType::Json => Type::JSON,
        DdlColType::Jsonb => Type::JSONB,
        DdlColType::Timestamp => Type::TIMESTAMP,
        DdlColType::Timestamptz => Type::TIMESTAMPTZ,
        DdlColType::Varchar => Type::VARCHAR,
        DdlColType::Float4Array => Type::FLOAT4_ARRAY,
        DdlColType::Float8Array => Type::FLOAT8_ARRAY,
        DdlColType::Numeric => Type::NUMERIC,
        DdlColType::Uuid => Type::UUID,
    }
}

/// Whether a column of type `ct` travels in binary when the client asks for
/// binary.
///
/// The integers, floats, `bool` and the timestamps have a PostgreSQL binary
/// form. `text` and `varchar` binary bytes are their text bytes. Every other
/// type travels in PostgreSQL text form: `bytea` as `\x` hex, `numeric`,
/// `uuid`, `json`, `jsonb`, and arrays as `{...}` literals. Its
/// RowDescription field carries format code 0, so a client that reads format
/// codes decodes text. A client that does not read them decodes these types
/// as text by this rule.
pub fn binary_capable(ct: DdlColType) -> bool {
    match ct {
        DdlColType::Int8
        | DdlColType::Int4
        | DdlColType::Int2
        | DdlColType::Float8
        | DdlColType::Float4
        | DdlColType::Bool
        | DdlColType::Timestamp
        | DdlColType::Timestamptz
        | DdlColType::Text
        | DdlColType::Varchar => true,
        DdlColType::Bytea
        | DdlColType::Json
        | DdlColType::Jsonb
        | DdlColType::Float4Array
        | DdlColType::Float8Array
        | DdlColType::Numeric
        | DdlColType::Uuid => false,
    }
}

/// The format the client requested for result column `idx`.
///
/// A Bind with fewer format codes than result columns leaves the remaining
/// columns in text. `Format::format_for` indexes its codes unchecked, so it
/// is not called for `Individual`.
pub fn requested_format(requested: &Format, idx: usize) -> FieldFormat {
    match requested {
        Format::Individual(codes) => codes
            .get(idx)
            .map_or(FieldFormat::Text, |code| FieldFormat::from(*code)),
        Format::UnifiedText => FieldFormat::Text,
        Format::UnifiedBinary => FieldFormat::Binary,
    }
}

/// The format a cell of type `ct` travels in when the client asked for
/// `format`: binary only for a binary-capable type, text otherwise.
pub fn effective_format(ct: DdlColType, format: FieldFormat) -> FieldFormat {
    if format == FieldFormat::Binary && binary_capable(ct) {
        FieldFormat::Binary
    } else {
        FieldFormat::Text
    }
}

/// The format result column `idx` of type `ct` travels in under the
/// client's `requested` result formats.
pub fn result_format(ct: DdlColType, requested: &Format, idx: usize) -> FieldFormat {
    effective_format(ct, requested_format(requested, idx))
}

/// The RowDescription field of result column `name` of type `ct`. The
/// field's format is `format` for a binary-capable type and text otherwise.
pub fn result_field(name: &str, ct: DdlColType, format: FieldFormat) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        pg_type(ct),
        effective_format(ct, format),
    )
}

/// The RowDescription of the result columns `columns` (name and type) under
/// the client's `requested` result formats.
pub fn result_fields<'a>(
    columns: impl IntoIterator<Item = (&'a str, DdlColType)>,
    requested: &Format,
) -> Vec<FieldInfo> {
    columns
        .into_iter()
        .enumerate()
        .map(|(idx, (name, ct))| result_field(name, ct, result_format(ct, requested, idx)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [DdlColType; 17] = [
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int4,
        DdlColType::Int2,
        DdlColType::Float8,
        DdlColType::Float4,
        DdlColType::Bool,
        DdlColType::Bytea,
        DdlColType::Json,
        DdlColType::Jsonb,
        DdlColType::Timestamp,
        DdlColType::Timestamptz,
        DdlColType::Varchar,
        DdlColType::Float4Array,
        DdlColType::Float8Array,
        DdlColType::Numeric,
        DdlColType::Uuid,
    ];

    /// Each column type advertises its own PostgreSQL type, and no two share
    /// one.
    #[test]
    fn every_column_type_has_a_distinct_pg_type() {
        for (i, a) in ALL.iter().enumerate() {
            for b in ALL.iter().skip(i + 1) {
                assert_ne!(pg_type(*a), pg_type(*b), "{a:?} and {b:?}");
            }
        }
        assert_eq!(pg_type(DdlColType::Numeric), Type::NUMERIC);
        assert_eq!(pg_type(DdlColType::Uuid), Type::UUID);
        assert_eq!(pg_type(DdlColType::Float4Array), Type::FLOAT4_ARRAY);
    }

    /// Binary travels only for the types with a binary form. The text-form
    /// types answer text under a binary request.
    #[test]
    fn binary_is_honoured_only_for_binary_capable_types() {
        for ct in ALL {
            let expected = if binary_capable(ct) {
                FieldFormat::Binary
            } else {
                FieldFormat::Text
            };
            assert_eq!(
                result_format(ct, &Format::UnifiedBinary, 0),
                expected,
                "{ct:?}"
            );
            assert_eq!(
                result_format(ct, &Format::UnifiedText, 0),
                FieldFormat::Text,
                "{ct:?}"
            );
        }
        for ct in [
            DdlColType::Bytea,
            DdlColType::Numeric,
            DdlColType::Uuid,
            DdlColType::Json,
            DdlColType::Float4Array,
            DdlColType::Float8Array,
        ] {
            assert!(!binary_capable(ct), "{ct:?} travels as text");
        }
    }

    /// A Bind with fewer codes than columns leaves the rest text.
    #[test]
    fn individual_codes_shorter_than_the_columns_default_to_text() {
        let requested = Format::Individual(vec![1]);
        assert_eq!(
            result_format(DdlColType::Int8, &requested, 0),
            FieldFormat::Binary
        );
        assert_eq!(
            result_format(DdlColType::Int8, &requested, 1),
            FieldFormat::Text
        );
    }

    /// The RowDescription field's format matches the format its cells use.
    #[test]
    fn result_fields_carry_the_effective_format() {
        let fields = result_fields(
            [
                ("n", DdlColType::Int8),
                ("price", DdlColType::Numeric),
                ("id", DdlColType::Uuid),
                ("ts", DdlColType::Timestamp),
            ],
            &Format::UnifiedBinary,
        );
        let formats: Vec<FieldFormat> = fields.iter().map(FieldInfo::format).collect();
        assert_eq!(
            formats,
            vec![
                FieldFormat::Binary,
                FieldFormat::Text,
                FieldFormat::Text,
                FieldFormat::Binary
            ]
        );
        assert_eq!(fields[1].datatype(), &Type::NUMERIC);
        assert_eq!(
            result_field("b", DdlColType::Bytea, FieldFormat::Binary).format(),
            FieldFormat::Text
        );
    }
}
