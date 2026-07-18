// SPDX-License-Identifier: Apache-2.0

//! `ColumnData` enum definition and constructor.

use nodedb_types::columnar::ColumnType;

/// Maximum cardinality for automatic dictionary encoding.
pub const DICT_ENCODE_MAX_CARDINALITY: u32 = 1024;

/// A single column's data in the memtable.
///
/// Each variant stores a contiguous Vec of the appropriate primitive type
/// plus an optional validity bitmap. When `valid` is `None`, all rows are
/// considered valid — this is the fast path for non-nullable columns,
/// eliminating one `Vec::push` per row and improving cache density.
///
/// When `valid` is `Some`, `true` = present, `false` = null.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ColumnData {
    Int64 {
        values: Vec<i64>,
        valid: Option<Vec<bool>>,
    },
    Float64 {
        values: Vec<f64>,
        valid: Option<Vec<bool>>,
    },
    Bool {
        values: Vec<bool>,
        valid: Option<Vec<bool>>,
    },
    Timestamp {
        values: Vec<i64>,
        valid: Option<Vec<bool>>,
    },
    Decimal {
        /// Stored as 16-byte serialized representations.
        values: Vec<[u8; 16]>,
        valid: Option<Vec<bool>>,
    },
    Uuid {
        /// Stored as 16-byte binary representations.
        values: Vec<[u8; 16]>,
        valid: Option<Vec<bool>>,
    },
    String {
        /// Concatenated string bytes.
        data: Vec<u8>,
        /// Byte offsets: offset[i] is the start of string i, offset[len] is end sentinel.
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Bytes {
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    /// JSON column: values serialized as MessagePack for structured access.
    ///
    /// Storage layout mirrors `Bytes` (offset-coded variable-length data), but
    /// the variant is distinct so exhaustive `match` arms across the codebase
    /// are forced to handle JSON explicitly.  Encoded by `zerompk`; decoded
    /// back to `nodedb_types::Value` on read so JSON path operators see a
    /// real `Value::Object` / `Value::Array`, not an opaque byte slice.
    Json {
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Geometry {
        /// Stored as JSON-serialized geometry bytes.
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Vector {
        /// Packed f32 values: dim floats per row.
        data: Vec<f32>,
        dim: u32,
        valid: Option<Vec<bool>>,
    },
    /// Dictionary-encoded string column: stores u32 symbol IDs + dictionary.
    ///
    /// Low-cardinality string columns (e.g. `qtype`, `rcode`) are converted to
    /// this representation before segment flush. The IDs are delta-encoded as
    /// i64 for compact storage; the dictionary is stored in `ColumnMeta`.
    DictEncoded {
        /// Symbol IDs per row (index into dictionary).
        ids: Vec<u32>,
        /// Dictionary: ID → string value.
        dictionary: Vec<String>,
        /// Reverse lookup: string → ID.
        reverse: std::collections::HashMap<String, u32>,
        valid: Option<Vec<bool>>,
    },
}

impl ColumnData {
    /// Create an empty column for the given type.
    ///
    /// When `nullable` is false, validity bitmap is omitted (`None`) — the fast
    /// path for non-nullable columns that saves one `Vec::push` per row.
    pub(crate) fn new(col_type: &ColumnType, nullable: bool) -> Self {
        let valid = if nullable { Some(Vec::new()) } else { None };
        match col_type {
            ColumnType::Int64 => Self::Int64 {
                values: Vec::new(),
                valid,
            },
            ColumnType::Float64 => Self::Float64 {
                values: Vec::new(),
                valid,
            },
            ColumnType::Bool => Self::Bool {
                values: Vec::new(),
                valid,
            },
            ColumnType::Timestamp | ColumnType::Timestamptz | ColumnType::SystemTimestamp => {
                Self::Timestamp {
                    values: Vec::new(),
                    valid,
                }
            }
            ColumnType::Decimal { .. } => Self::Decimal {
                values: Vec::new(),
                valid,
            },
            ColumnType::Uuid => Self::Uuid {
                values: Vec::new(),
                valid,
            },
            ColumnType::String => Self::String {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            ColumnType::Bytes => Self::Bytes {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            ColumnType::Json => Self::Json {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            ColumnType::Geometry => Self::Geometry {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            ColumnType::Vector(dim) => Self::Vector {
                data: Vec::new(),
                dim: *dim,
                valid,
            },
            ColumnType::Array | ColumnType::Set | ColumnType::Range | ColumnType::Record => {
                Self::Bytes {
                    data: Vec::new(),
                    offsets: vec![0],
                    valid,
                }
            }
            ColumnType::Ulid => Self::Uuid {
                values: Vec::new(),
                valid,
            },
            ColumnType::Duration => Self::Timestamp {
                values: Vec::new(),
                valid,
            },
            ColumnType::Regex => Self::String {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            // SparseVector holds a `'{id: weight}'` UTF-8 literal — string-backed.
            ColumnType::SparseVector => Self::String {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            // ColumnType is #[non_exhaustive]; unknown future types are stored
            // as raw bytes until the memtable learns about them.
            _ => Self::Bytes {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
        }
    }

    /// Number of rows in this column.
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Int64 { values, .. } => values.len(),
            Self::Float64 { values, .. } => values.len(),
            Self::Bool { values, .. } => values.len(),
            Self::Timestamp { values, .. } => values.len(),
            Self::Decimal { values, .. } => values.len(),
            Self::Uuid { values, .. } => values.len(),
            Self::String { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Bytes { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Json { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Geometry { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Vector { data, dim, .. } => {
                if *dim == 0 {
                    0
                } else {
                    data.len() / *dim as usize
                }
            }
            Self::DictEncoded { ids, .. } => ids.len(),
        }
    }
}
