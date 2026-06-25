// SPDX-License-Identifier: Apache-2.0

pub mod crypto;
pub mod read;
pub mod types;
pub mod write;

pub use crypto::parse_encrypted;
pub use read::parse;
pub use types::{
    DEFAULT_MAX_SECTION_BYTES, DEFAULT_MAX_TOTAL_BYTES, HEADER_LEN, MAGIC,
    SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_SOURCE_TOMBSTONES, SECTION_ORIGIN_SURROGATE_PK,
    SECTION_OVERHEAD, TRAILER_LEN, VERSION,
};
pub use types::{
    Envelope, EnvelopeError, EnvelopeMeta, Section, SourceTombstoneEntry, StoredCollectionBlob,
    SurrogateBindBlob,
};
pub use write::EnvelopeWriter;
