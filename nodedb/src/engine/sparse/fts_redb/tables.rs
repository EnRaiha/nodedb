// SPDX-License-Identifier: BUSL-1.1

//! Redb table definitions for the full-text search backend.
//!
//! Every table is keyed by a structural `(database_id, tenant_id, collection, …)`
//! tuple — matching the EdgeStore pattern. Per-`(database, tenant)` drops become
//! range scans `(db, tid, ..)..(db, tid+1, ..)` instead of fragile lexical-prefix
//! scans.

use redb::TableDefinition;

/// Inverted index: key = `(database_id, tenant_id, collection, term)`,
/// value = MessagePack-encoded `Vec<Posting>`.
pub const POSTINGS: TableDefinition<(u64, u64, &str, &str), &[u8]> =
    TableDefinition::new("text.postings");

/// Document lengths: key = `(database_id, tenant_id, collection, surrogate)`,
/// value = MessagePack-encoded `u32` token count.
/// The surrogate is stored as its raw `u32` value (redb native key type).
pub const DOC_LENGTHS: TableDefinition<(u64, u64, &str, u32), &[u8]> =
    TableDefinition::new("text.doc_lengths");

/// Index metadata blobs: key = `(database_id, tenant_id, collection, sub_key)`,
/// value = opaque blob. Sub-keys: `"docmap"`, `"fieldnorms"`, `"analyzer"`,
/// `"language"`.
pub const INDEX_META: TableDefinition<(u64, u64, &str, &str), &[u8]> =
    TableDefinition::new("text.meta");

/// Corpus stats: key = `(database_id, tenant_id, collection)`,
/// value = MessagePack-encoded `(doc_count, total_token_sum)`.
pub const STATS: TableDefinition<(u64, u64, &str), &[u8]> = TableDefinition::new("text.stats");

/// Segment blobs: key = `(database_id, tenant_id, collection, segment_id)`,
/// value = compressed segment bytes. `segment_id` format `"L{level}:{id:016x}"`.
pub const SEGMENTS: TableDefinition<(u64, u64, &str, &str), &[u8]> =
    TableDefinition::new("text.segments");
