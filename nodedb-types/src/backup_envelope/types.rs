// SPDX-License-Identifier: Apache-2.0

//! Shared types, constants, and error definitions for the backup envelope.

use thiserror::Error;

pub const MAGIC: &[u8; 4] = b"NDBB";

/// Backup envelope version stamped in byte 4 of every envelope header.
/// Both plaintext and encrypted envelopes use version 1; the presence
/// of the crypto block (68 bytes after the header) distinguishes them.
pub const VERSION: u8 = 1;

/// Header is fixed-size — 52 bytes (48 framed + 4 crc).
///
/// The header grew by 8 bytes when `tenant_id` was widened from u32 to u64
/// (format v1, pre-launch format break).
pub const HEADER_LEN: usize = 52;
/// Per-section framing overhead: origin(8) + len(4) + crc(4).
pub const SECTION_OVERHEAD: usize = 16;
/// Trailing crc.
pub const TRAILER_LEN: usize = 4;

/// Default cap on total envelope size: 16 GiB. Tunable per call.
pub const DEFAULT_MAX_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
/// Default cap on a single section body: 16 GiB.
pub const DEFAULT_MAX_SECTION_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Sentinel `origin_node_id` values that mark sections carrying
/// metadata rather than per-node engine data. Restore handlers
/// recognize the sentinel and route the body to the correct
/// catalog writer. Section CRCs validate independently of whether
/// the reader acts on the section body.
pub const SECTION_ORIGIN_CATALOG_ROWS: u64 = 0xFFFF_FFFF_FFFF_FFF0;
pub const SECTION_ORIGIN_SOURCE_TOMBSTONES: u64 = 0xFFFF_FFFF_FFFF_FFF1;
/// Section carrying the tenant's PK→surrogate identity bindings. The surrogate
/// map is DATA-derived per-node state that the per-node data sections do NOT
/// carry (the Data-Plane snapshot handler has no catalog access), so without
/// this section a restored node has documents but cannot resolve PK
/// point-lookups (`WHERE id=<pk>`). The body is a msgpack-encoded
/// `Vec<SurrogateBindBlob>`.
pub const SECTION_ORIGIN_SURROGATE_PK: u64 = 0xFFFF_FFFF_FFFF_FFF2;

/// Single catalog-row entry in a catalog-rows section. The outer
/// container is `Vec<StoredCollectionBlob>` msgpack-encoded into the
/// section body. Bytes are the zerompk-encoded `StoredCollection`
/// from the `nodedb` crate — `nodedb-types` intentionally doesn't
/// depend on the `nodedb` catalog types, so the blob is opaque here.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredCollectionBlob {
    pub name: String,
    /// zerompk-encoded `StoredCollection`.
    pub bytes: Vec<u8>,
}

/// Single source-side tombstone entry. `purge_lsn` is the Origin WAL
/// LSN at which the hard-delete committed — restore uses it as a
/// per-collection replay barrier so rows older than the purge don't
/// resurrect.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct SourceTombstoneEntry {
    pub collection: String,
    pub purge_lsn: u64,
}

/// Single PK→surrogate binding carried in a `SECTION_ORIGIN_SURROGATE_PK`
/// section. The outer container is `Vec<SurrogateBindBlob>` msgpack-encoded into
/// the section body. Mirrors one row of the source catalog's `surrogate_pk_v3`
/// table for one `(tenant_id, collection)`; rebound on the restore side so PK
/// point-lookups resolve.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct SurrogateBindBlob {
    pub tenant_id: u64,
    pub collection: String,
    pub pk: Vec<u8>,
    pub surrogate: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum EnvelopeError {
    #[error("invalid backup format")]
    BadMagic,
    #[error("unsupported backup version: {0}")]
    UnsupportedVersion(u8),
    #[error("invalid backup format")]
    HeaderCrcMismatch,
    #[error("invalid backup format")]
    BodyCrcMismatch,
    #[error("invalid backup format")]
    TrailerCrcMismatch,
    #[error("backup truncated")]
    Truncated,
    #[error("backup tenant mismatch: expected {expected}, got {actual}")]
    TenantMismatch { expected: u64, actual: u64 },
    #[error("backup exceeds size cap of {cap} bytes")]
    OverSizeTotal { cap: u64 },
    #[error("backup section exceeds size cap of {cap} bytes")]
    OverSizeSection { cap: u64 },
    #[error("too many sections: {0}")]
    TooManySections(u16),
    /// The KEK presented at restore time does not match the KEK fingerprint
    /// embedded in the envelope. Surfaces before any decryption attempt so
    /// the caller receives a clear, actionable error rather than an opaque
    /// authentication failure.
    #[error("wrong backup KEK: presented key fingerprint does not match envelope")]
    WrongBackupKek,
    /// AES-256-GCM authentication tag verification failed. Either the
    /// ciphertext or the key is corrupt.
    #[error("backup decryption failed: authentication tag mismatch")]
    DecryptionFailed,
    /// AES-256-GCM encryption failed (e.g. plaintext exceeds the per-message
    /// limit of 2^36 - 31 bytes). Distinct from `DecryptionFailed` so callers
    /// can tell which side of the crypto pipeline produced the error.
    #[error("backup encryption failed")]
    EncryptionFailed,
    /// `getrandom` returned an error when generating nonces or the DEK.
    #[error("backup encryption failed: {0}")]
    RandomFailure(String),
}

/// Header metadata captured at backup time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeMeta {
    pub tenant_id: u64,
    pub source_vshard_count: u16,
    pub hash_seed: u64,
    pub snapshot_watermark: u64,
}

/// One contiguous body produced by one origin node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    pub origin_node_id: u64,
    pub body: Vec<u8>,
}

/// Decoded envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub meta: EnvelopeMeta,
    pub sections: Vec<Section>,
}

// ── byte helpers ─────────────────────────────────────────────────────────────

pub fn read2(s: &[u8]) -> [u8; 2] {
    [s[0], s[1]]
}
pub fn read4(s: &[u8]) -> [u8; 4] {
    [s[0], s[1], s[2], s[3]]
}
pub fn read8(s: &[u8]) -> [u8; 8] {
    [s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]
}
