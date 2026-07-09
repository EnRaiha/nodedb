// SPDX-License-Identifier: BUSL-1.1

//! Per-engine segment/checkpoint encryption keys (WAL-derived), grouped so
//! the core-loop state struct stays cohesive.

/// At-rest encryption keys for per-engine segment and checkpoint files.
pub(in crate::data::executor) struct SegmentKeks {
    /// Encryption key for at-rest encryption of vector checkpoints.
    ///
    /// When `Some`, `checkpoint_vector_indexes` writes encrypted checkpoint
    /// files and `load_vector_checkpoints` refuses to load plaintext ones.
    /// Sourced from the same WAL key used by `nodedb-wal` and snapshot writers.
    pub(in crate::data::executor) vector_checkpoint_kek:
        Option<nodedb_wal::crypto::WalEncryptionKey>,

    /// Encryption key for at-rest encryption of spatial (R-tree and geohash) checkpoints.
    ///
    /// When `Some`, `checkpoint_spatial_indexes` writes encrypted checkpoint files
    /// and `load_spatial_checkpoints` refuses to load plaintext ones.
    pub(in crate::data::executor) spatial_checkpoint_kek:
        Option<nodedb_wal::crypto::WalEncryptionKey>,

    /// Encryption key for at-rest encryption of columnar segments.
    ///
    /// When `Some`, columnar segment flushes wrap the segment bytes in an
    /// AES-256-GCM SEGC envelope and the reader refuses to load plaintext
    /// segments.
    pub(in crate::data::executor) columnar_segment_kek:
        Option<nodedb_wal::crypto::WalEncryptionKey>,

    /// Encryption key for at-rest encryption of array (NDAS) segments.
    ///
    /// When `Some`, array segment flushes wrap the segment bytes in an
    /// AES-256-GCM SEGA envelope and the segment handle refuses to load
    /// plaintext segments.
    pub(in crate::data::executor) array_segment_kek: Option<nodedb_wal::crypto::WalEncryptionKey>,

    /// Encryption key for at-rest encryption of timeseries columnar segment files
    /// (`.col`, `.sym`, `schema.json`, `sparse_index.bin`, `partition.meta`).
    ///
    /// When `Some`, `flush_ts_collection` writes SEGT-encrypted files; readers
    /// refuse to load plaintext segment files.
    pub(in crate::data::executor) ts_segment_kek: Option<nodedb_wal::crypto::WalEncryptionKey>,
}
