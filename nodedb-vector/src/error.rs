// SPDX-License-Identifier: Apache-2.0

//! Vector engine error types.

use nodedb_mem::MemError;

/// Errors from vector index operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum VectorError {
    #[error("memory budget exhausted: {0}")]
    BudgetExhausted(#[from] MemError),
    #[error("vector dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("unsupported HNSW checkpoint version {found}; expected {expected}")]
    UnsupportedVersion { found: u8, expected: u8 },
    #[error("invalid PQ codec magic bytes")]
    InvalidMagic,
    #[error("PQ codec deserialization failed: {0}")]
    DeserializationFailed(String),
    /// Checkpoint file is encrypted (starts with `SEGV`) but no KEK was supplied.
    #[error(
        "vector checkpoint is encrypted but no encryption key was provided; \
         cannot load plaintext from an encrypted checkpoint"
    )]
    CheckpointEncryptedNoKey,
    /// Checkpoint file is plaintext but a KEK was configured (policy violation).
    #[error(
        "vector checkpoint is plaintext but an encryption key is configured; \
         refusing to load an unencrypted checkpoint when encryption is required"
    )]
    CheckpointPlaintextKeyRequired,
    /// AES-256-GCM encryption/decryption or envelope framing of a checkpoint failed.
    #[error("vector checkpoint encryption error: {detail}")]
    CheckpointEncryptionError { detail: String },
    /// rkyv or MessagePack serialization of a vector checkpoint failed.
    #[error("vector checkpoint serialization error: {detail}")]
    CheckpointSerializationError { detail: String },
    /// I/O error from segment file operations (open, mmap, metadata).
    #[error("vector segment I/O error: {0}")]
    SegmentIo(#[from] std::io::Error),
}
