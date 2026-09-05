// SPDX-License-Identifier: BUSL-1.1

//! Quarantine wrapper for vector NDVS segment reads.
//!
//! `MmapVectorSegment::open` / `open_with_policy` return `VectorError`.
//! Structural corruption (magic, version, size mismatch, CRC32C mismatch)
//! surfaces as `VectorError::SegmentIo` wrapping an `io::Error` whose kind
//! is `InvalidData` — every other variant (budget exhaustion, a plain I/O
//! error such as a missing file) is not corruption and must not quarantine.

use std::path::Path;
use std::sync::Arc;

use nodedb_mem::ScopedMemory;
use nodedb_vector::error::VectorError;
use nodedb_vector::mmap_segment::{MmapVectorSegment, VectorSegmentDropPolicy};

use crate::storage::quarantine::error::QuarantineError;
use crate::storage::quarantine::registry::{QuarantineEngine, QuarantineRegistry, SegmentKey};

/// Arguments for [`open_vector_segment_with_quarantine`].
pub struct VectorQuarantineOpen<'a> {
    pub registry: &'a Arc<QuarantineRegistry>,
    pub path: &'a Path,
    pub policy: VectorSegmentDropPolicy,
    pub collection: &'a str,
    pub segment_id: &'a str,
    pub memory: &'a ScopedMemory,
}

/// Attempt to open a vector NDVS segment, routing CRC-class corruption
/// through the quarantine registry.
pub fn open_vector_segment_with_quarantine(
    args: VectorQuarantineOpen,
) -> Result<MmapVectorSegment, VectorOrQuarantine> {
    let VectorQuarantineOpen {
        registry,
        path,
        policy,
        collection,
        segment_id,
        memory,
    } = args;

    match MmapVectorSegment::open_with_policy(path, memory, policy) {
        Ok(seg) => {
            let key = SegmentKey {
                engine: QuarantineEngine::Vector,
                collection: collection.to_string(),
                segment_id: segment_id.to_string(),
            };
            registry.record_success(&key);
            Ok(seg)
        }
        Err(VectorError::SegmentIo(e)) if e.kind() == std::io::ErrorKind::InvalidData => {
            let key = SegmentKey {
                engine: QuarantineEngine::Vector,
                collection: collection.to_string(),
                segment_id: segment_id.to_string(),
            };
            // Provide the actual file path for rename on second strike.
            let path_for_rename = if path.exists() { Some(path) } else { None };
            registry
                .record_failure(key, &e.to_string(), path_for_rename)
                .map_err(VectorOrQuarantine::Quarantined)?;
            Err(VectorOrQuarantine::Vector(VectorError::SegmentIo(e)))
        }
        Err(e) => Err(VectorOrQuarantine::Vector(e)),
    }
}

/// Error type returned by `open_vector_segment_with_quarantine`.
#[derive(Debug, thiserror::Error)]
pub enum VectorOrQuarantine {
    #[error(transparent)]
    Vector(#[from] VectorError),
    #[error(transparent)]
    Quarantined(#[from] QuarantineError),
}
