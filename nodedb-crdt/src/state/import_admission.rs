// SPDX-License-Identifier: Apache-2.0

//! Bounded admission checks for encoded Loro imports.

use crate::error::{CrdtError, Result};

/// Maximum encoded bytes accepted by the generic CRDT import API.
pub const DEFAULT_MAX_IMPORT_BYTES: usize = 64 * 1024 * 1024;
/// Maximum operations a generic CRDT import may contribute.
pub const DEFAULT_MAX_IMPORT_OPS: usize = 1_000_000;

/// Explicit resource limits for a Loro import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrdtImportLimits {
    /// Maximum raw encoded Loro bytes before metadata parsing or import.
    pub max_bytes: usize,
    /// Maximum operations encoded in the import, including already-known ones.
    pub max_encoded_operations: usize,
    /// Maximum new operations relative to the current oplog version vector.
    pub max_new_operations: usize,
}

impl Default for CrdtImportLimits {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_IMPORT_BYTES,
            max_encoded_operations: DEFAULT_MAX_IMPORT_OPS,
            max_new_operations: DEFAULT_MAX_IMPORT_OPS,
        }
    }
}

/// Validate an encoded Loro import before a fork or Loro import can allocate.
///
/// Returns the exact number of operations that would be new relative to
/// `current`. Metadata is decoded with Loro's authenticated decoder; malformed
/// or regressing per-peer ranges are rejected before any state mutation.
pub(crate) fn admit_import(
    bytes: &[u8],
    current: &loro::VersionVector,
    limits: CrdtImportLimits,
) -> Result<usize> {
    if bytes.len() > limits.max_bytes {
        return Err(CrdtError::ImportTooLarge {
            limit: limits.max_bytes,
            actual: bytes.len(),
        });
    }

    let metadata = loro::LoroDoc::decode_import_blob_meta(bytes, true).map_err(|error| {
        CrdtError::ImportMalformed {
            detail: error.to_string(),
        }
    })?;
    let encoded = count_range_operations(&metadata.partial_start_vv, &metadata.partial_end_vv)?;
    if encoded > limits.max_encoded_operations {
        return Err(CrdtError::ImportOperationLimitExceeded {
            limit: limits.max_encoded_operations,
            actual: encoded,
        });
    }
    let imported = count_new_operations(
        &metadata.partial_start_vv,
        &metadata.partial_end_vv,
        current,
    )?;
    if imported > limits.max_new_operations {
        return Err(CrdtError::ImportOperationLimitExceeded {
            limit: limits.max_new_operations,
            actual: imported,
        });
    }
    Ok(imported)
}

/// Count all operations carried by authenticated import metadata.
fn count_range_operations(start: &loro::VersionVector, end: &loro::VersionVector) -> Result<usize> {
    for (peer, start_counter) in start.iter() {
        let end_counter = end.get(peer).copied().unwrap_or_default();
        if end_counter < *start_counter {
            return Err(CrdtError::ImportInvalidOperationRange);
        }
    }
    end.iter().try_fold(0usize, |total, (peer, end_counter)| {
        let start_counter = start.get(peer).copied().unwrap_or_default();
        let operations = end_counter
            .checked_sub(start_counter)
            .ok_or(CrdtError::ImportInvalidOperationRange)?;
        let operations =
            usize::try_from(operations).map_err(|_| CrdtError::ImportInvalidOperationRange)?;
        total
            .checked_add(operations)
            .ok_or(CrdtError::ImportInvalidOperationRange)
    })
}

/// Count exact newly contributed operations from authenticated import metadata.
fn count_new_operations(
    start: &loro::VersionVector,
    end: &loro::VersionVector,
    current: &loro::VersionVector,
) -> Result<usize> {
    for (peer, start_counter) in start.iter() {
        let end_counter = end.get(peer).copied().unwrap_or_default();
        if end_counter < *start_counter {
            return Err(CrdtError::ImportInvalidOperationRange);
        }
    }
    end.iter().try_fold(0usize, |total, (peer, end_counter)| {
        let start_counter = start.get(peer).copied().unwrap_or_default();
        if *end_counter < start_counter {
            return Err(CrdtError::ImportInvalidOperationRange);
        }
        let current_counter = current.get(peer).copied().unwrap_or_default();
        let new_operations = if current_counter >= *end_counter {
            0
        } else {
            let new_start = start_counter.max(current_counter);
            end_counter
                .checked_sub(new_start)
                .ok_or(CrdtError::ImportInvalidOperationRange)?
        };
        let new_operations =
            usize::try_from(new_operations).map_err(|_| CrdtError::ImportInvalidOperationRange)?;
        total
            .checked_add(new_operations)
            .ok_or(CrdtError::ImportInvalidOperationRange)
    })
}
