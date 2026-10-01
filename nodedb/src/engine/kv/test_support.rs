// SPDX-License-Identifier: BUSL-1.1

//! Bound identities for KV rows a unit test writes.

use nodedb_types::Surrogate;

/// A bound surrogate for a KV test row, derived from its key (FNV-1a).
///
/// The same key always maps to the same value, and distinct keys map to
/// distinct values with overwhelming likelihood. The value is never
/// `Surrogate::ZERO`, which the engine refuses.
pub(crate) fn row_surrogate(key: &[u8]) -> Surrogate {
    const FNV_OFFSET: u32 = 0x811c_9dc5;
    const FNV_PRIME: u32 = 0x0100_0193;
    let hash = key.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u32::from(*byte)).wrapping_mul(FNV_PRIME)
    });
    Surrogate::new(hash.max(1))
}
