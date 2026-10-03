// SPDX-License-Identifier: BUSL-1.1

//! Parameters for [`super::engine::KvEngine::batch_put`].

use nodedb_types::Surrogate;

/// Parameters for [`super::engine::KvEngine::batch_put`].
///
/// Bundles all per-batch inputs so the method stays argument-count clean.
///
/// `surrogates` carries each entry's stable cross-engine identity, same
/// order and length as `entries` -- assigned by the CP-side
/// `SurrogateAssigner` from `(collection, key)`, same mechanism as a
/// single-key `put`. Never `Surrogate::ZERO`: a batch that carries it is
/// refused before any entry is written.
pub struct KvBatchPutParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub entries: &'a [(Vec<u8>, Vec<u8>)],
    pub ttl_ms: u64,
    pub now_ms: u64,
    pub surrogates: &'a [Surrogate],
}
