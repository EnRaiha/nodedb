// SPDX-License-Identifier: BUSL-1.1

//! Shared state reads and mutation constructors for resolving a governed KV
//! write.
//!
//! Every resolver in this module reads state and computes images; not one of
//! them writes. The mutation each produces carries a `precondition` recording
//! exactly what it read, so the apply can refuse a resolution that state has
//! moved past.

use nodedb_physical::physical_plan::{KvResolveOutcome, KvResolvedMutation};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;

/// What a resolver returns: the decided mutations and the decided reply, or
/// the error the live handler would have returned for the same input.
pub(super) type ResolveResult = Result<KvResolveOutcome, ErrorCode>;

impl CoreLoop {
    /// The stored body of `key`, or `None` when it is absent or expired.
    ///
    /// This value becomes the mutation's `precondition`: `Some(bytes)` means
    /// the apply requires exactly these bytes, `None` that it requires the key
    /// to still be absent.
    pub(super) fn kv_resolve_read(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        self.kv_engine.get(did, tid, collection, key, now_ms)
    }

    /// The absolute expiry instant `key` currently carries, or `0` for none.
    ///
    /// The atomic write path (`atomic_put`) preserves an existing TTL when the
    /// op requests none, while `KvEngine::put` clears it. A resolved `Put`
    /// installs an absolute instant, so the preserved one has to be read here
    /// rather than inferred from a `ttl_ms` of zero at apply time.
    pub(super) fn kv_resolve_preserved_expiry(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
    ) -> u64 {
        match self.kv_engine.get_ttl_meta(did, tid, collection, key) {
            Some(meta) if meta.has_ttl => meta.expire_at_ms,
            _ => 0,
        }
    }
}

/// Bundled inputs for [`put_mutation`] — a plain positional list of seven
/// exceeds clippy's arity threshold.
pub(super) struct ResolvedPut<'a> {
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: Vec<u8>,
    pub ttl_ms: u64,
    pub expire_at_ms: u64,
    pub surrogate: nodedb_types::Surrogate,
    pub precondition: Option<Vec<u8>>,
}

/// Build the `Put` mutation a resolved write applies.
pub(super) fn put_mutation(put: ResolvedPut<'_>) -> KvResolvedMutation {
    KvResolvedMutation::Put {
        collection: put.collection.to_owned(),
        key: put.key.to_vec(),
        value: put.value,
        ttl_ms: put.ttl_ms,
        expire_at_ms: put.expire_at_ms,
        surrogate: put.surrogate,
        precondition: put.precondition,
    }
}

/// Build the `Delete` mutation a resolved write applies.
pub(super) fn delete_mutation(
    collection: &str,
    key: &[u8],
    precondition: Option<Vec<u8>>,
) -> KvResolvedMutation {
    KvResolvedMutation::Delete {
        collection: collection.to_owned(),
        key: key.to_vec(),
        precondition,
    }
}

/// Absolute expiry for a write that installs `ttl_ms` from `now_ms`, matching
/// `KvEngine::put`: a zero TTL means no expiry at all, not "expire now".
pub(super) fn expiry_from_ttl(ttl_ms: u64, now_ms: u64) -> u64 {
    if ttl_ms == 0 { 0 } else { now_ms + ttl_ms }
}

/// The outcome of a resolver that decided exactly one mutation.
pub(super) fn one(mutation: KvResolvedMutation, response_payload: Vec<u8>) -> KvResolveOutcome {
    KvResolveOutcome {
        mutations: vec![mutation],
        response_payload,
    }
}
