// SPDX-License-Identifier: Apache-2.0

//! The decided mutation set a governed KV write resolved to.
//!
//! A KV write whose stored image depends on the stored row — an atomic
//! increment, a CAS, a field merge, a transfer — cannot cross the Raft wire
//! carrying a live write predicate: a follower has no writing identity to
//! decide it against. The Control Plane resolves the write against the rows
//! the Data Plane holds, decides the policy there, and ships the mutations
//! themselves.

use nodedb_types::Surrogate;

/// One storage mutation a resolved KV write applies.
///
/// `collection` is per-mutation rather than hoisted onto the write:
/// `KvOp::TransferItem` moves a row between two collections in a single
/// resolved write.
///
/// ## Precondition
///
/// `precondition` is the drift check, NOT user-facing CAS business logic:
///
/// - `None` — the key must currently be ABSENT.
/// - `Some(bytes)` — the key must currently hold EXACTLY `bytes`, the raw
///   stored MessagePack, compared with `==`.
///
/// Deliberately not `KvOp::Cas`'s `expected` semantics, which match a typed
/// map's first string field as a fallback and read an empty `expected` as
/// "must be absent" — that conflates absent with present-and-empty, and it is
/// the user's swap condition rather than a check that the state the resolve
/// read is still the state the apply sees.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvResolvedMutation {
    /// Write `value` under `key`, replacing whatever is there.
    Put {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        /// TTL the originating op requested, carried for the durable record's
        /// shape only. `expire_at_ms` is what the apply installs.
        ttl_ms: u64,
        /// Absolute expiry instant (ms since epoch) to install verbatim, or
        /// `0` for none.
        ///
        /// Resolved once, on the resolving node, and applied identically on
        /// every replica. It is a separate field from `ttl_ms` because the two
        /// engine write paths a KV op can take disagree on what `ttl_ms == 0`
        /// means: `KvEngine::put` clears a key's TTL, while the atomic path's
        /// `atomic_put` preserves it. Deriving expiry from `ttl_ms` at apply
        /// time would therefore silently drop the TTL of every key an `INCR` /
        /// `CAS` / `GETSET` touches.
        expire_at_ms: u64,
        surrogate: Surrogate,
        precondition: Option<Vec<u8>>,
    },
    /// Remove `key`.
    Delete {
        collection: String,
        key: Vec<u8>,
        precondition: Option<Vec<u8>>,
    },
    /// Install a TTL on `key`, leaving its body untouched.
    Expire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
        /// The wall-clock instant the resolving node read. The apply installs
        /// `resolved_now_ms + ttl_ms`, so every replica reaches the same
        /// absolute expiry instead of reading its own clock.
        resolved_now_ms: u64,
        precondition: Option<Vec<u8>>,
    },
    /// Clear `key`'s TTL, leaving its body untouched.
    Persist {
        collection: String,
        key: Vec<u8>,
        precondition: Option<Vec<u8>>,
    },
}

impl KvResolvedMutation {
    /// The collection this one mutation targets.
    pub fn collection(&self) -> &str {
        match self {
            KvResolvedMutation::Put { collection, .. }
            | KvResolvedMutation::Delete { collection, .. }
            | KvResolvedMutation::Expire { collection, .. }
            | KvResolvedMutation::Persist { collection, .. } => collection.as_str(),
        }
    }

    /// The key this one mutation targets.
    pub fn key(&self) -> &[u8] {
        match self {
            KvResolvedMutation::Put { key, .. }
            | KvResolvedMutation::Delete { key, .. }
            | KvResolvedMutation::Expire { key, .. }
            | KvResolvedMutation::Persist { key, .. } => key.as_slice(),
        }
    }

    /// The state this mutation requires the key to be in before it applies.
    pub fn precondition(&self) -> Option<&[u8]> {
        match self {
            KvResolvedMutation::Put { precondition, .. }
            | KvResolvedMutation::Delete { precondition, .. }
            | KvResolvedMutation::Expire { precondition, .. }
            | KvResolvedMutation::Persist { precondition, .. } => precondition.as_deref(),
        }
    }
}

/// What `KvOp::ResolveWrite` reports back: every mutation the intercepted
/// write applies, plus the exact response payload the statement returns once
/// they all apply cleanly.
///
/// An empty `mutations` list is a legitimate outcome — a `CAS` whose expected
/// value did not match writes nothing and still owes the caller its
/// `{"success": false, ...}` reply.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct KvResolveOutcome {
    pub mutations: Vec<KvResolvedMutation>,
    pub response_payload: Vec<u8>,
}
