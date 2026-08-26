// SPDX-License-Identifier: Apache-2.0

//! The decided mutation set a governed document write resolved to.
//!
//! An RLS-governed write can't cross Raft carrying the live predicate — a
//! follower has no writing identity to judge it against. The Control Plane
//! resolves the write and policy against the rows the Data Plane holds, then
//! ships the mutations themselves. `value` is always pre-encode MessagePack;
//! `precondition` is always raw stored bytes (Binary Tuple or MessagePack).
//! Swapping the two forms fails silently or never matches — never mix them.

use nodedb_types::Surrogate;

use super::sum_target::ResolvedSumTarget;

/// One row mutation a resolved document write applies.
///
/// `precondition` is the resolve→apply drift check, not business logic:
/// `None` requires the row absent, `Some(bytes)` requires it hold exactly
/// `bytes` (`==`). A surrogate-existence check would miss a concurrent write
/// that changed content without deleting the row — the lost update this
/// precondition prevents.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DocumentResolvedMutation {
    /// Store `value` for `document_id`, replacing whatever is there.
    Put {
        collection: String,
        /// User-facing primary key, as `RETURNING` and CDC name the row.
        document_id: String,
        /// Catalog-bound identity; the handler hex-encodes it for the row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes, for follower-side WAL decode rebind.
        pk_bytes: Vec<u8>,
        /// Pre-encode MessagePack body — see this module's doc.
        value: Vec<u8>,
        /// Raw stored bytes the resolve read, or `None` for an absent row.
        precondition: Option<Vec<u8>>,
        /// `(target collection, join value)` → target row surrogate,
        /// resolved by the Control Plane (Data Plane has no PK→surrogate map).
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },
    /// Remove `document_id`.
    Delete {
        collection: String,
        document_id: String,
        surrogate: Surrogate,
        pk_bytes: Vec<u8>,
        /// Raw stored pre-image the resolve read and decided the policy against.
        precondition: Option<Vec<u8>>,
        /// See [`DocumentResolvedMutation::Put::resolved_sum_targets`].
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },
}

impl DocumentResolvedMutation {
    /// The collection this one mutation targets.
    pub fn collection(&self) -> &str {
        match self {
            DocumentResolvedMutation::Put { collection, .. }
            | DocumentResolvedMutation::Delete { collection, .. } => collection.as_str(),
        }
    }

    /// The user-facing primary key this one mutation targets.
    pub fn document_id(&self) -> &str {
        match self {
            DocumentResolvedMutation::Put { document_id, .. }
            | DocumentResolvedMutation::Delete { document_id, .. } => document_id.as_str(),
        }
    }

    /// The row identity this one mutation targets.
    pub fn surrogate(&self) -> Surrogate {
        match self {
            DocumentResolvedMutation::Put { surrogate, .. }
            | DocumentResolvedMutation::Delete { surrogate, .. } => *surrogate,
        }
    }

    /// The state this mutation requires the row to be in before it applies.
    pub fn precondition(&self) -> Option<&[u8]> {
        match self {
            DocumentResolvedMutation::Put { precondition, .. }
            | DocumentResolvedMutation::Delete { precondition, .. } => precondition.as_deref(),
        }
    }
}

/// What `DocumentOp::ResolveWrite` reports back for a point/bulk write: every
/// mutation the intercepted write applies, plus the exact response payload the
/// statement returns once they all apply cleanly.
///
/// An empty `mutations` list is a legitimate outcome — a `PointUpdate` whose
/// row is already gone writes nothing and still owes the caller its
/// `{"affected": 0}` reply.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DocumentResolveOutcome {
    pub mutations: Vec<DocumentResolvedMutation>,
    pub response_payload: Vec<u8>,
}
