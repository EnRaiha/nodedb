// SPDX-License-Identifier: Apache-2.0

//! The decided mutation set a governed document write resolved to.
//!
//! A document `UPDATE` / `DELETE` / `UPSERT` on a collection carrying an RLS
//! write policy cannot cross the Raft wire carrying the live predicate: a
//! follower has no writing identity to decide it against, and the leader
//! re-derives its plan from the committed entry, so the predicate would be
//! judged against whatever policy catalog the applying node holds. The Control
//! Plane resolves the write against the rows the Data Plane holds, decides the
//! policy there, and ships the mutations themselves.
//!
//! ## Body form — one form per field, both sides agree
//!
//! - `value` is the PRE-ENCODE MessagePack body, the same form
//!   [`DocumentOp::PointPut::value`](super::op::DocumentOp::PointPut) carries
//!   and the only form the document write path takes for BOTH storage modes.
//!   A strict collection's Binary Tuple is encoded on the way to disk, by the
//!   write path, from this body. Never JSON, never `serde_json::Value` on the
//!   wire.
//! - `precondition` is RAW STORED bytes — a Binary Tuple for `document_strict`,
//!   MessagePack for schemaless — because it is compared byte-for-byte with
//!   what storage currently holds.
//!
//! The two differ deliberately, and each is fixed: shipping a stored-form body
//! into the write path re-encodes a Binary Tuple as if it were MessagePack and
//! fails; comparing a pre-encode body against stored bytes never matches.

use nodedb_types::Surrogate;

use super::sum_target::ResolvedSumTarget;

/// One row mutation a resolved document write applies.
///
/// ## Precondition
///
/// `precondition` is the drift check between resolve and apply, NOT user-facing
/// business logic:
///
/// - `None` — the row must currently be ABSENT.
/// - `Some(bytes)` — the row must currently hold EXACTLY `bytes`, the raw
///   stored body, compared with `==`.
///
/// A surrogate-existence check would not do: it proves only that the row was
/// not deleted, and is blind to a concurrent write that changed the row's
/// CONTENT between resolve and apply. That is the lost update the policy
/// decision was made to prevent.
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
        /// `(target collection, join value)` → target row surrogate for this
        /// collection's materialized-sum bindings, resolved on the Control
        /// Plane. The Data Plane cannot derive it: the PK→surrogate map lives
        /// in the catalog redb, which is Control-Plane state.
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
