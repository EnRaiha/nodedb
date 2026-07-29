// SPDX-License-Identifier: BUSL-1.1

//! Inputs to a CRDT delta apply, and the names refusals are reported under.

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;

/// Rejection name for a delta that wrote rows other than its frame target.
pub(crate) const CRDT_SINGLE_DOCUMENT_DELTA: &str = "crdt_single_document_delta";

/// Refusal name for a delta whose causal predecessors are absent from the
/// target collection's document, so nothing was applied.
///
/// Retryable: the identical bytes apply cleanly once the missing history
/// arrives, which is why callers that own a retry channel must not turn this
/// into a terminal rejection.
pub(crate) const CRDT_PENDING_DEPENDENCIES: &str = "crdt_pending_dependencies";

/// Parameters for
/// [`execute_crdt_apply`](crate::data::executor::core_loop::CoreLoop::execute_crdt_apply).
pub(in crate::data::executor) struct CrdtApplyParams<'a> {
    pub collection: &'a str,
    pub document_id: &'a str,
    pub delta: &'a [u8],
    pub surrogate: Surrogate,
    pub peer_id: u64,
    pub provenance: Option<&'a SyncProvenance>,
    pub constraint_version_required: u64,
    pub expected_frontier_digest: Option<[u8; 32]>,
    pub auth_user_id: u64,
    pub auth_device_id: u64,
    pub auth_seq_no: u64,
    pub delta_signature: [u8; 32],
    pub signing_required: bool,
}
