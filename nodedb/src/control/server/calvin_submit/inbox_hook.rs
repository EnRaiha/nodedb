// SPDX-License-Identifier: BUSL-1.1

//! `RegistryCalvinSubmitInbox` — bridges the cluster `SubmitCalvinInbox` trigger
//! to a node-local Calvin submit-and-ASSIGN (Cv1).
//!
//! OLLP dependent sibling of [`RegistryCalvinSubmit`](super::hook::RegistryCalvinSubmit):
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the submit logic
//! lives here and is exposed to the transport via the
//! [`nodedb_cluster::CalvinSubmitInbox`] hook. The `RaftLoop` is built
//! `with_calvin_submit_inbox(Arc::new(RegistryCalvinSubmitInbox { .. }))`.
//!
//! # What it does
//!
//! This handler only ever runs on the SEQUENCER-GROUP leader (the coordinator
//! routes the `SubmitCalvinInboxRequest` there precisely so the submit lands
//! where the sequencer service assigns). On `on_submit_calvin_inbox`, the leader:
//! 1. decodes `tx_class_bytes` (msgpack) into a `TxClass` and re-derives its
//!    cached participating-vshard set via `restore_derived`;
//! 2. submits it to this node's Calvin sequencer inbox and awaits only the
//!    ASSIGNMENT (NOT completion) through the local `CalvinCompletionRegistry`,
//!    bounded by the forwarded `deadline_remaining_ms` so it cannot outlive the
//!    coordinator's remaining budget;
//! 3. maps `Ok(RoutedAssignment { .. })` → a success response carrying
//!    the assignment and `Err` → a typed [`TypedClusterError::Internal`] — never
//!    a silent drop.
//!
//! It must NOT await completion: the OLLP coordinator loop drives the dependent
//! transaction to completion itself in a later unit; this hook returns as soon as
//! the assignment is observed.
//!
//! # Plane discipline
//!
//! This runs on the leader's Control Plane (the Tokio transport reactor). The
//! submit-and-assign blocks only on the assignment oneshot channel; the
//! transaction execution itself happens on the Data Plane via the sequencer
//! service / per-vshard schedulers. This hook never touches storage I/O or
//! io_uring directly.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::{SubmitCalvinInboxRequest, SubmitCalvinInboxResponse, TypedClusterError};

use crate::control::planner::calvin::submit::submit_local_assign;
use crate::control::state::SharedState;

/// `nodedb`-side implementation of [`nodedb_cluster::CalvinSubmitInbox`].
///
/// Holds the node's [`SharedState`] so it can reach the Calvin sequencer inbox
/// and completion registry. The coordinator only routes here when this node is
/// the sequencer-group leader, so the submit-and-assign is the one that actually
/// gets sequenced.
pub struct RegistryCalvinSubmitInbox {
    /// Shared node state — the source of the local sequencer inbox + registry.
    state: Arc<SharedState>,
}

impl RegistryCalvinSubmitInbox {
    /// Build a Calvin-inbox hook over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::CalvinSubmitInbox for RegistryCalvinSubmitInbox {
    async fn on_submit_calvin_inbox(
        &self,
        req: SubmitCalvinInboxRequest,
    ) -> SubmitCalvinInboxResponse {
        let mut tx_class: TxClass = match zerompk::from_msgpack(&req.tx_class_bytes) {
            Ok(tc) => tc,
            Err(e) => {
                return SubmitCalvinInboxResponse {
                    inbox_seq: 0,
                    epoch: 0,
                    position: 0,
                    participants: 0,
                    error: Some(TypedClusterError::Internal {
                        code: 0,
                        message: format!("calvin-inbox: failed to decode TxClass: {e}"),
                    }),
                };
            }
        };
        // Re-derive the participating-vshard set skipped during serialization
        // (the wire bytes carry only the read/write sets).
        tx_class.restore_derived();

        let timeout = Duration::from_millis(req.deadline_remaining_ms.max(1));
        match submit_local_assign(&self.state, tx_class, timeout).await {
            Ok(a) => SubmitCalvinInboxResponse {
                inbox_seq: a.inbox_seq,
                epoch: a.epoch,
                position: a.position,
                participants: a.participants as u64,
                error: None,
            },
            Err(e) => SubmitCalvinInboxResponse {
                inbox_seq: 0,
                epoch: 0,
                position: 0,
                participants: 0,
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: format!("calvin-inbox local submit-and-assign failed: {e}"),
                }),
            },
        }
    }
}
