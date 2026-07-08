// SPDX-License-Identifier: BUSL-1.1

//! `RegistryCalvinSubmit` — bridges the cluster `SubmitCalvinTxn` trigger to a
//! node-local Calvin submit-and-await (Cv1).
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the submit logic
//! lives here and is exposed to the transport via the
//! [`nodedb_cluster::CalvinSubmit`] hook. The `RaftLoop` is built
//! `with_calvin_submit(Arc::new(RegistryCalvinSubmit { .. }))`.
//!
//! # What it does
//!
//! This handler only ever runs on the SEQUENCER-GROUP leader (the coordinator
//! routes the `SubmitCalvinTxnRequest` there precisely so the submit lands where
//! the sequencer service assigns and the local registry receives the replicated
//! completion ack). On `on_submit_calvin_txn`, the leader:
//! 1. decodes `tx_class_bytes` (msgpack) into a `TxClass` and re-derives its
//!    cached participating-vshard set via `restore_derived`;
//! 2. submits it to this node's Calvin sequencer inbox and awaits assignment +
//!    completion through `submit_and_await_calvin_with_timeout`, bounded by the
//!    forwarded `deadline_remaining_ms` so it cannot outlive the coordinator's
//!    remaining budget;
//! 3. maps `Ok(())` → `error: None` and `Err` → a typed
//!    [`TypedClusterError::Internal`] — never a silent drop.
//!
//! # Plane discipline
//!
//! This runs on the leader's Control Plane (the Tokio transport reactor). The
//! submit-and-await blocks only on the assignment / completion oneshot channels;
//! the transaction execution itself happens on the Data Plane via the sequencer
//! service / per-vshard schedulers. This hook never touches storage I/O or
//! io_uring directly.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::{SubmitCalvinTxnRequest, SubmitCalvinTxnResponse, TypedClusterError};

use crate::control::planner::calvin::submit_and_await_calvin_with_timeout;
use crate::control::state::SharedState;

/// `nodedb`-side implementation of [`nodedb_cluster::CalvinSubmit`].
///
/// Holds the node's [`SharedState`] so it can reach the Calvin sequencer inbox
/// and completion registry. The coordinator only routes here when this node is
/// the sequencer-group leader, so the submit-and-await is the one that actually
/// completes.
pub struct RegistryCalvinSubmit {
    /// Shared node state — the source of the local sequencer inbox + registry.
    state: Arc<SharedState>,
}

impl RegistryCalvinSubmit {
    /// Build a Calvin-submit hook over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::CalvinSubmit for RegistryCalvinSubmit {
    async fn on_submit_calvin_txn(&self, req: SubmitCalvinTxnRequest) -> SubmitCalvinTxnResponse {
        let mut tx_class: TxClass = match zerompk::from_msgpack(&req.tx_class_bytes) {
            Ok(tc) => tc,
            Err(e) => {
                return SubmitCalvinTxnResponse {
                    error: Some(TypedClusterError::Internal {
                        code: 0,
                        message: format!("calvin-submit: failed to decode TxClass: {e}"),
                    }),
                    payload_bytes: None,
                };
            }
        };
        // Re-derive the participating-vshard set skipped during serialization
        // (the wire bytes carry only the read/write sets).
        tx_class.restore_derived();

        let timeout = Duration::from_millis(req.deadline_remaining_ms.max(1));
        match submit_and_await_calvin_with_timeout(&self.state, tx_class, timeout).await {
            // Forward the applied RETURNING payload (drained from this leader's
            // local sidecar) back to the remote coordinator over the non-Raft
            // RPC response; `None` for a plain write with no rows to surface.
            Ok(applied) => SubmitCalvinTxnResponse {
                error: None,
                payload_bytes: applied.map(|r| r.payload.to_vec()),
            },
            Err(e) => SubmitCalvinTxnResponse {
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: format!("calvin-submit local submit-and-await failed: {e}"),
                }),
                payload_bytes: None,
            },
        }
    }
}
