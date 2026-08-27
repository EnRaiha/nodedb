// SPDX-License-Identifier: BUSL-1.1

//! Submit-and-assign for the dependent (OLLP) Calvin path.
//!
//! The sibling of [`super::routed`] that stops at the sequencer ASSIGNMENT:
//! the OLLP coordinator loop drives the transaction to completion itself.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::{RaftRpc, SubmitCalvinInboxRequest, SubmitCalvinInboxResponse};

use crate::Error;
use crate::control::server::exchange::resolve::register_peers_from_topology;
use crate::control::state::SharedState;

/// The sequencer ASSIGNMENT for a submitted dependent (OLLP) `TxClass`.
///
/// Returned by [`submit_calvin_routed_assign`] AS SOON AS the sequencer assigns
/// the transaction — completion is NOT awaited (the OLLP coordinator loop drives
/// the dependent transaction to completion itself in a later unit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutedAssignment {
    pub inbox_seq: u64,
    pub epoch: u64,
    pub position: u32,
    pub participants: usize,
}

/// Submit `tx_class` to THIS node's Calvin sequencer inbox and await only its
/// ASSIGNMENT (NOT completion), bounded by `timeout`.
///
/// PRECONDITION: this node is the sequencer-group leader (its service assigns).
/// Callers that are not the leader MUST route via
/// [`submit_calvin_routed_assign`]. The local primitive for the OLLP dependent
/// path — the sibling of [`super::local::submit_and_await_calvin_with_timeout`] that stops at
/// the assignment phase.
///
/// `pub(crate)` so [`crate::control::server::calvin_submit::inbox_hook`] can
/// call it after decoding the wire bytes, mirroring how `hook.rs` delegates to
/// `submit_and_await_calvin_with_timeout`.
pub(crate) async fn submit_local_assign(
    state: &SharedState,
    tx_class: TxClass,
    timeout: Duration,
) -> crate::Result<RoutedAssignment> {
    let inbox = state
        .sequencer_inbox
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let registry = state
        .calvin_completion_registry
        .get()
        .ok_or(Error::SequencerUnavailable)?;

    let inbox_seq = inbox.submit(tx_class).map_err(|e| Error::BadRequest {
        detail: format!("Calvin sequencer rejected transaction: {e}"),
    })?;

    let assignment_rx = registry.register_submission(inbox_seq);
    let (epoch, position, participants) = tokio::time::timeout(timeout, assignment_rx)
        .await
        .map_err(|_| Error::Internal {
            detail: "timed out waiting for Calvin sequencer assignment".to_owned(),
        })?
        .map_err(|_| Error::Internal {
            detail: "Calvin sequencer assignment channel closed".to_owned(),
        })?;

    Ok(RoutedAssignment {
        inbox_seq,
        epoch,
        position,
        participants,
    })
}

/// Submit a cross-shard dependent (OLLP) Calvin `tx_class`, routing it to the
/// sequencer-group leader, and return its ASSIGNMENT immediately — WITHOUT
/// awaiting completion.
///
/// The OLLP dependent sibling of [`super::routed::submit_calvin_routed`]. Routing logic mirrors
/// it exactly:
/// - **Not cluster mode** (no `cluster_transport` / `cluster_routing`) OR
///   **leader is self**: submit-and-assign locally — single-node / this node IS
///   the sequencer leader.
/// - **No leader elected (0 / none)**: return a typed error — never submit
///   locally, since a non-leader submit is silently discarded.
/// - **Leader is a remote node**: register the leader's address from the live
///   topology, then send one `SubmitCalvinInboxRequest` (carrying the
///   msgpack-encoded `TxClass`); the leader runs the submit-and-assign and
///   replies with the assignment. Map transport / leader errors to a typed
///   `crate::Error`.
pub async fn submit_calvin_routed_assign(
    state: &SharedState,
    tx_class: TxClass,
) -> crate::Result<RoutedAssignment> {
    let local_timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);

    // Not cluster mode — single-node is the only sequencer member, hence the
    // leader. Submit-and-assign locally.
    let (Some(transport), Some(_routing)) = (
        state.cluster_transport.as_ref(),
        state.cluster_routing.as_ref(),
    ) else {
        return submit_local_assign(state, tx_class, local_timeout).await;
    };

    // Resolve the sequencer-group leader from THIS node's live Raft status.
    let status_fn = state.raft_status_fn.get().ok_or_else(|| Error::Internal {
        detail: "calvin-inbox: raft status fn not installed (cluster not started)".to_owned(),
    })?;
    let leader = status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0);

    // `0` = no sequencer leader elected yet. We must NOT submit locally: a
    // non-leader submit is drained and discarded by the local sequencer service.
    if leader == 0 {
        return Err(Error::Internal {
            detail: "calvin-inbox: no sequencer leader elected yet; cannot submit cross-shard \
                     transaction"
                .to_owned(),
        });
    }

    // Leader is self: submit-and-assign locally (a self-RPC would be a pointless
    // extra hop and the local registry is the one that gets the assignment).
    if leader == state.node_id {
        return submit_local_assign(state, tx_class, local_timeout).await;
    }

    // Remote leader: ensure its address is registered before dispatch, then send
    // the one-shot RPC carrying the msgpack-encoded TxClass.
    let mut targets = BTreeSet::new();
    targets.insert(leader);
    register_peers_from_topology(state, transport, &targets);

    let tx_class_bytes = zerompk::to_msgpack_vec(&tx_class).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode TxClass for routed Calvin inbox submit: {e}"),
    })?;

    let deadline_remaining_ms = state
        .tuning
        .network
        .default_deadline_secs
        .saturating_mul(1000)
        .max(1);
    let req = SubmitCalvinInboxRequest {
        tx_class_bytes,
        deadline_remaining_ms,
        trace_id: [0u8; 16],
    };

    // The leader-side handler holds this RPC open until the transaction is
    // assigned (up to `deadline_remaining_ms`). The generic short `rpc_timeout`
    // would abort the call long before that, so bound the response read by the
    // forwarded deadline plus a margin for the round-trip itself.
    let read_timeout = Duration::from_millis(deadline_remaining_ms.saturating_add(2_000));
    match transport
        .send_rpc_with_read_timeout(leader, RaftRpc::SubmitCalvinInboxRequest(req), read_timeout)
        .await
    {
        Ok(RaftRpc::SubmitCalvinInboxResponse(SubmitCalvinInboxResponse {
            inbox_seq,
            epoch,
            position,
            participants,
            error: None,
        })) => Ok(RoutedAssignment {
            inbox_seq,
            epoch,
            position,
            participants: participants as usize,
        }),
        Ok(RaftRpc::SubmitCalvinInboxResponse(SubmitCalvinInboxResponse {
            error: Some(e),
            ..
        })) => Err(Error::Internal {
            detail: format!("calvin-inbox failed on sequencer leader node {leader}: {e:?}"),
        }),
        Ok(other) => Err(Error::Internal {
            detail: format!("calvin-inbox: unexpected reply from node {leader}: {other:?}"),
        }),
        Err(e) => Err(Error::Internal {
            detail: format!("calvin-inbox RPC to sequencer leader node {leader} failed: {e}"),
        }),
    }
}
