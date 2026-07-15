// SPDX-License-Identifier: BUSL-1.1

//! `RegistryReserveRead` / `RegistryReleaseReservation` — bridge the cluster
//! `ReserveRead` / `ReleaseReservation` triggers to a node-local reservation
//! submit/release.
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the reservation
//! primitives live in `crate::control::planner::calvin::reservation` and are
//! exposed to the transport via these hooks. The `RaftLoop` is built
//! `.with_reserve_read(Arc::new(RegistryReserveRead { .. }))` and
//! `.with_release_reservation(Arc::new(RegistryReleaseReservation { .. }))`.
//!
//! Both handlers only ever run on the SEQUENCER-GROUP leader (the coordinator
//! routes the requests there precisely so the submit lands where the
//! reservation service assigns/releases).
//!
//! # Plane discipline
//!
//! Runs on the leader's Control Plane (the Tokio transport reactor). The
//! submit blocks only on the assignment oneshot channel (reserve-read) or is
//! fire-and-forget (release); the actual admission bookkeeping happens on the
//! reservation service. This hook never touches storage I/O or io_uring
//! directly.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::calvin::types::{LockKeyWire, ReleaseReason, TxnIdWire};
use nodedb_cluster::{
    ReleaseReservationRequest, ReleaseReservationResponse, ReserveReadRequest, ReserveReadResponse,
    TypedClusterError,
};

use crate::control::planner::calvin::reservation::{
    submit_local_release, submit_local_reserve_read,
};
use crate::control::state::SharedState;

/// `nodedb`-side implementation of [`nodedb_cluster::ReserveRead`].
///
/// Holds the node's [`SharedState`] so it can reach the local reservation
/// inbox. The coordinator only routes here when this node is the
/// sequencer-group leader, so the local submit is the one that actually gets
/// assigned.
pub struct RegistryReserveRead {
    /// Shared node state — the source of the local reservation inbox.
    state: Arc<SharedState>,
}

impl RegistryReserveRead {
    /// Build a reserve-read hook over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::ReserveRead for RegistryReserveRead {
    async fn on_reserve_read(&self, req: ReserveReadRequest) -> ReserveReadResponse {
        let key: LockKeyWire = match zerompk::from_msgpack(&req.lock_key_bytes) {
            Ok(k) => k,
            Err(e) => {
                return ReserveReadResponse {
                    owner_bytes: None,
                    error: Some(TypedClusterError::Internal {
                        code: 0,
                        message: format!("reserve-read: failed to decode LockKeyWire: {e}"),
                    }),
                };
            }
        };
        let owner: Option<TxnIdWire> = match req.owner_bytes {
            Some(b) => match zerompk::from_msgpack(&b) {
                Ok(o) => Some(o),
                Err(e) => {
                    return ReserveReadResponse {
                        owner_bytes: None,
                        error: Some(TypedClusterError::Internal {
                            code: 0,
                            message: format!("reserve-read: failed to decode TxnIdWire owner: {e}"),
                        }),
                    };
                }
            },
            None => None,
        };

        let timeout = Duration::from_millis(req.deadline_remaining_ms.max(1));
        match submit_local_reserve_read(&self.state, key, req.vshard, owner, timeout).await {
            Ok(assigned) => match zerompk::to_msgpack_vec(&assigned) {
                Ok(bytes) => ReserveReadResponse {
                    owner_bytes: Some(bytes),
                    error: None,
                },
                Err(e) => ReserveReadResponse {
                    owner_bytes: None,
                    error: Some(TypedClusterError::Internal {
                        code: 0,
                        message: format!("reserve-read: failed to encode assigned owner: {e}"),
                    }),
                },
            },
            Err(e) => ReserveReadResponse {
                owner_bytes: None,
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: format!("reserve-read local submit failed: {e}"),
                }),
            },
        }
    }
}

/// `nodedb`-side implementation of [`nodedb_cluster::ReleaseReservation`].
///
/// Holds the node's [`SharedState`] so it can reach the local reservation
/// inbox. The coordinator only routes here when this node is the
/// sequencer-group leader.
pub struct RegistryReleaseReservation {
    /// Shared node state — the source of the local reservation inbox.
    state: Arc<SharedState>,
}

impl RegistryReleaseReservation {
    /// Build a release-reservation hook over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::ReleaseReservation for RegistryReleaseReservation {
    async fn on_release_reservation(
        &self,
        req: ReleaseReservationRequest,
    ) -> ReleaseReservationResponse {
        let owner: TxnIdWire = match zerompk::from_msgpack(&req.owner_bytes) {
            Ok(o) => o,
            Err(e) => {
                return ReleaseReservationResponse {
                    error: Some(TypedClusterError::Internal {
                        code: 0,
                        message: format!(
                            "release-reservation: failed to decode TxnIdWire owner: {e}"
                        ),
                    }),
                };
            }
        };
        let reason: ReleaseReason = match zerompk::from_msgpack(&req.reason_bytes) {
            Ok(r) => r,
            Err(e) => {
                return ReleaseReservationResponse {
                    error: Some(TypedClusterError::Internal {
                        code: 0,
                        message: format!(
                            "release-reservation: failed to decode ReleaseReason: {e}"
                        ),
                    }),
                };
            }
        };

        match submit_local_release(&self.state, owner, req.vshard, reason).await {
            Ok(()) => ReleaseReservationResponse { error: None },
            Err(e) => ReleaseReservationResponse {
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: format!("release-reservation local submit failed: {e}"),
                }),
            },
        }
    }
}
