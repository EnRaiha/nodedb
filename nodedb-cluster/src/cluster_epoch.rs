// SPDX-License-Identifier: BUSL-1.1

//! Cluster generation/epoch — a monotonic, leader-bumped fence token
//! stamped on every Raft RPC frame.
//!
//! # Purpose
//!
//! `cluster_epoch` is a process-wide `u64` that advances on every
//! cluster-membership-shifting event the metadata-group leader observes
//! (currently: becoming leader of the metadata group). Stamping it on every
//! outbound RPC and observing it on every inbound RPC lets every peer keep
//! a cluster-wide high-water mark and reject (or quarantine) frames from
//! peers stuck on a strictly older epoch — i.e. peers that missed a topology
//! transition and may be acting on stale state.
//!
//! # Mechanics
//!
//! * A process-global `AtomicU64`, [`LOCAL_CLUSTER_EPOCH`], holds the local
//!   high-water mark. On startup it is loaded from the cluster catalog
//!   (see [`ClusterCatalog::load_cluster_epoch`]).
//! * [`current_local_cluster_epoch`] reads it for the encoder.
//! * [`observe_peer_cluster_epoch`] is called by the decoder for every
//!   inbound frame; it bumps the local mark via `fetch_max` (monotonic).
//! * [`bump_local_cluster_epoch`] is called by the metadata-group leader
//!   when leadership transitions to it; it advances the local mark and
//!   persists the new value.
//!
//! Persistence is best-effort: a bump is committed in-memory atomically;
//! if the catalog write fails, the in-memory value is still advanced and
//! the failure is logged. (After a crash, the persisted value is a lower
//! bound — the new leader will re-bump beyond it.)
//!
//! # Why a global atomic
//!
//! Every encode/decode call site needs the current epoch. Threading it
//! through the existing 19 encode/decode functions and their callers
//! would touch hundreds of sites for what is, semantically, a single
//! per-process value. An `AtomicU64` is the right shape for this kind
//! of read-mostly, monotonic counter.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::ClusterCatalog;
use crate::error::{ClusterError, Result};
use crate::rpc_codec::discriminants::{RPC_JOIN_REQ, RPC_JOIN_RESP, RPC_PING, RPC_PONG};

/// Process-global cluster epoch high-water mark.
///
/// Initialized to 0 (genesis). Loaded from catalog at startup via
/// [`init_local_cluster_epoch_from_catalog`].
static LOCAL_CLUSTER_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Read the current local cluster epoch (the value an outbound RPC
/// should stamp). Cheap; lock-free.
pub fn current_local_cluster_epoch() -> u64 {
    LOCAL_CLUSTER_EPOCH.load(Ordering::Acquire)
}

/// Set the local epoch directly. Only intended for use by
/// [`init_local_cluster_epoch_from_catalog`] at startup and by tests.
/// Production code paths use [`observe_peer_cluster_epoch`] (monotonic
/// max) or [`bump_local_cluster_epoch`] (leader-side increment).
pub fn set_local_cluster_epoch(value: u64) {
    LOCAL_CLUSTER_EPOCH.store(value, Ordering::Release);
}

/// Observe an epoch carried by an inbound RPC. Advances the local mark
/// via `fetch_max` (so concurrent observations are safe and monotonic).
///
/// Returns the new local high-water mark.
pub fn observe_peer_cluster_epoch(peer_epoch: u64) -> u64 {
    let prev = LOCAL_CLUSTER_EPOCH.fetch_max(peer_epoch, Ordering::AcqRel);
    prev.max(peer_epoch)
}

/// RPC types exempt from the cluster-epoch fence.
///
/// * Join handshake (`RPC_JOIN_REQ` / `RPC_JOIN_RESP`): a joining or rejoining
///   node legitimately carries a zero or stale epoch — the join response is the
///   mechanism by which it learns (observes) the cluster's current epoch.
/// * Ping/pong (`RPC_PING` / `RPC_PONG`): the pre-join bootstrap probe
///   (`bootstrap/probe.rs`) pings the elected bootstrapper before joining, and
///   ping is the side-effect-free liveness channel a fenced peer needs in order
///   to be discovered and told to rejoin.
///
/// Everything else (raft consensus, topology, execute, shuffle, calvin,
/// surrogate, data/metadata propose, vshard envelopes) is fenced.
pub(crate) const EPOCH_EXEMPT_RPC_TYPES: &[u8] =
    &[RPC_JOIN_REQ, RPC_JOIN_RESP, RPC_PING, RPC_PONG];

/// Enforce the cluster-epoch fence on one inbound frame.
///
/// Rejects `peer_epoch < local` unless the RPC type is exempt. Called from
/// the decode path ([`crate::rpc_codec::header::parse_frame`]) for every
/// inbound rpc_codec frame, in both directions (server-side requests and
/// client-side responses).
///
/// `peer_epoch == 0` is *not* special-cased: against a local epoch of 0
/// (genesis / pre-init startup) it passes; against a local epoch > 0 it is
/// rejected exactly like any other stale stamp. The epoch check runs on
/// MAC-authenticated header bytes (the envelope MAC is verified before
/// decode in `transport/server.rs` and `transport/client/send.rs`), so a
/// spoofed stamp cannot trigger spurious rejections.
pub fn validate_peer_cluster_epoch(rpc_type: u8, peer_epoch: u64) -> Result<()> {
    if EPOCH_EXEMPT_RPC_TYPES.contains(&rpc_type) {
        return Ok(());
    }
    let local_epoch = LOCAL_CLUSTER_EPOCH.load(Ordering::Acquire);
    if peer_epoch < local_epoch {
        return Err(ClusterError::StalePeerEpoch {
            peer_epoch,
            local_epoch,
        });
    }
    Ok(())
}

/// Increment the local epoch by 1 and persist the new value to the
/// cluster catalog. Called by the metadata-group leader on a leadership
/// transition.
///
/// Returns the new epoch. The persistence failure path advances the
/// in-memory value anyway (so RPCs immediately reflect the bump) and
/// returns the persistence error to the caller.
pub fn bump_local_cluster_epoch(catalog: &ClusterCatalog) -> Result<u64> {
    let new_epoch = LOCAL_CLUSTER_EPOCH.fetch_add(1, Ordering::AcqRel) + 1;
    catalog.save_cluster_epoch(new_epoch)?;
    Ok(new_epoch)
}

/// Initialize the local epoch from the cluster catalog at process
/// startup. Idempotent — safe to call multiple times during boot.
pub fn init_local_cluster_epoch_from_catalog(catalog: &Arc<ClusterCatalog>) -> Result<u64> {
    let persisted = catalog.load_cluster_epoch()?.unwrap_or(0);
    // fetch_max so we don't regress past anything already observed
    // earlier in startup (e.g. an inbound frame on the join path).
    let prev = LOCAL_CLUSTER_EPOCH.fetch_max(persisted, Ordering::AcqRel);
    Ok(prev.max(persisted))
}

/// Serialises every test that mutates the shared `LOCAL_CLUSTER_EPOCH`
/// global — including the header decode-path tests in `rpc_codec::header`,
/// which set the epoch from outside this module. `pub(crate)` so both test
/// suites take the SAME lock (two locks would not exclude each other).
#[cfg(test)]
pub(crate) static EPOCH_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    fn reset() -> std::sync::MutexGuard<'static, ()> {
        // The shared crate-wide epoch test lock also serialises against
        // the decode-path tests in `rpc_codec::header`, which set the
        // global from outside this module.
        let g = EPOCH_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        LOCAL_CLUSTER_EPOCH.store(0, Ordering::Release);
        g
    }

    #[test]
    fn observe_is_monotonic_max() {
        let _g = reset();
        assert_eq!(observe_peer_cluster_epoch(5), 5);
        assert_eq!(current_local_cluster_epoch(), 5);
        // Older peer epoch must not regress the local mark.
        assert_eq!(observe_peer_cluster_epoch(3), 5);
        assert_eq!(current_local_cluster_epoch(), 5);
        // Newer advances.
        assert_eq!(observe_peer_cluster_epoch(7), 7);
        assert_eq!(current_local_cluster_epoch(), 7);
    }

    #[test]
    fn set_overrides_for_init() {
        let _g = reset();
        set_local_cluster_epoch(42);
        assert_eq!(current_local_cluster_epoch(), 42);
    }

    #[test]
    fn observe_zero_is_noop() {
        let _g = reset();
        set_local_cluster_epoch(9);
        assert_eq!(observe_peer_cluster_epoch(0), 9);
        assert_eq!(current_local_cluster_epoch(), 9);
    }

    #[test]
    fn bump_increments_and_persists() {
        let _g = reset();
        let dir = tempfile::tempdir().unwrap();
        let catalog = ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap();
        set_local_cluster_epoch(10);
        let new_epoch = bump_local_cluster_epoch(&catalog).unwrap();
        assert_eq!(new_epoch, 11);
        assert_eq!(current_local_cluster_epoch(), 11);
        assert_eq!(catalog.load_cluster_epoch().unwrap(), Some(11));
    }

    #[test]
    fn init_from_catalog_loads_persisted_value() {
        let _g = reset();
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap());
        catalog.save_cluster_epoch(123).unwrap();
        let v = init_local_cluster_epoch_from_catalog(&catalog).unwrap();
        assert_eq!(v, 123);
        assert_eq!(current_local_cluster_epoch(), 123);
    }

    #[test]
    fn init_with_no_persisted_value_starts_at_zero() {
        let _g = reset();
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap());
        let v = init_local_cluster_epoch_from_catalog(&catalog).unwrap();
        assert_eq!(v, 0);
        assert_eq!(current_local_cluster_epoch(), 0);
    }

    #[test]
    fn validate_rejects_stale_non_exempt() {
        let _g = reset();
        use crate::rpc_codec::discriminants::RPC_APPEND_ENTRIES_REQ;
        set_local_cluster_epoch(5);
        assert!(matches!(
            validate_peer_cluster_epoch(RPC_APPEND_ENTRIES_REQ, 3),
            Err(ClusterError::StalePeerEpoch {
                peer_epoch: 3,
                local_epoch: 5,
            })
        ));
        // Rejection must not touch the local mark.
        assert_eq!(current_local_cluster_epoch(), 5);
    }

    #[test]
    fn validate_accepts_equal_and_newer() {
        let _g = reset();
        use crate::rpc_codec::discriminants::RPC_EXECUTE_REQ;
        set_local_cluster_epoch(5);
        assert!(validate_peer_cluster_epoch(RPC_EXECUTE_REQ, 5).is_ok());
        assert!(validate_peer_cluster_epoch(RPC_EXECUTE_REQ, 6).is_ok());
        assert_eq!(current_local_cluster_epoch(), 5);
    }

    #[test]
    fn validate_genesis_zero_zero_ok() {
        let _g = reset();
        use crate::rpc_codec::discriminants::RPC_APPEND_ENTRIES_REQ;
        // Pre-init startup: local 0 must not reject a peer stamp of 0.
        assert!(validate_peer_cluster_epoch(RPC_APPEND_ENTRIES_REQ, 0).is_ok());
    }

    #[test]
    fn validate_exempts_join_and_ping() {
        let _g = reset();
        set_local_cluster_epoch(9);
        assert!(validate_peer_cluster_epoch(RPC_JOIN_REQ, 0).is_ok());
        assert!(validate_peer_cluster_epoch(RPC_JOIN_RESP, 0).is_ok());
        assert!(validate_peer_cluster_epoch(RPC_PING, 0).is_ok());
        assert!(validate_peer_cluster_epoch(RPC_PONG, 0).is_ok());
    }

    #[test]
    fn validate_rejects_stale_for_other_mgmt_types() {
        let _g = reset();
        use crate::rpc_codec::discriminants::{RPC_REQUEST_VOTE_RESP, RPC_TOPOLOGY_ACK};
        set_local_cluster_epoch(5);
        assert!(matches!(
            validate_peer_cluster_epoch(RPC_TOPOLOGY_ACK, 4),
            Err(ClusterError::StalePeerEpoch { .. })
        ));
        // Elections are fenced too.
        assert!(matches!(
            validate_peer_cluster_epoch(RPC_REQUEST_VOTE_RESP, 4),
            Err(ClusterError::StalePeerEpoch { .. })
        ));
    }
}
