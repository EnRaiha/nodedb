// SPDX-License-Identifier: BUSL-1.1

//! Raft-backed `TokenStateBackend` for crash-safe single-use enforcement.
//!
//! # Design
//!
//! Every token lifecycle transition is proposed through the metadata Raft
//! group via `MetadataProposer::propose_and_wait`. The proposal blocks
//! until the entry has been committed **and applied on this node** (the
//! contract of `propose_and_wait`). The apply path updates the
//! `SharedTokenStateMirror` held by `CacheApplier::with_token_state`.
//! Because propose_and_wait returns only after local apply, the mirror
//! is always current when the method returns — no extra synchronisation
//! window is needed.
//!
//! # Registration-on-first-sight
//!
//! Join tokens are issued offline by `nodedb-ctl join-token --create` and
//! printed to the operator; the CLI has no live Raft proposer available.
//! Rather than requiring a separate online registration RPC, the bootstrap
//! listener registers a valid-HMAC token on first sight by proposing a
//! `Register` entry before `BeginInFlight`. This means the CLI can remain
//! stateless and the Raft log is the single source of truth for every
//! token that has ever been presented.
//!
//! # Pre-Raft window
//!
//! If the bootstrap listener starts before the metadata Raft group has a
//! leader (e.g. the very first node of a fresh cluster bootstrapping
//! itself), `propose_and_wait` will time out or fail. In that scenario
//! the operator should use `InMemoryTokenStore` until the group is up,
//! then switch. The `RaftBackedTokenStore` does NOT fall back to
//! in-memory silently — any proposer failure is surfaced as an error so
//! the operator knows single-use enforcement was not applied.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tracing::{error, warn};

use crate::auth::token_state::{
    INFLIGHT_LEASE_DURATION_MS, InflightLease, JoinTokenLifecycle, JoinTokenState,
    SharedTokenStateMirror, TokenStateBackend, TokenStateError,
};
use crate::decommission::coordinator::MetadataProposer;
use crate::metadata_group::entry::{JoinTokenTransitionKind, MetadataEntry};

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Raft-backed token state backend. All lifecycle transitions are proposed
/// through the metadata group and reflected into the local mirror by the
/// apply path (`CacheApplier::with_token_state`).
///
/// The mirror is the read-side cache; writes (i.e. applying transitions)
/// happen exclusively in the `CacheApplier` so all nodes see the same
/// ordered sequence of state changes.
pub struct RaftBackedTokenStore {
    proposer: Arc<dyn MetadataProposer>,
    /// Read-side mirror updated by the apply path. Shared with the
    /// `CacheApplier` that owns the write-side.
    mirror: SharedTokenStateMirror,
}

impl RaftBackedTokenStore {
    /// Construct a new store. `mirror` must be the same `Arc` that is
    /// passed to `CacheApplier::with_token_state` so apply-path writes
    /// are immediately visible here.
    pub fn new(proposer: Arc<dyn MetadataProposer>, mirror: SharedTokenStateMirror) -> Self {
        Self { proposer, mirror }
    }

    /// Propose a transition entry and wait for it to commit and apply.
    async fn propose(
        &self,
        token_hash: [u8; 32],
        transition: JoinTokenTransitionKind,
    ) -> Result<(), TokenStateError> {
        let entry = MetadataEntry::JoinTokenTransition {
            token_hash,
            transition,
            ts_ms: epoch_ms(),
        };
        self.proposer
            .propose_and_wait(entry)
            .await
            .map(|_| ())
            .map_err(|e| {
                error!(error = %e, "RaftBackedTokenStore: proposer error");
                TokenStateError::ProposerError {
                    detail: e.to_string(),
                }
            })
    }

    fn read_state(&self, token_hash: &[u8; 32]) -> Option<JoinTokenState> {
        let map = self.mirror.lock().unwrap_or_else(|p| p.into_inner());
        map.get(token_hash).cloned()
    }
}

#[async_trait]
impl TokenStateBackend for RaftBackedTokenStore {
    /// Propose `Register` so all Raft peers learn about this token.
    /// The apply path inserts it into the mirror with `Issued` lifecycle.
    async fn register(&self, state: JoinTokenState) {
        let hash = state.token_hash;
        let expires_at_ms = state.expires_at_ms;
        if let Err(e) = self
            .propose(hash, JoinTokenTransitionKind::Register { expires_at_ms })
            .await
        {
            // Registration failure means single-use is not enforced across
            // nodes. Surface loudly — never silently degrade.
            error!(
                error = %e,
                token_hash = ?hash,
                "RaftBackedTokenStore: register failed — token not replicated"
            );
        }
    }

    /// Propose `BeginInFlight` and read back the post-apply state to
    /// detect races (two nodes proposing simultaneously; only one wins).
    async fn begin_inflight(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
    ) -> Result<InflightLease, TokenStateError> {
        let lease = InflightLease::generate()?;
        // Pre-flight: check for terminal states before round-tripping Raft.
        if let Some(current) = self.read_state(token_hash) {
            match &current.lifecycle {
                JoinTokenLifecycle::Consumed { .. } => {
                    return Err(TokenStateError::AlreadyConsumed);
                }
                JoinTokenLifecycle::Expired => return Err(TokenStateError::Expired),
                JoinTokenLifecycle::Aborted => return Err(TokenStateError::Aborted),
                _ => {}
            }
        }

        let addr_str = node_addr.to_string();
        self.propose(
            *token_hash,
            JoinTokenTransitionKind::BeginInFlight {
                node_addr: addr_str.clone(),
                lease_id: lease.id,
            },
        )
        .await?;

        // Re-read post-apply state. The apply arm enforces the transition
        // semantics; if a concurrent proposal won, the mirror will reflect
        // the winner's state and we surface the right error.
        match self.read_state(token_hash) {
            None => Err(TokenStateError::NotFound),
            Some(s) => match &s.lifecycle {
                JoinTokenLifecycle::InFlight {
                    node_addr: winner,
                    lease_id,
                    ..
                } => {
                    if *winner == node_addr && *lease_id == lease.id {
                        Ok(lease)
                    } else {
                        Err(TokenStateError::InFlightConflict)
                    }
                }
                JoinTokenLifecycle::Consumed { .. } => Err(TokenStateError::AlreadyConsumed),
                JoinTokenLifecycle::Expired => Err(TokenStateError::Expired),
                JoinTokenLifecycle::Aborted => Err(TokenStateError::Aborted),
                JoinTokenLifecycle::Issued => {
                    // Apply arm rejected the transition (e.g. expiry detected).
                    // The exact reason is in the mirror state — distinguish.
                    warn!(
                        token_hash = ?token_hash,
                        "RaftBackedTokenStore: begin_inflight propose succeeded but state remained Issued"
                    );
                    Err(TokenStateError::InvalidTransition)
                }
            },
        }
    }

    async fn mark_consumed(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
        lease: InflightLease,
        _ts_ms: u64,
        recovery_bundle: Vec<u8>,
    ) -> Result<(), TokenStateError> {
        let addr_str = node_addr.to_string();
        self.propose(
            *token_hash,
            JoinTokenTransitionKind::MarkConsumed {
                node_addr: addr_str,
                lease_id: lease.id,
                recovery_bundle,
            },
        )
        .await?;

        match self.read_state(token_hash) {
            None => Err(TokenStateError::NotFound),
            Some(s) => match &s.lifecycle {
                JoinTokenLifecycle::Consumed {
                    node_addr: consumed_addr,
                    lease_id,
                    ..
                } if *consumed_addr == node_addr && *lease_id == lease.id => Ok(()),
                JoinTokenLifecycle::Consumed { .. } => Err(TokenStateError::AlreadyConsumed),
                _ => Err(TokenStateError::InvalidTransition),
            },
        }
    }

    async fn revert_inflight(
        &self,
        token_hash: &[u8; 32],
        lease: InflightLease,
    ) -> Result<(), TokenStateError> {
        self.propose(
            *token_hash,
            JoinTokenTransitionKind::RevertInFlight { lease_id: lease.id },
        )
        .await?;

        match self.read_state(token_hash) {
            None => Err(TokenStateError::NotFound),
            Some(s) => match &s.lifecycle {
                JoinTokenLifecycle::Issued => Ok(()),
                _ => Err(TokenStateError::InvalidTransition),
            },
        }
    }

    fn get(&self, token_hash: &[u8; 32]) -> Option<JoinTokenState> {
        self.read_state(token_hash)
    }
}
/// Apply a single `JoinTokenTransitionKind` to the mirror.
///
/// This function is the **single** place where the state machine
/// transitions are written; it is called from:
/// - `CacheApplier::cascade_live_state` (production apply path), and
/// - tests in this module (simulated proposer).
///
/// Apply must be **deterministic** and **idempotent**:
/// - Replaying the same log entry on recovery converges to the same state.
/// - Transitions that are already in the target state are no-ops.
/// - Invalid transitions (e.g. `BeginInFlight` on a `Consumed` token) log
///   an error but do NOT panic — the apply path must never abort.
pub fn apply_token_transition_to_mirror(
    mirror: &SharedTokenStateMirror,
    token_hash: [u8; 32],
    transition: &JoinTokenTransitionKind,
    ts_ms: u64,
) {
    let mut map = mirror.lock().unwrap_or_else(|p| p.into_inner());

    match transition {
        JoinTokenTransitionKind::Register { expires_at_ms } => {
            // Idempotent: if already registered, keep existing state.
            map.entry(token_hash).or_insert_with(|| JoinTokenState {
                token_hash,
                lifecycle: JoinTokenLifecycle::Issued,
                expires_at_ms: *expires_at_ms,
                attempt: 0,
            });
        }

        JoinTokenTransitionKind::BeginInFlight {
            node_addr,
            lease_id,
        } => {
            let Ok(parsed_addr) = node_addr.parse::<SocketAddr>() else {
                error!(
                    token_hash = ?token_hash,
                    addr = %node_addr,
                    "apply BeginInFlight: invalid address — skipping"
                );
                return;
            };
            if let Some(entry) = map.get_mut(&token_hash) {
                match &entry.lifecycle {
                    JoinTokenLifecycle::Issued if ts_ms > entry.expires_at_ms => {
                        entry.lifecycle = JoinTokenLifecycle::Expired;
                    }
                    JoinTokenLifecycle::Issued => {
                        entry.lifecycle = JoinTokenLifecycle::InFlight {
                            node_addr: parsed_addr,
                            lease_id: *lease_id,
                            lease_expires_at_ms: ts_ms.saturating_add(INFLIGHT_LEASE_DURATION_MS),
                        };
                        entry.attempt += 1;
                    }
                    JoinTokenLifecycle::InFlight {
                        node_addr: existing,
                        lease_id: existing_lease,
                        ..
                    } if *existing == parsed_addr && *existing_lease == *lease_id => {
                        // Idempotent replay of the same committed proposal.
                    }
                    JoinTokenLifecycle::InFlight {
                        lease_expires_at_ms,
                        ..
                    } if ts_ms >= *lease_expires_at_ms => {
                        if ts_ms > entry.expires_at_ms {
                            entry.lifecycle = JoinTokenLifecycle::Expired;
                        } else {
                            entry.lifecycle = JoinTokenLifecycle::InFlight {
                                node_addr: parsed_addr,
                                lease_id: *lease_id,
                                lease_expires_at_ms: ts_ms
                                    .saturating_add(INFLIGHT_LEASE_DURATION_MS),
                            };
                            entry.attempt += 1;
                        }
                    }
                    other => {
                        error!(
                            token_hash = ?token_hash,
                            ?other,
                            "apply BeginInFlight: active or terminal lifecycle rejected"
                        );
                    }
                }
            } else {
                error!(
                    token_hash = ?token_hash,
                    "apply BeginInFlight: token not in mirror (Register must precede this)"
                );
            }
        }

        JoinTokenTransitionKind::MarkConsumed {
            node_addr,
            lease_id,
            recovery_bundle,
        } => {
            let Ok(parsed_addr) = node_addr.parse::<SocketAddr>() else {
                error!(
                    token_hash = ?token_hash,
                    addr = %node_addr,
                    "apply MarkConsumed: invalid address — skipping"
                );
                return;
            };
            if let Some(entry) = map.get_mut(&token_hash) {
                match &entry.lifecycle {
                    JoinTokenLifecycle::InFlight {
                        lease_id: active_lease,
                        ..
                    } if *active_lease == *lease_id => {
                        entry.lifecycle = JoinTokenLifecycle::Consumed {
                            node_addr: parsed_addr,
                            lease_id: *lease_id,
                            ts_ms,
                            recovery_bundle: recovery_bundle.clone(),
                        };
                    }
                    JoinTokenLifecycle::Consumed { .. } => {
                        // Idempotent: already consumed (duplicate delivery).
                    }
                    other => {
                        error!(
                            token_hash = ?token_hash,
                            ?other,
                            "apply MarkConsumed: unexpected lifecycle"
                        );
                    }
                }
            } else {
                error!(token_hash = ?token_hash, "apply MarkConsumed: token not in mirror");
            }
        }

        JoinTokenTransitionKind::RevertInFlight { lease_id } => {
            if let Some(entry) = map.get_mut(&token_hash) {
                match &entry.lifecycle {
                    JoinTokenLifecycle::InFlight {
                        lease_id: active_lease,
                        ..
                    } if *active_lease == *lease_id => {
                        entry.lifecycle = JoinTokenLifecycle::Issued;
                    }
                    JoinTokenLifecycle::Issued => {
                        // Idempotent: already reverted (duplicate delivery).
                    }
                    other => {
                        error!(
                            token_hash = ?token_hash,
                            ?other,
                            "apply RevertInFlight: unexpected lifecycle"
                        );
                    }
                }
            } else {
                error!(token_hash = ?token_hash, "apply RevertInFlight: token not in mirror");
            }
        }

        JoinTokenTransitionKind::MarkExpired => {
            if let Some(entry) = map.get_mut(&token_hash) {
                match &entry.lifecycle {
                    JoinTokenLifecycle::Issued | JoinTokenLifecycle::InFlight { .. } => {
                        entry.lifecycle = JoinTokenLifecycle::Expired;
                    }
                    JoinTokenLifecycle::Expired => {} // Idempotent.
                    other => {
                        error!(
                            token_hash = ?token_hash,
                            ?other,
                            "apply MarkExpired: unexpected lifecycle"
                        );
                    }
                }
            } else {
                error!(token_hash = ?token_hash, "apply MarkExpired: token not in mirror");
            }
        }

        JoinTokenTransitionKind::MarkAborted => {
            if let Some(entry) = map.get_mut(&token_hash) {
                match &entry.lifecycle {
                    JoinTokenLifecycle::Aborted => {} // Idempotent.
                    _ => {
                        entry.lifecycle = JoinTokenLifecycle::Aborted;
                    }
                }
            } else {
                error!(token_hash = ?token_hash, "apply MarkAborted: token not in mirror");
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token_state::InMemoryTokenStore;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    // A proposer that applies transitions to both a mirror and a recording list.
    struct MirroringProposer {
        counter: AtomicU64,
        mirror: SharedTokenStateMirror,
    }

    impl MirroringProposer {
        fn new(mirror: SharedTokenStateMirror) -> Arc<Self> {
            Arc::new(Self {
                counter: AtomicU64::new(0),
                mirror,
            })
        }
    }

    #[async_trait]
    impl MetadataProposer for MirroringProposer {
        async fn propose_and_wait(&self, entry: MetadataEntry) -> crate::error::Result<u64> {
            let idx = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
            // Simulate the apply path: write into mirror.
            if let MetadataEntry::JoinTokenTransition {
                token_hash,
                transition,
                ts_ms,
            } = entry
            {
                apply_token_transition_to_mirror(&self.mirror, token_hash, &transition, ts_ms);
            }
            Ok(idx)
        }
    }

    fn make_store() -> (RaftBackedTokenStore, SharedTokenStateMirror) {
        let mirror: SharedTokenStateMirror = Arc::new(Mutex::new(HashMap::new()));
        let proposer = MirroringProposer::new(mirror.clone());
        let store = RaftBackedTokenStore::new(proposer, mirror.clone());
        (store, mirror)
    }

    fn issued_state(hash: [u8; 32], expires_at_ms: u64) -> JoinTokenState {
        JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms,
            attempt: 0,
        }
    }

    fn far_future_ms() -> u64 {
        epoch_ms() + 3_600_000
    }

    #[tokio::test]
    async fn register_and_begin_inflight_consume() {
        let (store, _mirror) = make_store();
        let hash = [0xA1u8; 32];
        let addr: SocketAddr = "127.0.0.1:9100".parse().unwrap();

        store.register(issued_state(hash, far_future_ms())).await;
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        store
            .mark_consumed(&hash, addr, lease, epoch_ms(), vec![1])
            .await
            .unwrap();

        let s = store.get(&hash).unwrap();
        assert!(matches!(s.lifecycle, JoinTokenLifecycle::Consumed { .. }));
    }

    #[tokio::test]
    async fn replay_after_consume_rejected() {
        let (store, _mirror) = make_store();
        let hash = [0xA2u8; 32];
        let addr: SocketAddr = "127.0.0.1:9101".parse().unwrap();

        store.register(issued_state(hash, far_future_ms())).await;
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        store
            .mark_consumed(&hash, addr, lease, epoch_ms(), Vec::new())
            .await
            .unwrap();

        assert_eq!(
            store.begin_inflight(&hash, addr).await.unwrap_err(),
            TokenStateError::AlreadyConsumed
        );
    }

    #[tokio::test]
    async fn concurrent_same_address_acquisition_has_one_owner() {
        let (store, _mirror) = make_store();
        let hash = [0xA7u8; 32];
        let addr: SocketAddr = "127.0.0.1:9107".parse().unwrap();
        store.register(issued_state(hash, far_future_ms())).await;

        let (first, second) = tokio::join!(
            store.begin_inflight(&hash, addr),
            store.begin_inflight(&hash, addr)
        );
        let successes = usize::from(first.is_ok()) + usize::from(second.is_ok());
        assert_eq!(successes, 1, "exactly one issuance attempt owns the token");
        let failure = first.err().or_else(|| second.err()).unwrap();
        assert_eq!(failure, TokenStateError::InFlightConflict);
    }

    #[test]
    fn expired_committed_inflight_lease_is_replaced_after_restart() {
        let mirror: SharedTokenStateMirror = Arc::new(Mutex::new(HashMap::new()));
        let hash = [0xA8; 32];
        let first_lease = [0x11; 16];
        let replacement_lease = [0x22; 16];
        apply_token_transition_to_mirror(
            &mirror,
            hash,
            &JoinTokenTransitionKind::Register {
                expires_at_ms: 1_000_000,
            },
            100,
        );
        apply_token_transition_to_mirror(
            &mirror,
            hash,
            &JoinTokenTransitionKind::BeginInFlight {
                node_addr: "127.0.0.1:9108".into(),
                lease_id: first_lease,
            },
            100,
        );
        apply_token_transition_to_mirror(
            &mirror,
            hash,
            &JoinTokenTransitionKind::BeginInFlight {
                node_addr: "127.0.0.1:9109".into(),
                lease_id: replacement_lease,
            },
            100 + INFLIGHT_LEASE_DURATION_MS,
        );

        let state = mirror.lock().unwrap().get(&hash).cloned().unwrap();
        assert!(matches!(
            state.lifecycle,
            JoinTokenLifecycle::InFlight {
                node_addr,
                lease_id,
                ..
            } if node_addr == "127.0.0.1:9109".parse::<SocketAddr>().unwrap() && lease_id == replacement_lease
        ));
        assert_eq!(state.attempt, 2);
    }

    #[tokio::test]
    async fn revert_inflight_allows_retry() {
        let (store, _mirror) = make_store();
        let hash = [0xA3u8; 32];
        let addr: SocketAddr = "127.0.0.1:9102".parse().unwrap();

        store.register(issued_state(hash, far_future_ms())).await;
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        store.revert_inflight(&hash, lease).await.unwrap();

        let s = store.get(&hash).unwrap();
        assert_eq!(s.lifecycle, JoinTokenLifecycle::Issued);
        // Retry works.
        store.begin_inflight(&hash, addr).await.unwrap();
    }

    /// Cross-node replay simulation: node A consumes via mirror, node B
    /// tries begin_inflight against the same mirror and is rejected.
    #[tokio::test]
    async fn cross_node_replay_rejected_via_shared_mirror() {
        // Both "nodes" share one mirror — simulates apply-path replication.
        let mirror: SharedTokenStateMirror = Arc::new(Mutex::new(HashMap::new()));
        let hash = [0xA4u8; 32];
        let addr_a: SocketAddr = "10.0.0.1:9000".parse().unwrap();
        let addr_b: SocketAddr = "10.0.0.2:9000".parse().unwrap();

        // Node A: register + consume.
        let proposer_a = MirroringProposer::new(mirror.clone());
        let store_a = RaftBackedTokenStore::new(proposer_a, mirror.clone());
        store_a.register(issued_state(hash, far_future_ms())).await;
        let lease = store_a.begin_inflight(&hash, addr_a).await.unwrap();
        store_a
            .mark_consumed(&hash, addr_a, lease, epoch_ms(), Vec::new())
            .await
            .unwrap();

        // Node B: same mirror, different proposer instance.
        let proposer_b = MirroringProposer::new(mirror.clone());
        let store_b = RaftBackedTokenStore::new(proposer_b, mirror.clone());
        assert_eq!(
            store_b.begin_inflight(&hash, addr_b).await.unwrap_err(),
            TokenStateError::AlreadyConsumed,
            "node B must reject replayed consumed token"
        );
    }

    /// Crash-recovery simulation: drop mirror and replay log into fresh one.
    #[tokio::test]
    async fn crash_recovery_replay_rejected() {
        let mirror: SharedTokenStateMirror = Arc::new(Mutex::new(HashMap::new()));
        let hash = [0xA5u8; 32];
        let addr: SocketAddr = "10.0.0.1:9001".parse().unwrap();

        // Original node: register + consume.
        let proposer = MirroringProposer::new(mirror.clone());
        let store = RaftBackedTokenStore::new(proposer, mirror.clone());
        store.register(issued_state(hash, far_future_ms())).await;
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        store
            .mark_consumed(&hash, addr, lease, epoch_ms(), vec![7, 8])
            .await
            .unwrap();

        // "Crash": create a fresh mirror and replay the committed log entries.
        let fresh_mirror: SharedTokenStateMirror = Arc::new(Mutex::new(HashMap::new()));
        // Simulate log replay: Register → BeginInFlight → MarkConsumed.
        let ts = epoch_ms();
        let replay_lease = [0x5a; 16];
        apply_token_transition_to_mirror(
            &fresh_mirror,
            hash,
            &JoinTokenTransitionKind::Register {
                expires_at_ms: far_future_ms(),
            },
            ts,
        );
        apply_token_transition_to_mirror(
            &fresh_mirror,
            hash,
            &JoinTokenTransitionKind::BeginInFlight {
                node_addr: addr.to_string(),
                lease_id: replay_lease,
            },
            ts,
        );
        apply_token_transition_to_mirror(
            &fresh_mirror,
            hash,
            &JoinTokenTransitionKind::MarkConsumed {
                node_addr: addr.to_string(),
                lease_id: replay_lease,
                recovery_bundle: vec![7, 8],
            },
            ts,
        );

        // After recovery, a replay attempt must be rejected.
        let proposer2 = MirroringProposer::new(fresh_mirror.clone());
        let store2 = RaftBackedTokenStore::new(proposer2, fresh_mirror.clone());
        assert_eq!(
            store2.begin_inflight(&hash, addr).await.unwrap_err(),
            TokenStateError::AlreadyConsumed,
            "post-crash replay must be rejected"
        );
    }

    /// `InMemoryTokenStore` is still usable unchanged (trait compat check).
    #[tokio::test]
    async fn in_memory_store_still_works_async() {
        let store = InMemoryTokenStore::new();
        let hash = [0xA6u8; 32];
        let addr: SocketAddr = "127.0.0.1:9200".parse().unwrap();
        store
            .register(JoinTokenState {
                token_hash: hash,
                lifecycle: JoinTokenLifecycle::Issued,
                expires_at_ms: far_future_ms(),
                attempt: 0,
            })
            .await;
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        store
            .mark_consumed(&hash, addr, lease, epoch_ms(), Vec::new())
            .await
            .unwrap();
        assert!(matches!(
            store.get(&hash).unwrap().lifecycle,
            JoinTokenLifecycle::Consumed { .. }
        ));
    }
}

// ── Apply helper (shared between applier.rs and tests) ──────────────────────
