// SPDX-License-Identifier: BUSL-1.1

//! In-process token state machine for single-use join-token enforcement.
//!
//! The `JoinTokenStore` tracks every issued token's lifecycle from
//! `Issued` through `InFlight` to `Consumed` (or `Expired`/`Aborted`).
//! In a full distributed deployment this state is proposed through
//! the metadata Raft group (as `MetadataEntry::JoinTokenTransition`) so
//! all nodes reject a replayed token even after a crash-restart.
//!
//! The trait [`TokenStateBackend`] abstracts the storage so the
//! bootstrap-listener handler can be tested with the in-memory backend
//! and wired to the Raft-backed backend in production.
//!
//! # Async trait
//!
//! `TokenStateBackend` is async (via `async_trait`) because the
//! production [`crate::auth::raft_backed_store::RaftBackedTokenStore`]
//! must call `MetadataProposer::propose_and_wait` which is inherently
//! async. Using `block_in_place` to bridge async→sync at the trait
//! boundary would couple the trait to Tokio internals and make it
//! untestable without a runtime. An async trait is the clean solution;
//! `InMemoryTokenStore` simply uses immediate `async { }` bodies.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

/// Maximum exclusive ownership interval for one credential issuance attempt.
/// A later Raft `BeginInFlight` may replace an abandoned lease only after this
/// absolute deadline, making crash recovery deterministic on every replica.
pub const INFLIGHT_LEASE_DURATION_MS: u64 = 30_000;

/// Lifecycle states for a join token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinTokenLifecycle {
    /// Token has been issued; no joiner has presented it yet.
    Issued,
    /// A joiner at `node_addr` is currently receiving its bundle.
    /// If the joiner times out, the state reverts to `Issued`.
    InFlight {
        node_addr: SocketAddr,
        lease_id: [u8; 16],
        lease_expires_at_ms: u64,
    },
    /// Bundle was successfully delivered to `node_addr`.
    /// Replay attempts on the same token are rejected.
    Consumed {
        node_addr: SocketAddr,
        lease_id: [u8; 16],
        ts_ms: u64,
        /// Opaque host-encrypted credential response retained for idempotent
        /// recovery when commit or transport acknowledgement is indeterminate.
        recovery_bundle: Vec<u8>,
    },
    /// Token's expiry timestamp has passed without being consumed.
    Expired,
    /// Explicitly invalidated (e.g. operator revoke).
    Aborted,
}

/// Complete state record for one token.
#[derive(Debug, Clone)]
pub struct JoinTokenState {
    /// SHA-256 of the token hex string. Never stores the raw token.
    pub token_hash: [u8; 32],
    pub lifecycle: JoinTokenLifecycle,
    /// Absolute unix-ms expiry derived from the token's `expiry_unix_secs`.
    pub expires_at_ms: u64,
    /// Number of times this token moved from `Issued` to `InFlight`
    /// (increments on retry after an in-flight timeout).
    pub attempt: u32,
}

/// Unforgeable ownership proof for one `Issued` → `InFlight` acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InflightLease {
    pub(crate) id: [u8; 16],
}

impl InflightLease {
    pub(crate) fn generate() -> Result<Self, TokenStateError> {
        let mut id = [0u8; 16];
        getrandom::fill(&mut id).map_err(|error| TokenStateError::Random {
            detail: error.to_string(),
        })?;
        Ok(Self { id })
    }
}

/// Error from token state transitions.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenStateError {
    #[error("join token already consumed")]
    AlreadyConsumed,
    #[error("join token expired")]
    Expired,
    #[error("join token aborted")]
    Aborted,
    #[error("join token is already in-flight from a different address")]
    InFlightConflict,
    #[error("join token not found")]
    NotFound,
    #[error("unexpected lifecycle state for this transition")]
    InvalidTransition,
    #[error("secure random generation failed: {detail}")]
    Random { detail: String },
    /// The Raft proposer returned an error. The transition was not
    /// replicated; single-use enforcement may be incomplete.
    #[error("raft proposer error: {detail}")]
    ProposerError { detail: String },
}

/// Shared read-side mirror of token lifecycle state, updated only by the
/// Raft apply path. The `RaftBackedTokenStore` and `CacheApplier` share
/// this handle so the applier can write apply-path updates and the store
/// can read post-apply state without a second round-trip.
pub type SharedTokenStateMirror = Arc<Mutex<HashMap<[u8; 32], JoinTokenState>>>;

/// Abstraction over where token state is persisted.
///
/// The in-memory implementation ([`InMemoryTokenStore`]) is used in
/// tests and single-node deployments that don't need cross-crash
/// single-use guarantees. Production deployments wire a Raft-backed
/// implementation that proposes each transition through the metadata
/// group.
///
/// The trait is async so the Raft-backed implementation can call
/// `MetadataProposer::propose_and_wait` directly. `InMemoryTokenStore`
/// uses immediate `async { }` bodies — zero overhead.
#[async_trait]
pub trait TokenStateBackend: Send + Sync + 'static {
    /// Register a freshly issued token with `Issued` state.
    async fn register(&self, state: JoinTokenState);
    /// Attempt to transition from `Issued` → `InFlight`.
    /// Returns `Err(AlreadyConsumed)` / `Err(Expired)` / `Err(Aborted)`
    /// if the token is already in a terminal or conflicting state.
    async fn begin_inflight(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
    ) -> Result<InflightLease, TokenStateError>;
    /// Transition from `InFlight` → `Consumed`. Production bootstrap must
    /// commit this transition before exposing usable credential bytes.
    async fn mark_consumed(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
        lease: InflightLease,
        ts_ms: u64,
        recovery_bundle: Vec<u8>,
    ) -> Result<(), TokenStateError>;
    /// Revert `InFlight` → `Issued` only while no usable credential bytes
    /// have been exposed to the joiner and the caller still owns that attempt.
    async fn revert_inflight(
        &self,
        token_hash: &[u8; 32],
        lease: InflightLease,
    ) -> Result<(), TokenStateError>;
    /// Look up the current state.
    fn get(&self, token_hash: &[u8; 32]) -> Option<JoinTokenState>;
}

/// Simple in-memory token store. Thread-safe via `Mutex`.
///
/// Suitable for tests and single-node deployments. Does not survive
/// process restart — a previously-consumed token is invisible after
/// restart, allowing replay on that edge case. Production deployments
/// must use a Raft-backed backend.
#[derive(Default, Clone)]
pub struct InMemoryTokenStore {
    inner: Arc<Mutex<HashMap<[u8; 32], JoinTokenState>>>,
}

impl InMemoryTokenStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TokenStateBackend for InMemoryTokenStore {
    async fn register(&self, state: JoinTokenState) {
        let mut map = self.inner.lock().expect("token store lock poisoned");
        map.entry(state.token_hash).or_insert(state);
    }

    async fn begin_inflight(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
    ) -> Result<InflightLease, TokenStateError> {
        let lease = InflightLease::generate()?;
        let mut map = self.inner.lock().expect("token store lock poisoned");
        let entry = map.get_mut(token_hash).ok_or(TokenStateError::NotFound)?;
        let now_ms = epoch_ms();
        match &entry.lifecycle {
            JoinTokenLifecycle::Issued => {
                if now_ms > entry.expires_at_ms {
                    entry.lifecycle = JoinTokenLifecycle::Expired;
                    return Err(TokenStateError::Expired);
                }
                entry.lifecycle = JoinTokenLifecycle::InFlight {
                    node_addr,
                    lease_id: lease.id,
                    lease_expires_at_ms: now_ms.saturating_add(INFLIGHT_LEASE_DURATION_MS),
                };
                entry.attempt += 1;
                Ok(lease)
            }
            JoinTokenLifecycle::InFlight {
                lease_expires_at_ms,
                ..
            } if now_ms >= *lease_expires_at_ms => {
                if now_ms > entry.expires_at_ms {
                    entry.lifecycle = JoinTokenLifecycle::Expired;
                    return Err(TokenStateError::Expired);
                }
                entry.lifecycle = JoinTokenLifecycle::InFlight {
                    node_addr,
                    lease_id: lease.id,
                    lease_expires_at_ms: now_ms.saturating_add(INFLIGHT_LEASE_DURATION_MS),
                };
                entry.attempt += 1;
                Ok(lease)
            }
            JoinTokenLifecycle::InFlight { .. } => Err(TokenStateError::InFlightConflict),
            JoinTokenLifecycle::Consumed { .. } => Err(TokenStateError::AlreadyConsumed),
            JoinTokenLifecycle::Expired => Err(TokenStateError::Expired),
            JoinTokenLifecycle::Aborted => Err(TokenStateError::Aborted),
        }
    }

    async fn mark_consumed(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
        lease: InflightLease,
        ts_ms: u64,
        recovery_bundle: Vec<u8>,
    ) -> Result<(), TokenStateError> {
        let mut map = self.inner.lock().expect("token store lock poisoned");
        let entry = map.get_mut(token_hash).ok_or(TokenStateError::NotFound)?;
        match &entry.lifecycle {
            JoinTokenLifecycle::InFlight { lease_id, .. } if *lease_id == lease.id => {
                entry.lifecycle = JoinTokenLifecycle::Consumed {
                    node_addr,
                    lease_id: lease.id,
                    ts_ms,
                    recovery_bundle,
                };
                Ok(())
            }
            JoinTokenLifecycle::Consumed { .. } => Err(TokenStateError::AlreadyConsumed),
            _ => Err(TokenStateError::InvalidTransition),
        }
    }

    async fn revert_inflight(
        &self,
        token_hash: &[u8; 32],
        lease: InflightLease,
    ) -> Result<(), TokenStateError> {
        let mut map = self.inner.lock().expect("token store lock poisoned");
        let entry = map.get_mut(token_hash).ok_or(TokenStateError::NotFound)?;
        match &entry.lifecycle {
            JoinTokenLifecycle::InFlight { lease_id, .. } if *lease_id == lease.id => {
                entry.lifecycle = JoinTokenLifecycle::Issued;
                Ok(())
            }
            _ => Err(TokenStateError::InvalidTransition),
        }
    }

    fn get(&self, token_hash: &[u8; 32]) -> Option<JoinTokenState> {
        let map = self.inner.lock().expect("token store lock poisoned");
        map.get(token_hash).cloned()
    }
}

/// Spawn an async dead-man timer that reverts an `InFlight` token to
/// `Issued` after `timeout` if it has not been consumed. This runs as a
/// detached task; callers call it immediately after `begin_inflight`.
pub fn spawn_inflight_timeout<B: TokenStateBackend>(
    backend: Arc<B>,
    token_hash: [u8; 32],
    lease: InflightLease,
    timeout: Duration,
) {
    tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        // If still InFlight, revert — the joiner timed out.
        let _ = backend.revert_inflight(&token_hash, lease).await;
    });
}

fn epoch_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    fn make_state(hash: [u8; 32], expires_in_secs: u64) -> JoinTokenState {
        let expires_at_ms = epoch_ms() + expires_in_secs * 1000;
        JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms,
            attempt: 0,
        }
    }

    #[tokio::test]
    async fn issued_to_inflight_to_consumed() {
        let store = InMemoryTokenStore::new();
        let hash = [0x01u8; 32];
        store.register(make_state(hash, 60)).await;

        let addr = dummy_addr();
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        {
            let s = store.get(&hash).unwrap();
            assert!(matches!(
                s.lifecycle,
                JoinTokenLifecycle::InFlight {
                    node_addr,
                    lease_id,
                    lease_expires_at_ms,
                } if node_addr == addr && lease_id == lease.id && lease_expires_at_ms > epoch_ms()
            ));
            assert_eq!(s.attempt, 1);
        }

        let ts = epoch_ms();
        store
            .mark_consumed(&hash, addr, lease, ts, vec![1, 2, 3])
            .await
            .unwrap();
        let s = store.get(&hash).unwrap();
        assert_eq!(
            s.lifecycle,
            JoinTokenLifecycle::Consumed {
                node_addr: addr,
                lease_id: lease.id,
                ts_ms: ts,
                recovery_bundle: vec![1, 2, 3],
            }
        );
    }

    #[tokio::test]
    async fn replay_on_consumed_token_returns_error() {
        let store = InMemoryTokenStore::new();
        let hash = [0x02u8; 32];
        store.register(make_state(hash, 60)).await;
        let addr = dummy_addr();
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        store
            .mark_consumed(&hash, addr, lease, epoch_ms(), Vec::new())
            .await
            .unwrap();

        // Second begin_inflight must be rejected.
        assert_eq!(
            store.begin_inflight(&hash, addr).await.unwrap_err(),
            TokenStateError::AlreadyConsumed
        );
    }

    #[tokio::test]
    async fn inflight_reverts_to_issued_on_timeout() {
        let store = InMemoryTokenStore::new();
        let hash = [0x03u8; 32];
        store.register(make_state(hash, 60)).await;
        let addr = dummy_addr();
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        store.revert_inflight(&hash, lease).await.unwrap();
        let s = store.get(&hash).unwrap();
        assert_eq!(s.lifecycle, JoinTokenLifecycle::Issued);
        // Second attempt is allowed after revert.
        store.begin_inflight(&hash, addr).await.unwrap();
        let s = store.get(&hash).unwrap();
        assert_eq!(s.attempt, 2);
    }

    #[tokio::test]
    async fn expired_token_rejected() {
        let store = InMemoryTokenStore::new();
        let hash = [0x04u8; 32];
        // expires_at_ms in the past
        let state = JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms: 1, // Jan 1, 1970 — long expired
            attempt: 0,
        };
        store.register(state).await;
        assert_eq!(
            store.begin_inflight(&hash, dummy_addr()).await.unwrap_err(),
            TokenStateError::Expired
        );
        // State machine must have transitioned to Expired.
        let s = store.get(&hash).unwrap();
        assert_eq!(s.lifecycle, JoinTokenLifecycle::Expired);
    }

    #[tokio::test]
    async fn aborted_token_rejected() {
        let store = InMemoryTokenStore::new();
        let hash = [0x05u8; 32];
        let mut state = make_state(hash, 60);
        state.lifecycle = JoinTokenLifecycle::Aborted;
        store.register(state).await;
        assert_eq!(
            store.begin_inflight(&hash, dummy_addr()).await.unwrap_err(),
            TokenStateError::Aborted
        );
    }

    #[tokio::test]
    async fn inflight_same_addr_is_exclusive() {
        let store = InMemoryTokenStore::new();
        let hash = [0x06u8; 32];
        store.register(make_state(hash, 60)).await;
        let addr = dummy_addr();
        let lease = store.begin_inflight(&hash, addr).await.unwrap();
        assert_eq!(
            store.begin_inflight(&hash, addr).await.unwrap_err(),
            TokenStateError::InFlightConflict
        );
        let wrong = InflightLease::generate().unwrap();
        assert_eq!(
            store
                .mark_consumed(&hash, addr, wrong, epoch_ms(), Vec::new())
                .await
                .unwrap_err(),
            TokenStateError::InvalidTransition
        );
        store
            .mark_consumed(&hash, addr, lease, epoch_ms(), Vec::new())
            .await
            .unwrap();
    }
}
