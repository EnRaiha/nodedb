// SPDX-License-Identifier: BUSL-1.1

//! [`SurrogateAssigner`] struct definition, construction, and the small
//! lock/handle accessors shared by [`super::assign_ops`] and
//! [`super::flush`].

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock, Weak};

use tokio::sync::{Notify, oneshot};

use super::super::super::registry::SurrogateRegistry;
use super::super::super::wal_appender::SurrogateWalAppender;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Shared handle to the surrogate registry. Lives on `SharedState`
/// and is cloned (cheaply) into every CP path that allocates
/// surrogates.
///
/// The inner `RwLock` is held only for the duration of one
/// `assign_surrogate` call (write lock) — the registry's hot-path
/// `alloc_one` uses atomics, so the lock is uncontended.
pub type SurrogateRegistryHandle = Arc<RwLock<SurrogateRegistry>>;

/// CP-side surrogate assigner. Owning shape — bundles the registry,
/// the credential store (which exposes the catalog), and the WAL
/// appender so call sites only need to pass `(collection, pk_bytes)`.
///
/// Stored as `Arc<SurrogateAssigner>` on `SharedState`.
///
/// Fields are `pub(in crate::control::surrogate::assign)` so the
/// sibling [`super::super::cluster_reserve`] module's `impl` block (the
/// cross-node reservation methods) can reach them despite now sitting
/// one directory deeper than `cluster_reserve.rs`; they remain private
/// outside the `assign` module.
pub struct SurrogateAssigner {
    pub(in crate::control::surrogate::assign) registry: SurrogateRegistryHandle,
    pub(in crate::control::surrogate::assign) credential_store: Arc<CredentialStore>,
    pub(in crate::control::surrogate::assign) wal_appender: Arc<dyn SurrogateWalAppender>,
    /// Weak handle to SharedState for Raft-mediated HWM proposals.
    /// Set after SharedState construction to break the Arc cycle.
    /// When set and a Raft cluster is active, the flush path proposes
    /// `MetadataEntry::SurrogateAlloc { hwm }` in addition to the
    /// local WAL record so all followers advance their HWM.
    pub(in crate::control::surrogate::assign) shared: std::sync::OnceLock<Weak<SharedState>>,
    /// Pending cluster-mode batch reservations keyed by `request_id`.
    /// `ensure_batch` registers a oneshot here before proposing; the
    /// metadata applier removes + fires it via `complete_reservation`
    /// once the carved `[start, end)` range is known at apply time.
    pub(in crate::control::surrogate::assign) pending_reservations:
        Mutex<HashMap<u64, oneshot::Sender<(u32, u32)>>>,
    /// Monotonic source of unique `request_id`s for reservations on
    /// this node. Only ever read/incremented locally.
    pub(in crate::control::surrogate::assign) next_request_id: AtomicU64,
    /// Serializes in-flight reservations so at most one batch is being
    /// reserved at a time per node. Without this, a burst of allocators
    /// that all observe an empty batch would each propose a reservation,
    /// over-reserving and wasting surrogate space.
    pub(in crate::control::surrogate::assign) reserve_gate: tokio::sync::Mutex<()>,
    /// Monotonic cache for `should_use_reservation`: set once the node
    /// first observes a multi-member metadata group, after which the
    /// per-row hot path skips the contended `cluster_topology` /
    /// `cluster_routing` RwLock reads.
    pub(in crate::control::surrogate::assign) reservation_latched: std::sync::atomic::AtomicBool,
    /// Wakes the background refill loop. The hot path nudges it (via
    /// `notify_one`) whenever a draw fails or the batch dips below the
    /// low-watermark; the refiller then performs the blocking reservation
    /// OFF the latency-critical insert path. `Notify` coalesces: a nudge
    /// while the refiller is already running is remembered as one pending
    /// permit, so no top-up is ever lost.
    pub(in crate::control::surrogate::assign) refill_notify: Arc<Notify>,
}

impl SurrogateAssigner {
    pub fn new(
        registry: SurrogateRegistryHandle,
        credential_store: Arc<CredentialStore>,
        wal_appender: Arc<dyn SurrogateWalAppender>,
    ) -> Self {
        Self {
            registry,
            credential_store,
            wal_appender,
            shared: std::sync::OnceLock::new(),
            pending_reservations: Mutex::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
            reserve_gate: tokio::sync::Mutex::new(()),
            reservation_latched: std::sync::atomic::AtomicBool::new(false),
            refill_notify: Arc::new(Notify::new()),
        }
    }

    /// Install a weak SharedState handle so the flush path can
    /// propose to Raft when in cluster mode. Called by `start_raft`
    /// after SharedState is fully wired.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// Expose the registry handle for read access by the Raft applier.
    ///
    /// The returned `Arc<RwLock<SurrogateRegistry>>` is used by
    /// `MetadataCommitApplier` to call `restore_hwm` when a
    /// `SurrogateAlloc` entry commits on a follower.
    pub fn registry_handle(&self) -> &SurrogateRegistryHandle {
        &self.registry
    }

    /// Acquire a write lock on the registry, converting a poisoned-lock
    /// error into the crate's typed `Internal` error.
    pub(super) fn registry_write(
        &self,
    ) -> crate::Result<std::sync::RwLockWriteGuard<'_, SurrogateRegistry>> {
        self.registry.write().map_err(|_| crate::Error::Internal {
            detail: "surrogate registry lock poisoned".into(),
        })
    }
}
