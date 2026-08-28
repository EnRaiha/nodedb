// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease drain proposer flow.
//!
//! Wraps the replicated `DescriptorDrainStart` / `DescriptorDrainEnd`
//! raft path with a synchronous wait loop:
//!
//! 1. Propose `DescriptorDrainStart(id, up_to_version, expires_at)`
//!    through the metadata raft group. Every node's applier installs
//!    the drain entry into `shared.lease_drain`, so a subsequent
//!    `force_refresh_lease` on any node rejects new acquires at the
//!    drained version.
//! 2. Poll `metadata_cache.leases` every 50ms, filtering for
//!    entries on the same descriptor at `version <= up_to_version`.
//!    Return `Ok(())` once the filtered set is empty.
//! 3. On deadline, propose `DescriptorDrainEnd(id)` explicitly so
//!    the cluster can make progress, then return
//!    `Err::Config { "drain timed out" }`.
//!
//! On the happy path, the `DescriptorDrainEnd` raft entry is NOT
//! emitted: the subsequent `Put*` raft entry carries the new
//! descriptor version, and the metadata applier's post-apply hook
//! calls `shared.lease_drain.install_end` implicitly on every node.
//! This saves one raft round-trip per DDL on the common path.
//!
//! ## Rolling upgrade
//!
//! The `MetadataEntry::DescriptorDrainStart` / `End` variants are
//! wire-format v4. Mixed clusters running v3 binaries can't decode
//! them, so the proposer gates on
//! `cluster_version_view().can_activate_feature(DESCRIPTOR_DRAIN_VERSION)`
//! and returns `Ok(())` immediately in compat mode — the same
//! "degrade to no drain" fallback catalog DDL uses. Mixed clusters
//! behave without drain safety until all nodes are upgraded.

use std::time::{Duration, Instant};
use tokio::runtime::RuntimeFlavor;

use nodedb_cluster::{DescriptorId, MetadataEntry, encode_entry};
use nodedb_types::Hlc;

use crate::control::rolling_upgrade::DESCRIPTOR_DRAIN_VERSION;
use crate::control::state::SharedState;
use crate::error::Error;

/// How often the drain wait loop re-polls `metadata_cache.leases`
/// to check whether the in-flight leases have drained.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Grace period added on top of the configured lease duration
/// when computing the `expires_at` stamped onto a drain entry.
/// `is_draining` does not read this value (see `lease::drain`) —
/// it is retained on the entry for observability only, so this
/// grace period has no effect on when a drain actually clears.
const DRAIN_TTL_GRACE: Duration = Duration::from_secs(30);

/// Orchestrate a full drain for a `Put*` DDL on the descriptor
/// identified by `id`, targeting prior version `up_to_version`.
///
/// Returns `Ok(())` when every lease at `version <= up_to_version`
/// has drained from `shared.metadata_cache.leases`, or when the
/// rolling-upgrade gate is closed (compat mode). Returns an error
/// on timeout, on propose failures, or if `prior_version == 0`
/// does not apply (callers should skip the call entirely for
/// creates).
///
/// `own_holds` is the count of `(id, version <= up_to_version)` refcount
/// units the REQUESTING transaction itself holds — pass `0` for a caller
/// with no transactional lease scope of its own (e.g. a bare, unbuffered
/// DDL statement). A transaction that both alters a descriptor and holds a
/// statement-time lease on that same descriptor (a buffered write to the
/// collection it is altering) cannot wait on its own hold; `own_holds` lets
/// the wait exclude exactly that many units rather than wedging until the
/// caller's own transaction releases a lease it cannot release until this
/// call returns.
pub fn drain_for_ddl(
    shared: &SharedState,
    id: DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    // Rolling upgrade gate: no drain in mixed-version clusters.
    {
        let vs = shared.cluster_version_view();
        if !vs.can_activate_feature(DESCRIPTOR_DRAIN_VERSION) {
            tracing::warn!(
                min_version = vs.min_version,
                required = DESCRIPTOR_DRAIN_VERSION,
                "descriptor lease drain: cluster in compat mode, skipping drain"
            );
            return Ok(());
        }
    }

    // Nothing to drain: no prior version means no lease could
    // have been acquired against this descriptor. Callers SHOULD
    // skip the call in that case but the guard is cheap.
    if up_to_version == 0 {
        return Ok(());
    }

    // Propose DrainStart. Every node's applier sees it and
    // installs into `shared.lease_drain`, so a subsequent
    // `force_refresh_lease` on any node rejects new acquires at
    // the drained version.
    let now_hlc = shared.hlc_clock.now();
    let ttl_ns: u64 = (max_wait + DRAIN_TTL_GRACE)
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX);
    let expires_at = Hlc::new(now_hlc.wall_ns.saturating_add(ttl_ns), 0);

    propose_drain(
        shared,
        MetadataEntry::DescriptorDrainStart {
            descriptor_id: id.clone(),
            up_to_version,
            expires_at,
        },
        "drain_start",
    )?;

    // Wait for matching leases to drain.
    match poll_leases_drained(shared, &id, up_to_version, max_wait, own_holds) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Timeout or other failure: emit DrainEnd explicitly
            // so the cluster isn't stuck rejecting acquires at
            // this version. `is_draining` has no wall-clock
            // expiry backstop (see `lease::drain`), so this
            // explicit propose is the only way the drain clears
            // if the wait above timed out — log and ignore
            // errors from the cleanup propose itself.
            if let Err(cleanup_err) = propose_drain(
                shared,
                MetadataEntry::DescriptorDrainEnd {
                    descriptor_id: id.clone(),
                },
                "drain_end",
            ) {
                tracing::warn!(
                    error = %cleanup_err,
                    "descriptor lease drain: cleanup propose failed after timeout"
                );
            }
            Err(e)
        }
    }
}

/// Wait until `metadata_cache.leases` and in-flight admission reservations
/// have no entries on `id` at `version <= up_to_version`. Polls every
/// [`POLL_INTERVAL`] until the deadline.
///
/// Stays sync on purpose. The replicated-DDL layer this sits under
/// (`metadata_proposer`) is deliberately synchronous because pgwire DDL
/// handlers are sync, so an `async fn` here would have to ripple through
/// every catalog-DDL call site and would strand the genuinely sync callers
/// (GC sweeper, clone materializer, backup restore).
///
/// It is nonetheless reached from async tasks — e.g. the ILP batch flush
/// path runs `persist_collection_replicated` -> `propose_catalog_entry` ->
/// here from a tokio worker. Parking that worker for the whole drain can
/// delay the very lease-release and raft-apply work the drain is waiting
/// on, so the wait is handed back to tokio for its duration, exactly as
/// the sibling apply wait in `propose_drain` does.
pub(crate) fn poll_leases_drained(
    shared: &SharedState,
    id: &DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    // `block_in_place` panics on the current-thread runtime and buys
    // nothing without a worker pool to hand the parked work to, so it is
    // used only where it is both legal and meaningful. Off a multi-thread
    // runtime the loop blocks the calling thread, which is what a sync
    // caller already expects.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| {
                wait_for_lease_drain(shared, id, up_to_version, max_wait, own_holds)
            })
        }
        _ => wait_for_lease_drain(shared, id, up_to_version, max_wait, own_holds),
    }
}

/// The drain wait loop itself. Split out so the convergence condition and
/// deadline handling are identical on both the `block_in_place` and the
/// plain-sync path above.
fn wait_for_lease_drain(
    shared: &SharedState,
    id: &DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    let deadline = Instant::now() + max_wait;
    loop {
        let remaining = count_matching_leases(shared, id, up_to_version, own_holds);
        if remaining == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::Config {
                detail: format!(
                    "descriptor lease drain timed out after {max_wait:?} \
                     waiting for {id:?} up to version {up_to_version} \
                     (still held: {remaining})"
                ),
            });
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Count metadata leases and admission reservations on `id` at
/// `version <= up_to_version`. `0` means the drain target has cleared. The
/// exact nonzero diagnostic count is not significant, so saturate rather than
/// risking arithmetic overflow.
///
/// Leases held by nodes that are no longer cluster members are ignored, as are
/// leases past their `expires_at`: a crashed node can never release its leases
/// (no SIGTERM path runs), so without this filter every DDL on those
/// descriptors would wedge on the drain wait forever. The membership filter is
/// fail-safe — a missing topology treats every holder as a member.
///
/// Dropping an expired lease is safe because a live holder never has one: the
/// renewal loop re-acquires any lease approaching expiry, so an expired record
/// means that node's renewal stopped. A live hold on THIS node is counted
/// separately through `lease_refcount`, below, and is unaffected by expiry.
///
/// Expiry is compared against wall time, not [`HlcClock::peek`], for the same
/// reason the renewal loop is (see `lease::renewal::tick`): `peek` returns the
/// last HLC the clock observed, which on a quiet cluster stays frozen where the
/// lease was stamped. Comparing against a frozen clock would find every lease
/// unexpired and reinstate exactly the wedge this filter removes — and an
/// idle cluster is precisely the case where a crashed node's leases are the
/// only ones left. `expires_at.wall_ns` was computed from real wall time when
/// the lease was stamped, so both sides of the comparison stay in one frame.
///
/// `own_holds` excludes that many local refcount units — the requesting
/// transaction's own — from both the local-refcount safety net AND this
/// node's own replicated cache entry, but ONLY once no other local holder
/// remains: a different session on this same node still blocks normally.
/// `own_holds == 0` (every caller except the requester's own DDL) reduces to
/// the exact original comparison.
fn count_matching_leases(
    shared: &SharedState,
    id: &DescriptorId,
    up_to_version: u64,
    own_holds: u32,
) -> usize {
    let now_wall_ns = super::wall_now_ns();
    let other_local_holds = shared
        .lease_refcount
        .current_at_or_below(id, up_to_version)
        .saturating_sub(own_holds);
    // Only the requester's own hold is left locally: its replicated cache
    // entry on this node is the very lease it is about to supersede, not a
    // conflicting holder, so it must not block the requester's own drain.
    let self_only = own_holds > 0 && other_local_holds == 0;
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let metadata_holds = cache
        .leases
        .iter()
        .filter(|((lid, holder), l)| {
            lid == id
                && l.version <= up_to_version
                && l.expires_at.wall_ns > now_wall_ns
                && lease_holder_is_member(shared, *holder)
                && !(self_only && *holder == shared.node_id)
        })
        .count();
    drop(cache);

    if other_local_holds == 0 {
        metadata_holds
    } else {
        metadata_holds.saturating_add(1)
    }
}

/// Whether `node_id` is a current cluster member. Missing topology
/// (single-node / not yet wired) treats every holder as member —
/// fail-safe: this filter only ever drops holds it is certain about.
fn lease_holder_is_member(shared: &SharedState, node_id: u64) -> bool {
    match &shared.cluster_topology {
        Some(topo) => topo
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(node_id),
        None => true,
    }
}

/// Encode + propose a drain variant through the shared
/// `metadata_proposer` helper, blocking until the local
/// applied-index watcher confirms the entry has been applied on
/// this node. Mirrors `lease::propose_and_wait` — extracted here
/// because drain variants are not `CatalogDdl` and go through a
/// different encode path.
fn propose_drain(
    shared: &SharedState,
    entry: MetadataEntry,
    operation: &'static str,
) -> Result<(), Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        // Single-node fallback: apply drain directly to the local
        // tracker by wrapping the entry in the same code path the
        // applier uses. This keeps single-node DDL tests honest:
        // they exercise drain state even without a real raft loop.
        apply_drain_locally(shared, &entry);
        return Ok(());
    };
    let raw = encode_entry(&entry).map_err(|e| Error::Config {
        detail: format!("descriptor drain {operation} encode: {e}"),
    })?;
    let log_index = handle.propose(raw)?;
    let watcher = shared.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
    const DRAIN_PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);
    let outcome =
        tokio::task::block_in_place(|| watcher.wait_for(log_index, DRAIN_PROPOSE_TIMEOUT));
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!(
                "descriptor drain {operation} did not apply within {DRAIN_PROPOSE_TIMEOUT:?} \
                 (log index {log_index}, current: {}, outcome: {outcome:?})",
                watcher.current()
            ),
        });
    }
    Ok(())
}

/// Single-node fallback: apply a drain variant directly to the
/// local tracker without going through raft. Single-node clusters
/// still install drains so DDL handlers that call `drain_for_ddl`
/// observe consistent semantics regardless of deployment mode.
fn apply_drain_locally(shared: &SharedState, entry: &MetadataEntry) {
    match entry {
        MetadataEntry::DescriptorDrainStart {
            descriptor_id,
            up_to_version,
            expires_at,
        } => {
            // Shares plan admission's gate: either an admission completes with
            // a refcount/lease before this start installs, or this drain wins
            // and subsequent admission fails closed.
            let _admission_gate = shared
                .lease_admission_gate
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            shared
                .lease_drain
                .install_start(descriptor_id.clone(), *up_to_version, *expires_at);
        }
        MetadataEntry::DescriptorDrainEnd { descriptor_id } => {
            shared.lease_drain.install_end(descriptor_id);
        }
        _ => {}
    }
}

// `descriptor_id_for_implicit_clear` and `descriptor_id_and_prior_version`
// moved to `descriptor_lookup.rs`; re-exported from `lease::mod`.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;
    use nodedb_cluster::DescriptorKind;

    #[tokio::test]
    async fn in_flight_admission_reservation_blocks_drain_count() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        state.lease_refcount.increment(&descriptor, 1);
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 1);
        state.lease_refcount.decrement(&descriptor, 1);
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);
    }

    #[tokio::test]
    async fn newer_admission_reservation_does_not_block_older_drain() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        state.lease_refcount.increment(&descriptor, 2);
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);
        state.lease_refcount.decrement(&descriptor, 2);
    }

    /// Build a topology containing exactly `ids` as active members.
    fn topo_with(ids: &[u64]) -> nodedb_cluster::ClusterTopology {
        let mut t = nodedb_cluster::ClusterTopology::new();
        for (i, id) in ids.iter().enumerate() {
            let addr: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(nodedb_cluster::NodeInfo::new(
                *id,
                addr,
                nodedb_cluster::NodeState::Active,
            ));
        }
        t
    }

    /// Insert a lease directly into the metadata cache (as if committed via a
    /// `DescriptorLeaseGrant` entry).
    fn insert_lease(
        state: &SharedState,
        id: &DescriptorId,
        holder: u64,
        version: u64,
        expires_at: nodedb_types::Hlc,
    ) {
        state
            .metadata_cache
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .leases
            .insert(
                (id.clone(), holder),
                nodedb_cluster::DescriptorLease {
                    descriptor_id: id.clone(),
                    version,
                    node_id: holder,
                    expires_at,
                },
            );
    }

    /// A lease expiry a minute into the future, in REAL wall time — the same
    /// frame the grant path stamps in. Deriving these from `hlc_clock.peek()`
    /// would put both the fixture and the code under test in one frozen frame
    /// and the assertions would hold whatever the comparison did.
    fn unexpired() -> nodedb_types::Hlc {
        nodedb_types::Hlc::new(
            super::super::wall_now_ns().saturating_add(60_000_000_000),
            0,
        )
    }

    /// A lease expiry a minute in the past, in REAL wall time.
    fn expired() -> nodedb_types::Hlc {
        nodedb_types::Hlc::new(
            super::super::wall_now_ns().saturating_sub(60_000_000_000),
            0,
        )
    }

    #[tokio::test]
    async fn non_member_lease_does_not_block_drain_count() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // Holder 99 is not in the topology (crashed node): its lease must not
        // block the drain count.
        insert_lease(&state, &descriptor, 99, 1, unexpired());
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);
    }

    #[tokio::test]
    async fn expired_lease_does_not_block_drain_count() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // Holder 1 is a member but its lease is already past expiry.
        insert_lease(&state, &descriptor, 1, 1, expired());
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);
    }

    /// A lease whose expiry has passed in REAL time must stop blocking the
    /// drain even when this node's HLC has not advanced.
    ///
    /// `HlcClock::peek` returns the last HLC the clock observed and never
    /// advances on its own, so on a quiet node it sits far behind wall time —
    /// at `Hlc::ZERO` if nothing has stamped it at all. Comparing expiry
    /// against it finds every lease unexpired and reinstates the wedge. An idle
    /// cluster is exactly the case that matters: a crashed node's leases are
    /// then the only ones left, and nothing is generating HLC events.
    #[tokio::test]
    async fn expired_lease_stops_blocking_even_with_an_unadvanced_hlc() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // The HLC is untouched, so `peek()` is still at zero while wall time is
        // decades ahead of it.
        assert_eq!(
            state.hlc_clock.peek().wall_ns,
            0,
            "this test is only meaningful while the HLC has not advanced"
        );

        insert_lease(&state, &descriptor, 1, 1, expired());
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            0,
            "an expired lease must not block the drain, however stale the HLC is"
        );
    }

    /// The other direction: an HLC dragged ahead of wall time by a peer with a
    /// skewed clock must not make a LIVE lease look expired. Dropping a live
    /// hold would let the DDL proceed underneath a holder still using the
    /// descriptor — trading a wedge for a correctness bug.
    #[tokio::test]
    async fn a_live_lease_still_blocks_when_the_hlc_runs_ahead_of_wall_time() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // A peer stamps an HLC an hour ahead of real time; the local clock
        // folds it in and now reads far past every live lease's expiry.
        let skewed = nodedb_types::Hlc::new(
            super::super::wall_now_ns().saturating_add(3_600_000_000_000),
            0,
        );
        state.hlc_clock.update(skewed);
        assert!(state.hlc_clock.peek().wall_ns > super::super::wall_now_ns());

        insert_lease(&state, &descriptor, 1, 1, unexpired());
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            1,
            "a lease that is live in wall time must keep blocking the drain"
        );
    }

    #[tokio::test]
    async fn member_unexpired_lease_still_blocks_drain_count() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // A live member's unexpired lease still blocks the drain — the
        // membership/expiry filters must never mask real holds.
        insert_lease(&state, &descriptor, 1, 1, unexpired());
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 1);
    }

    /// Pins the self-drain fix: a transaction altering its own descriptor
    /// while it still holds a statement-time lease on that same descriptor
    /// (a buffered write to the collection it is altering) must not wait on
    /// its own hold — both the local refcount AND this node's own
    /// replicated cache entry are excluded once `own_holds` accounts for
    /// everything left locally.
    #[tokio::test]
    async fn own_holds_excludes_the_requesters_own_sole_local_hold() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // The requesting transaction's own statement-time hold: refcount AND
        // this node's own replicated cache entry.
        state.lease_refcount.increment(&descriptor, 1);
        insert_lease(&state, &descriptor, state.node_id, 1, unexpired());

        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            2,
            "without exclusion the requester's own hold blocks its own drain"
        );
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 1),
            0,
            "own_holds must exclude the requester's own sole local hold"
        );
    }

    /// The exclusion is precise: a DIFFERENT session's hold on the same
    /// node still blocks even after the requester's own contribution is
    /// excluded.
    #[tokio::test]
    async fn own_holds_does_not_mask_a_different_local_holder() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // Two local holds: one is the requester's own (excluded), one
        // belongs to a different session on the same node (must still
        // block).
        state.lease_refcount.increment(&descriptor, 1);
        state.lease_refcount.increment(&descriptor, 1);
        insert_lease(&state, &descriptor, state.node_id, 1, unexpired());

        assert_ne!(
            count_matching_leases(&state, &descriptor, 1, 1),
            0,
            "a different session's hold on the same descriptor must still block the drain"
        );
    }
}
