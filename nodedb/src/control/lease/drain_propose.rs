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

use nodedb_types::DatabaseId;
use std::time::{Duration, Instant};
use tokio::runtime::RuntimeFlavor;

use nodedb_cluster::{DescriptorId, DescriptorKind, MetadataEntry, encode_entry};
use nodedb_types::Hlc;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::rolling_upgrade::DESCRIPTOR_DRAIN_VERSION;
use crate::control::state::SharedState;
use crate::error::Error;

/// How often the drain wait loop re-polls `metadata_cache.leases`
/// to check whether the in-flight leases have drained.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Grace period added on top of the configured lease duration
/// when computing a drain entry's TTL. Prevents a drain entry
/// from expiring before the leases it's waiting on.
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
pub fn drain_for_ddl(
    shared: &SharedState,
    id: DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
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
    match poll_leases_drained(shared, &id, up_to_version, max_wait) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Timeout or other failure: emit DrainEnd explicitly
            // so the cluster isn't stuck rejecting acquires at
            // this version. Log and ignore errors from the
            // cleanup propose — the TTL on the drain entry is
            // the last line of defence.
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
) -> Result<(), Error> {
    // `block_in_place` panics on the current-thread runtime and buys
    // nothing without a worker pool to hand the parked work to, so it is
    // used only where it is both legal and meaningful. Off a multi-thread
    // runtime the loop blocks the calling thread, which is what a sync
    // caller already expects.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| {
                wait_for_lease_drain(shared, id, up_to_version, max_wait)
            })
        }
        _ => wait_for_lease_drain(shared, id, up_to_version, max_wait),
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
) -> Result<(), Error> {
    let deadline = Instant::now() + max_wait;
    loop {
        let remaining = count_matching_leases(shared, id, up_to_version);
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
fn count_matching_leases(shared: &SharedState, id: &DescriptorId, up_to_version: u64) -> usize {
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let metadata_holds = cache
        .leases
        .iter()
        .filter(|((lid, _), l)| lid == id && l.version <= up_to_version)
        .count();
    drop(cache);

    if shared.lease_refcount.current_at_or_below(id, up_to_version) == 0 {
        metadata_holds
    } else {
        metadata_holds.saturating_add(1)
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

/// For a `Put*` entry that carries `descriptor_version`, return
/// the `DescriptorId` whose drain should be implicitly cleared
/// after the entry applies. Returns `None` for variants without
/// descriptor versioning (auth, schedules, change streams, etc.).
///
/// Called from `MetadataCommitApplier::apply_host_side_effects`
/// on every node — after the `apply_to` succeeds, the applier
/// looks up the drained id via this helper and calls
/// `shared.lease_drain.install_end` on it. This is how drain
/// clears implicitly on the happy path without a second raft
/// round-trip.
pub fn descriptor_id_for_implicit_clear(entry: &CatalogEntry) -> Option<DescriptorId> {
    match entry {
        CatalogEntry::PutCollection(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Collection,
            stored.name.clone(),
        )),
        CatalogEntry::PutCollectionIfAbsent(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Collection,
            stored.name.clone(),
        )),
        CatalogEntry::PutMaterializedView(stored) => Some(DescriptorId::new(
            DatabaseId::DEFAULT.as_u64(),
            stored.tenant_id,
            DescriptorKind::MaterializedView,
            stored.name.clone(),
        )),
        CatalogEntry::PutFunction(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Function,
            stored.name.clone(),
        )),
        CatalogEntry::PutProcedure(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Procedure,
            stored.name.clone(),
        )),
        CatalogEntry::PutTrigger(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Trigger,
            stored.name.clone(),
        )),

        CatalogEntry::PutSequence(stored) => Some(DescriptorId::new(
            DatabaseId::DEFAULT.as_u64(),
            stored.tenant_id,
            DescriptorKind::Sequence,
            stored.name.clone(),
        )),
        _ => None,
    }
}

/// For a `Put*` entry that carries `descriptor_version`, return
/// `(descriptor_id, prior_persisted_version)` so the proposer can
/// decide whether to run drain. `prior_persisted_version` is `0`
/// on create (no prior record) and causes `drain_for_ddl` to
/// return immediately.
///
/// Called from `metadata_proposer::propose_catalog_entry_with_timeout`
/// BEFORE the raft propose path. Reads from `SystemCatalog` under
/// a short read txn — the read is consistent with the subsequent
/// propose because the stamp logic in the applier increments
/// from the same prior value under its own write txn.
pub fn descriptor_id_and_prior_version(
    entry: &CatalogEntry,
    shared: &SharedState,
) -> Option<(DescriptorId, u64)> {
    let catalog = shared.credentials.catalog();
    match entry {
        CatalogEntry::PutCollection(stored) => {
            let prior = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|c| c.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Collection,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutCollectionIfAbsent(stored) => {
            let prior = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|c| c.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Collection,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutMaterializedView(stored) => {
            let prior = catalog
                .get_materialized_view(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|v| v.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    DatabaseId::DEFAULT.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::MaterializedView,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutFunction(stored) => {
            let prior = catalog
                .get_function_in_database(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|f| f.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Function,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutProcedure(stored) => {
            let prior = catalog
                .get_procedure_in_database(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|p| p.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Procedure,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutTrigger(stored) => {
            let prior = catalog
                .get_trigger_in_database(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|t| t.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Trigger,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutSequence(stored) => {
            let prior = catalog
                .get_sequence(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|s| s.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    DatabaseId::DEFAULT.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Sequence,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::catalog::procedure_types::ProcedureRoutability;
    use crate::control::security::catalog::trigger_types::{
        TriggerBatchMode, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity,
        TriggerTiming,
    };
    use crate::control::security::catalog::{
        FunctionLanguage, FunctionSecurity, FunctionVolatility, StoredCollection, StoredFunction,
        StoredProcedure, StoredTrigger,
    };
    use crate::wal::WalManager;

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
        assert_eq!(count_matching_leases(&state, &descriptor, 1), 1);
        state.lease_refcount.decrement(&descriptor, 1);
        assert_eq!(count_matching_leases(&state, &descriptor, 1), 0);
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
        assert_eq!(count_matching_leases(&state, &descriptor, 1), 0);
        state.lease_refcount.decrement(&descriptor, 2);
    }

    fn function(database_id: DatabaseId) -> StoredFunction {
        StoredFunction {
            tenant_id: 41,
            database_id,
            name: "same_name".into(),
            parameters: vec![],
            return_type: "INT".into(),
            body_sql: "1".into(),
            compiled_body_sql: None,
            volatility: FunctionVolatility::Immutable,
            security: FunctionSecurity::Invoker,
            language: FunctionLanguage::Sql,
            wasm_hash: None,
            wasm_module: None,
            dependencies: vec![],
            wasm_fuel: 1_000_000,
            wasm_memory: 16 * 1024 * 1024,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    fn procedure(database_id: DatabaseId) -> StoredProcedure {
        StoredProcedure {
            tenant_id: 41,
            database_id,
            name: "same_name".into(),
            parameters: vec![],
            body_sql: "BEGIN END".into(),
            max_iterations: 1_000_000,
            timeout_secs: 60,
            routability: ProcedureRoutability::MultiCollection,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    fn trigger(database_id: DatabaseId) -> StoredTrigger {
        StoredTrigger {
            tenant_id: 41,
            database_id,
            name: "same_name".into(),
            collection: "orders".into(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: "BEGIN END".into(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::Async,
            security: TriggerSecurity::Invoker,
            batch_mode: TriggerBatchMode::BatchSafe,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn routine_descriptor_ids_preserve_selected_database() {
        let database_id = DatabaseId::new(73);
        let entries = [
            CatalogEntry::PutFunction(Box::new(function(database_id))),
            CatalogEntry::PutProcedure(Box::new(procedure(database_id))),
            CatalogEntry::PutTrigger(Box::new(trigger(database_id))),
        ];

        for entry in entries {
            let id = descriptor_id_for_implicit_clear(&entry).expect("routine descriptor id");
            assert_eq!(id.database_id, database_id.as_u64());
            assert_eq!(id.tenant_id, 41);
            assert_eq!(id.name, "same_name");
        }
    }

    #[test]
    fn implicit_clear_collection_id_preserves_non_default_database() {
        let mut stored = StoredCollection::new(41, "orders", "owner");
        stored.database_id = DatabaseId::new(73);
        let entry = CatalogEntry::PutCollection(Box::new(stored));

        let id = descriptor_id_for_implicit_clear(&entry).expect("collection descriptor id");
        assert_eq!(id.database_id, 73);
        assert_eq!(id.tenant_id, 41);
        assert_eq!(id.kind, DescriptorKind::Collection);
        assert_eq!(id.name, "orders");
    }
}
