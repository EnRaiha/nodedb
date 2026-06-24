// SPDX-License-Identifier: BUSL-1.1

//! Data Plane core spawning and array catalog initialization.

use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::bridge::dispatch::{CoreChannelDataSide, Dispatcher};
use crate::bridge::quiesce::CollectionQuiesce;
use crate::control::array_catalog::ArrayCatalog;
use crate::control::metrics::SystemMetrics;
use crate::data::eventfd::EventFdNotifier;
use crate::data::runtime::{CoreCompactionConfig, spawn_core};
use crate::event::EventProducer;
use crate::storage::quarantine::QuarantineRegistry;

/// Load the persisted ND-array catalog from redb into the shared in-memory handle.
pub fn load_array_catalog(
    config: &ServerConfig,
) -> crate::control::array_catalog::ArrayCatalogHandle {
    let array_catalog = ArrayCatalog::handle();
    let catalog_path = config.catalog_path();
    match crate::control::security::catalog::SystemCatalog::open(&catalog_path) {
        Ok(catalog) => match catalog.load_all_arrays() {
            Ok(entries) => {
                let mut guard = array_catalog
                    .write()
                    .expect("array catalog lock poisoned at startup");
                for entry in entries {
                    if let Err(e) = guard.register(entry) {
                        tracing::warn!(error = %e, "failed to register array at startup");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to load _system.arrays at startup");
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "could not open system catalog to load arrays");
        }
    }
    array_catalog
}

/// Shared Arc resources passed to each Data Plane core at spawn time.
pub struct CoreSharedResources {
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
    pub quiesce: Arc<CollectionQuiesce>,
    pub hlc: Arc<nodedb_types::OrdinalClock>,
    pub array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    pub quarantine_registry: Arc<QuarantineRegistry>,
    pub system_metrics: Arc<SystemMetrics>,
    pub maintenance_budget: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
}

/// Spawn all Data Plane cores, wire dispatcher notifiers, and return core handles.
///
/// Returns `(core_handles, event_consumers)`.
pub fn spawn_data_plane_cores(
    config: &ServerConfig,
    data_sides: Vec<CoreChannelDataSide>,
    event_producers: Vec<EventProducer>,
    wal_records: Arc<[nodedb_wal::WalRecord]>,
    replay_tombstones: nodedb_wal::TombstoneSet,
    dispatcher: &mut Dispatcher,
    resources: CoreSharedResources,
) -> anyhow::Result<Vec<std::thread::JoinHandle<()>>> {
    let CoreSharedResources {
        governor,
        quiesce,
        hlc,
        array_catalog,
        quarantine_registry,
        system_metrics,
        maintenance_budget,
    } = resources;
    let num_cores = config.server.data_plane_cores;
    let compaction_cfg = CoreCompactionConfig {
        interval: config.checkpoint.compaction_interval(),
        tombstone_threshold: config.checkpoint.compaction_tombstone_threshold,
        query: config.tuning.query.clone(),
        graph: config.tuning.graph.clone(),
    };

    let mut core_handles = Vec::with_capacity(num_cores);
    let mut notifiers: Vec<(usize, EventFdNotifier)> = Vec::with_capacity(num_cores);

    for (core_id, (data_side, event_producer)) in
        data_sides.into_iter().zip(event_producers).enumerate()
    {
        let (handle, notifier) = spawn_core(
            core_id,
            data_side.request_rx,
            data_side.response_tx,
            &config.server.data_dir,
            Arc::clone(&wal_records),
            replay_tombstones.clone(),
            num_cores,
            compaction_cfg.clone(),
            Some(Arc::clone(&system_metrics)),
            Some(event_producer),
            Arc::clone(&governor),
            Some(Arc::clone(&quiesce)),
            Arc::clone(&hlc),
            Arc::clone(&array_catalog),
            Arc::clone(&quarantine_registry),
            Arc::clone(&maintenance_budget),
        )?;
        core_handles.push(handle);
        notifiers.push((core_id, notifier));
    }

    for (core_id, notifier) in &notifiers {
        dispatcher.set_notifier(*core_id, *notifier);
    }

    info!(num_cores, "data plane cores running (eventfd-driven)");
    Ok(core_handles)
}
