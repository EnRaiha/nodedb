// SPDX-License-Identifier: BUSL-1.1

//! The periodic base snapshot task, and its boot wiring.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::cycle::{BaseCycle, CycleOutcome};
use super::node_life::NodeLife;
use super::pins::ColdPins;
use super::retention::list_snapshots;
use super::state::unix_now_secs;
use crate::config::server::PitrSettings;
use crate::control::server::exchange::capture_base_on_local_cores;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::startup::StartupPhase;
use crate::control::state::SharedState;
use crate::storage::snapshot::{SnapshotCatalog, SnapshotMeta};
use crate::storage::snapshot_node::capture_node_snapshot;
use crate::storage::snapshot_writer::CdcParams;

/// Wire PITR at boot: resolve this node life with the archiver's incarnation,
/// install the cluster catalog, and rebuild the base catalog, cold pins, and
/// chunk pins from the store.
///
/// A listing that fails, or a manifest that does not load, leaves the pins
/// incomplete: every cold key and chunk counts as pinned until a run lists
/// cleanly.
pub async fn wire_pitr(
    state: &SharedState,
    cluster_catalog: Arc<nodedb_cluster::ClusterCatalog>,
) -> crate::Result<()> {
    let life = NodeLife::resolve(state.node_id, state.data_dir.clone()).await?;
    state.pitr.install_cluster_catalog(cluster_catalog);
    let key = wal_key(state)?;
    let store = life.snapshot_store(&state.snapshot_storage);
    let mut catalog = SnapshotCatalog::new();
    let (pins, chunk_pins) = match list_snapshots(&store, &key).await {
        Ok(listed) => {
            let mut metas: Vec<_> = listed.bases.iter().map(|base| base.meta.clone()).collect();
            metas.sort_by_key(|meta| (meta.applied_high_lsn, meta.created_at_us));
            for meta in metas {
                catalog.add(meta);
            }
            let complete = listed.unreadable.is_empty();
            let pins = ColdPins::from_bases(
                listed
                    .bases
                    .iter()
                    .map(|base| (base.prefix.as_str(), base.cold_segments.as_slice())),
                complete,
            );
            let chunk_pins = ColdPins::from_bases(
                listed
                    .bases
                    .iter()
                    .map(|base| (base.prefix.as_str(), base.chunk_ids.as_slice())),
                complete,
            );
            (pins, chunk_pins)
        }
        Err(e) => {
            warn!(
                error = %e,
                "PITR snapshot listing failed; every cold key and chunk stays pinned"
            );
            state
                .pitr
                .record_failure(state.system_metrics.as_deref(), &e, unix_now_secs());
            (
                ColdPins::from_bases([], false),
                ColdPins::from_bases([], false),
            )
        }
    };
    info!(
        node_id = life.node_id,
        incarnation = life.incarnation.as_str(),
        snapshots = catalog.len(),
        pinned_cold_keys = pins.len(),
        pinned_chunks = chunk_pins.len(),
        "PITR base snapshot catalog rebuilt"
    );
    *state.pitr.cold_pins().write().await = pins;
    *state.pitr.chunk_pins().write().await = chunk_pins;
    state
        .pitr
        .replace_catalog(catalog, state.system_metrics.as_deref());
    state.pitr.install_life(life);
    Ok(())
}

/// Spawn the base snapshot task when PITR is on.
///
/// The first base is due one interval after the newest base in the catalog,
/// or at once when there is none. Runs start once the gateway is open, so a
/// capture never races boot recovery.
pub fn spawn_base_snapshot_task(shared: &Arc<SharedState>, settings: &PitrSettings) {
    if !settings.enabled {
        return;
    }
    let interval = settings.base_snapshot_interval();
    let retention = match settings.retention() {
        Ok(retention) => retention,
        Err(e) => {
            shared
                .pitr
                .record_failure(shared.system_metrics.as_deref(), &e, unix_now_secs());
            warn!(error = %e, "PITR base snapshots not started");
            return;
        }
    };
    let task_state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "pitr_base_snapshot",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let state = task_state;
            let ready = tokio::select! {
                ready = state.startup.await_phase(StartupPhase::GatewayEnable) => ready.is_ok(),
                _ = shutdown.wait_cancelled() => false,
            };
            if !ready {
                return;
            }
            let Some(life) = state.pitr.life().cloned() else {
                let e = crate::Error::Internal {
                    detail: "PITR is on but boot wired no node life".into(),
                };
                state
                    .pitr
                    .record_failure(state.system_metrics.as_deref(), &e, unix_now_secs());
                warn!(error = %e, "PITR base snapshots not started");
                return;
            };
            let first = first_delay(&state.pitr.catalog(), unix_now_secs(), interval);
            info!(
                interval_secs = interval.as_secs(),
                retention = retention.get(),
                first_in_secs = first.as_secs(),
                "PITR base snapshot task started"
            );
            let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + first, interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                // A failure is recorded and logged; the next tick retries.
                let _ = run_once(&state, &life, retention, interval).await;
            }
        },
    );
}

/// Take one base and record the outcome in state and metrics. Returns the
/// base.
pub(super) async fn run_once(
    state: &Arc<SharedState>,
    life: &NodeLife,
    retention: NonZeroUsize,
    interval: Duration,
) -> Result<SnapshotMeta, crate::Error> {
    let metrics = state.system_metrics.as_deref();
    let outcome = match take_base(state, life, retention, interval).await {
        Ok(outcome) => outcome,
        Err(e) => {
            warn!(error = %e, "PITR base snapshot failed");
            state.pitr.record_failure(metrics, &e, unix_now_secs());
            return Err(e);
        }
    };
    let CycleOutcome {
        base,
        uploads,
        catalog,
        deleted_bases,
        collected_chunks,
        collected_segments,
        cleanup_error,
    } = outcome;
    let now = unix_now_secs();
    let catalog = catalog.unwrap_or_else(|| {
        let mut held = state.pitr.catalog();
        held.add(base.clone());
        held
    });
    state.pitr.replace_catalog(catalog, metrics);
    state.pitr.record_success(metrics, now);
    if let Some(metrics) = metrics {
        use std::sync::atomic::Ordering::Relaxed;
        metrics
            .pitr_wal_segments_collected_total
            .fetch_add(collected_segments, Relaxed);
        metrics
            .pitr_base_chunks_uploaded_total
            .fetch_add(uploads.uploaded, Relaxed);
        metrics
            .pitr_base_chunk_bytes_uploaded_total
            .fetch_add(uploads.uploaded_bytes, Relaxed);
        metrics
            .pitr_base_chunks_reused_total
            .fetch_add(uploads.reused, Relaxed);
        metrics
            .pitr_base_chunks_collected_total
            .fetch_add(collected_chunks, Relaxed);
    }
    info!(
        snapshot_id = base.snapshot_id,
        parent_id = ?base.parent_id,
        begin_lsn = base.begin_lsn.as_u64(),
        applied_high_lsn = base.applied_high_lsn.as_u64(),
        chunks_uploaded = uploads.uploaded,
        chunks_reused = uploads.reused,
        deleted_bases,
        collected_chunks,
        collected_segments,
        "PITR base snapshot taken"
    );
    if let Some(e) = cleanup_error {
        warn!(error = %e, "PITR retention did not finish; the next run retries");
        state.pitr.record_failure(metrics, &e, now);
    }
    Ok(base)
}

/// Capture every core and the node-level stores, then run the cycle.
async fn take_base(
    state: &Arc<SharedState>,
    life: &NodeLife,
    retention: NonZeroUsize,
    interval: Duration,
) -> crate::Result<CycleOutcome> {
    let key = wal_key(state)?;
    // A base that outlasts the interval overlaps the next one.
    let deadline = std::time::Instant::now() + interval;
    let cores = capture_base_on_local_cores(state, deadline).await?;
    let node = capture_node_snapshot(
        state.data_dir.clone(),
        Arc::clone(state),
        state.pitr.cluster_catalog().cloned(),
        deadline.saturating_duration_since(std::time::Instant::now()),
    )
    .await?;
    BaseCycle {
        root: &state.snapshot_storage,
        cold: state.cold_storage.as_deref(),
        life,
        state: &state.pitr,
        encryption_key: &key,
        retention,
        chunk_params: CdcParams::DEFAULT,
    }
    .run(cores, node)
    .await
}

fn wal_key(state: &SharedState) -> crate::Result<nodedb_wal::crypto::WalEncryptionKey> {
    state
        .wal
        .encryption_key()
        .cloned()
        .ok_or_else(|| crate::Error::Config {
            detail: "PITR base snapshots are encrypted with the WAL key, and this node has \
                     none. Add [encryption] key_path or set pitr.enabled = false"
                .into(),
        })
}

/// Time until the next base is due: one interval after the newest base, or
/// zero with no base.
fn first_delay(catalog: &SnapshotCatalog, now_unix_secs: u64, interval: Duration) -> Duration {
    let Some(newest_us) = catalog.all().iter().map(|meta| meta.created_at_us).max() else {
        return Duration::ZERO;
    };
    let age = Duration::from_secs(now_unix_secs.saturating_sub(newest_us / 1_000_000));
    interval.saturating_sub(age)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::snapshot::{SNAPSHOT_FORMAT_VERSION, SnapshotKind, SnapshotMeta};
    use crate::types::Lsn;

    fn catalog_created_at(secs: &[u64]) -> SnapshotCatalog {
        let mut catalog = SnapshotCatalog::new();
        for (id, &at) in secs.iter().enumerate() {
            catalog.add(SnapshotMeta {
                format_version: SNAPSHOT_FORMAT_VERSION,
                snapshot_id: id as u64,
                begin_lsn: Lsn::new(1),
                end_lsn: Lsn::new(1),
                applied_high_lsn: Lsn::new(1),
                created_at_us: at * 1_000_000,
                created_by: "node-1".into(),
                kind: SnapshotKind::Base,
                parent_id: None,
                data_bytes: 0,
            });
        }
        catalog
    }

    #[test]
    fn with_no_base_the_first_run_is_immediate() {
        let delay = first_delay(&SnapshotCatalog::new(), 1_000, Duration::from_secs(60));
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn a_restart_keeps_the_schedule_of_the_newest_base() {
        let catalog = catalog_created_at(&[100, 940]);
        let delay = first_delay(&catalog, 1_000, Duration::from_secs(100));
        assert_eq!(delay, Duration::from_secs(40));
        let overdue = first_delay(&catalog, 5_000, Duration::from_secs(100));
        assert_eq!(overdue, Duration::ZERO);
    }
}
