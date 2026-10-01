// SPDX-License-Identifier: BUSL-1.1

//! PITR state shared across the Control Plane: this node life, the base
//! snapshot catalog, the cold keys and chunk ids kept bases pin, and the
//! outcome of the last base snapshot run.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use super::node_life::NodeLife;
use super::pins::ColdPins;
use super::restore_point::RecordedCut;
use crate::control::metrics::SystemMetrics;
use crate::storage::snapshot::SnapshotCatalog;

/// The last base snapshot run that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PitrFailure {
    pub at_unix_secs: u64,
    pub detail: String,
}

#[derive(Default)]
pub struct PitrState {
    /// Set at boot when PITR is on.
    life: OnceLock<NodeLife>,
    /// Set at boot on a cluster node. A base captures it beside the system catalog.
    cluster_catalog: OnceLock<Arc<nodedb_cluster::ClusterCatalog>>,
    /// Bases of this node life that retention kept.
    catalog: RwLock<SnapshotCatalog>,
    /// Cold keys the kept bases reference. Async so a deleter holds it across
    /// its check and its delete.
    cold_pins: tokio::sync::RwLock<ColdPins>,
    /// Chunk ids the kept bases and the bases in progress list.
    chunk_pins: tokio::sync::RwLock<ColdPins>,
    last_failure: Mutex<Option<PitrFailure>>,
    /// The cut barriers the system catalog held at boot.
    recorded_cuts: OnceLock<Vec<RecordedCut>>,
    /// Each group's durable applied index, as this life saved it.
    durable_applied: Mutex<HashMap<u64, u64>>,
}

impl PitrState {
    /// Install this node life. A second install keeps the first.
    pub fn install_life(&self, life: NodeLife) {
        let _ = self.life.set(life);
    }

    pub fn life(&self) -> Option<&NodeLife> {
        self.life.get()
    }

    /// Install the cluster catalog. A second install keeps the first.
    pub fn install_cluster_catalog(&self, catalog: Arc<nodedb_cluster::ClusterCatalog>) {
        let _ = self.cluster_catalog.set(catalog);
    }

    pub fn cluster_catalog(&self) -> Option<&Arc<nodedb_cluster::ClusterCatalog>> {
        self.cluster_catalog.get()
    }

    /// Install the cut barriers read from the system catalog at boot. A
    /// second install keeps the first.
    pub fn install_recorded_cuts(&self, cuts: Vec<RecordedCut>) {
        let _ = self.recorded_cuts.set(cuts);
    }

    /// The cut barriers the system catalog held at boot.
    pub fn recorded_cuts(&self) -> &[RecordedCut] {
        self.recorded_cuts.get().map_or(&[], Vec::as_slice)
    }

    /// Note that `group_id`'s durable applied index reached `index`.
    pub fn note_durable_applied(&self, group_id: u64, index: u64) {
        let mut held = self
            .durable_applied
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let slot = held.entry(group_id).or_insert(0);
        *slot = (*slot).max(index);
    }

    /// `group_id`'s durable applied index as this life saved it, `0` before
    /// the first save.
    pub fn durable_applied(&self, group_id: u64) -> u64 {
        self.durable_applied
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&group_id)
            .copied()
            .unwrap_or(0)
    }

    /// Every cold object delete or overwrite holds the read lock across its
    /// pin check and the operation. A base pins its keys under the write lock.
    pub fn cold_pins(&self) -> &tokio::sync::RwLock<ColdPins> {
        &self.cold_pins
    }

    /// Every chunk delete holds the read lock across its pin check and the
    /// delete. A base pins its chunk ids under the write lock before it
    /// checks which chunks the store holds.
    pub fn chunk_pins(&self) -> &tokio::sync::RwLock<ColdPins> {
        &self.chunk_pins
    }

    /// A copy of the catalog, for restore planning.
    pub fn catalog(&self) -> SnapshotCatalog {
        self.catalog
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Replace the catalog and publish its base count.
    pub fn replace_catalog(&self, catalog: SnapshotCatalog, metrics: Option<&SystemMetrics>) {
        let bases = base_count(&catalog);
        *self.catalog.write().unwrap_or_else(|p| p.into_inner()) = catalog;
        if let Some(metrics) = metrics {
            metrics.pitr_base_snapshots.store(bases, Ordering::Relaxed);
        }
    }

    pub fn last_failure(&self) -> Option<PitrFailure> {
        self.last_failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Record a base that completed at `at_unix_secs`.
    pub fn record_success(&self, metrics: Option<&SystemMetrics>, at_unix_secs: u64) {
        if let Some(metrics) = metrics {
            metrics
                .pitr_base_last_success_timestamp_seconds
                .store(at_unix_secs, Ordering::Relaxed);
        }
    }

    /// Record a run that failed at `at_unix_secs`.
    pub fn record_failure(
        &self,
        metrics: Option<&SystemMetrics>,
        error: &crate::Error,
        at_unix_secs: u64,
    ) {
        *self.last_failure.lock().unwrap_or_else(|p| p.into_inner()) = Some(PitrFailure {
            at_unix_secs,
            detail: error.to_string(),
        });
        if let Some(metrics) = metrics {
            metrics
                .pitr_base_last_failure_timestamp_seconds
                .store(at_unix_secs, Ordering::Relaxed);
            metrics
                .pitr_base_failures_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Every snapshot is a full base, incremental ones included.
fn base_count(catalog: &SnapshotCatalog) -> u64 {
    catalog.len() as u64
}

/// Seconds since the Unix epoch.
pub fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::snapshot::{SNAPSHOT_FORMAT_VERSION, SnapshotKind, SnapshotMeta};
    use crate::types::Lsn;

    fn base(id: u64) -> SnapshotMeta {
        SnapshotMeta {
            format_version: SNAPSHOT_FORMAT_VERSION,
            snapshot_id: id,
            begin_lsn: Lsn::new(id),
            end_lsn: Lsn::new(id),
            applied_high_lsn: Lsn::new(id),
            created_at_us: id,
            created_by: "node-1".into(),
            kind: SnapshotKind::Base,
            parent_id: None,
            data_bytes: 0,
        }
    }

    #[test]
    fn metrics_follow_the_catalog_and_every_outcome() {
        let state = PitrState::default();
        let metrics = SystemMetrics::new();
        let mut catalog = SnapshotCatalog::new();
        catalog.add(base(1));
        catalog.add(base(2));
        state.replace_catalog(catalog, Some(&metrics));
        assert_eq!(metrics.pitr_base_snapshots.load(Ordering::Relaxed), 2);
        assert_eq!(state.catalog().len(), 2);

        state.record_success(Some(&metrics), 100);
        assert_eq!(
            metrics
                .pitr_base_last_success_timestamp_seconds
                .load(Ordering::Relaxed),
            100
        );

        let error = crate::Error::Config {
            detail: "no key".into(),
        };
        state.record_failure(Some(&metrics), &error, 200);
        let failure = state.last_failure().unwrap();
        assert_eq!(failure.at_unix_secs, 200);
        assert!(failure.detail.contains("no key"), "{}", failure.detail);
        assert_eq!(metrics.pitr_base_failures_total.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics
                .pitr_base_last_failure_timestamp_seconds
                .load(Ordering::Relaxed),
            200
        );
    }
}
