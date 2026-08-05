// SPDX-License-Identifier: BUSL-1.1

//! Opt-in load-stage diagnostics for the embedded Graphalytics runner.

use std::fs;
use std::path::Path;

use serde::Serialize;

use crate::engine::graph::edge_store::snapshot::DeferredImportProfile;

pub(super) const ENVIRONMENT_VARIABLE: &str = "NODEDB_GRAPHALYTICS_DIAGNOSTICS";
const FORMAT_VERSION: u32 = 1;

#[derive(Default)]
pub(super) struct LoadDiagnostics {
    pub(super) edge_store_open_seconds: f64,
    pub(super) vertex_parse_write_seconds: f64,
    pub(super) vertex_count: usize,
    pub(super) producer_wall_seconds: f64,
    pub(super) producer_batch_active_seconds: f64,
    pub(super) pipeline_wall_seconds: f64,
    pub(super) edge_count: usize,
    pub(super) encoded_value_bytes: u64,
    pub(super) deferred_import: DeferredImportProfile,
}

impl LoadDiagnostics {
    pub(super) fn enabled_path() -> Option<std::path::PathBuf> {
        std::env::var_os(ENVIRONMENT_VARIABLE).map(std::path::PathBuf::from)
    }

    pub(super) fn write(
        &self,
        path: &Path,
        dataset: &str,
        raw_wall_seconds: f64,
        classified_load_seconds: f64,
        database_bytes: Option<u64>,
    ) -> anyhow::Result<()> {
        let classified_stages_seconds = self.edge_store_open_seconds
            + self.vertex_parse_write_seconds
            + self.pipeline_wall_seconds;
        let sidecar = Sidecar {
            format_version: FORMAT_VERSION,
            system: "nodedb-origin",
            dataset,
            source_revision: option_env!("GIT_COMMIT"),
            durability: Durability {
                mode: "deferred-transaction-commits-with-final-barrier",
                crash_atomic_publication: false,
            },
            load: Load {
                raw_wall_seconds,
                classified_load_seconds,
                classified_stages_seconds,
                residual_seconds: classified_load_seconds - classified_stages_seconds,
            },
            stages: Stages {
                edge_store_open_seconds: self.edge_store_open_seconds,
                vertex_parse_write_seconds: self.vertex_parse_write_seconds,
                producer_wall_seconds: self.producer_wall_seconds,
                producer_batch_active_seconds: self.producer_batch_active_seconds,
                pipeline_wall_seconds: self.pipeline_wall_seconds,
                reverse_sort_seconds: self.deferred_import.reverse_sort_seconds,
                forward_insert_seconds: self.deferred_import.forward_insert_seconds,
                reverse_insert_seconds: self.deferred_import.reverse_insert_seconds,
                deferred_commit_seconds: self.deferred_import.deferred_commit_seconds,
                final_durability_barrier_seconds: self
                    .deferred_import
                    .final_durability_barrier_seconds,
            },
            counts: Counts {
                vertices: self.vertex_count as u64,
                edges: self.edge_count as u64,
                forward_records: self.deferred_import.forward_records,
                reverse_records: self.deferred_import.reverse_records,
            },
            storage: Storage {
                encoded_value_bytes: self.encoded_value_bytes,
                forward_value_bytes: self.deferred_import.forward_value_bytes,
                reverse_value_bytes: self.deferred_import.reverse_value_bytes,
                database_bytes,
            },
            unsupported: Unsupported::default(),
        };
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, sonic_rs::to_vec(&sidecar)?)?;
        Ok(())
    }
}

#[derive(Serialize)]
struct Sidecar<'a> {
    format_version: u32,
    system: &'a str,
    dataset: &'a str,
    source_revision: Option<&'static str>,
    durability: Durability,
    load: Load,
    stages: Stages,
    counts: Counts,
    storage: Storage,
    unsupported: Unsupported,
}

#[derive(Serialize)]
struct Durability {
    mode: &'static str,
    crash_atomic_publication: bool,
}

#[derive(Serialize)]
struct Load {
    raw_wall_seconds: f64,
    classified_load_seconds: f64,
    classified_stages_seconds: f64,
    residual_seconds: f64,
}

#[derive(Serialize)]
struct Stages {
    edge_store_open_seconds: f64,
    vertex_parse_write_seconds: f64,
    producer_wall_seconds: f64,
    producer_batch_active_seconds: f64,
    pipeline_wall_seconds: f64,
    reverse_sort_seconds: f64,
    forward_insert_seconds: f64,
    reverse_insert_seconds: f64,
    deferred_commit_seconds: f64,
    final_durability_barrier_seconds: f64,
}

#[derive(Serialize)]
struct Counts {
    vertices: u64,
    edges: u64,
    forward_records: u64,
    reverse_records: u64,
}

#[derive(Serialize)]
struct Storage {
    encoded_value_bytes: u64,
    forward_value_bytes: u64,
    reverse_value_bytes: u64,
    database_bytes: Option<u64>,
}

#[derive(Default, Serialize)]
struct Unsupported {
    peak_rss_bytes: Option<u64>,
    peak_open_file_descriptors: Option<u64>,
    storage_page_write_seconds: Option<f64>,
    storage_sync_count: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidecar_uses_shared_schema_and_creates_parent_directory() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/diagnostics.json");
        let diagnostics = LoadDiagnostics {
            edge_store_open_seconds: 0.25,
            vertex_count: 3,
            edge_count: 5,
            encoded_value_bytes: 17,
            deferred_import: DeferredImportProfile {
                forward_records: 5,
                reverse_records: 5,
                forward_value_bytes: 17,
                ..Default::default()
            },
            ..Default::default()
        };

        diagnostics
            .write(&path, "fixture", 3.0, 2.0, Some(42))
            .unwrap();
        let sidecar: serde_json::Value = sonic_rs::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(sidecar["format_version"], FORMAT_VERSION);
        assert_eq!(sidecar["system"], "nodedb-origin");
        assert_eq!(sidecar["dataset"], "fixture");
        assert_eq!(sidecar["counts"]["vertices"], 3);
        assert_eq!(sidecar["counts"]["edges"], 5);
        assert_eq!(sidecar["counts"]["forward_records"], 5);
        assert_eq!(sidecar["storage"]["database_bytes"], 42);
        assert!(sidecar["unsupported"]["storage_sync_count"].is_null());
    }
}
