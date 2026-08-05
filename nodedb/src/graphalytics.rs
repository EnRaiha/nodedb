// SPDX-License-Identifier: BUSL-1.1

//! Product-owned embedded Graphalytics runner support.

use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use nodedb_types::{DatabaseId, TenantId};
use serde_json::{Map, json};

use crate::engine::graph::algo::params::AlgoParams;
use crate::engine::graph::algo::util::cmp_desc_nan_last;
use crate::engine::graph::algo::{label_propagation, lcc, pagerank, sssp, wcc};
use crate::engine::graph::csr::rebuild::rebuild_sharded_from_store;
use crate::engine::graph::edge_store::{
    EdgeImportRecord, EdgeStore, EdgeValuePayload, NodeSurrogateRecord, versioned_edge_key,
};
use crate::graphalytics_diagnostics::LoadDiagnostics;
use crate::graphalytics_output::{write_depths, write_json_result};

const DATABASE: DatabaseId = DatabaseId::DEFAULT;
const TENANT: TenantId = TenantId::new(1);
const COLLECTION: &str = "graphalytics";
const LABEL: &str = "edge";
const SOURCE: &str = "6";
const BATCH_SIZE: usize = 10_000_000;
const OPERATION_TIMEOUT_SECONDS: f64 = 300.0;
const EDGE_STORE_CACHE_BYTES: usize = 16 * 1024 * 1024 * 1024;

pub fn run(dataset: &Path, output: &Path, database: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(output)?;
    if database.exists() {
        fs::remove_file(database)?;
    }
    let dataset_name = dataset
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("dataset path has no UTF-8 name"))?;
    let vertices = dataset.join(format!("{dataset_name}.v"));
    let edges = dataset.join(format!("{dataset_name}.e"));

    let diagnostics_path = LoadDiagnostics::enabled_path();
    let load_start = Instant::now();
    let open_started = diagnostics_path.as_ref().map(|_| Instant::now());
    let store = EdgeStore::open_with_cache_size(database, EDGE_STORE_CACHE_BYTES)?;
    let mut diagnostics = diagnostics_path
        .as_ref()
        .map(|_| LoadDiagnostics::default());
    if let (Some(diagnostics), Some(open_started)) = (diagnostics.as_mut(), open_started) {
        diagnostics.edge_store_open_seconds = open_started.elapsed().as_secs_f64();
    }
    if let Some(diagnostics) = diagnostics.as_mut() {
        import_vertices::<true>(&store, &vertices, Some(diagnostics))?;
        import_edges_profiled(&store, &edges, diagnostics)?;
    } else {
        import_vertices::<false>(&store, &vertices, None)?;
        import_edges(&store, &edges)?;
    }
    let raw_load_seconds = load_start.elapsed().as_secs_f64();
    let load_seconds = checked_elapsed(load_start, "load")?;

    let prepare_start = Instant::now();
    let sharded = rebuild_sharded_from_store(&store)?;
    let csr = sharded
        .partition(DATABASE, TENANT)
        .ok_or_else(|| anyhow::anyhow!("Graphalytics partition was not rebuilt"))?;
    let prepare_seconds = checked_elapsed(prepare_start, "prepare")?;

    let params = AlgoParams {
        source_node: Some(SOURCE.to_string()),
        max_iterations: Some(10),
        damping: Some(0.85),
        tolerance: Some(f64::MIN_POSITIVE),
        direction: Some("both".to_string()),
        ..Default::default()
    };
    let mut timings = Map::new();

    let start = Instant::now();
    let dense_ranks = pagerank::run_raw(csr, &params);
    timings.insert("PR".into(), json!(checked_elapsed(start, "PR")?));
    let mut ranks: Vec<(usize, f64)> = dense_ranks.into_iter().enumerate().collect();
    ranks.sort_by(|left, right| cmp_desc_nan_last(left.1, right.1));
    let mut result = crate::engine::graph::algo::result::AlgoResultBatch::new(
        crate::engine::graph::algo::GraphAlgorithm::PageRank,
    );
    for (node, rank) in ranks {
        result.push_node_f64(csr.node_name_raw(node as u32).to_string(), rank);
    }
    write_json_result(output, dataset_name, "PR", result.to_json()?, "rank")?;

    let start = Instant::now();
    let labels = wcc::run_raw(csr);
    timings.insert("WCC".into(), json!(checked_elapsed(start, "WCC")?));
    let mut result = crate::engine::graph::algo::result::AlgoResultBatch::new(
        crate::engine::graph::algo::GraphAlgorithm::Wcc,
    );
    for (node, label) in labels.into_iter().enumerate() {
        result.push_node_i64(csr.node_name_raw(node as u32).to_string(), label as i64);
    }
    write_json_result(
        output,
        dataset_name,
        "WCC",
        result.to_json()?,
        "component_id",
    )?;

    let start = Instant::now();
    let depths = bfs_depths(csr, SOURCE)?;
    timings.insert("BFS".into(), json!(checked_elapsed(start, "BFS")?));
    write_depths(output, dataset_name, csr, &depths)?;

    let start = Instant::now();
    let coefficients = lcc::run_raw(csr, usize::MAX, usize::MAX);
    timings.insert("LCC".into(), json!(checked_elapsed(start, "LCC")?));
    let mut result = crate::engine::graph::algo::result::AlgoResultBatch::new(
        crate::engine::graph::algo::GraphAlgorithm::Lcc,
    );
    for (node, coefficient) in coefficients.into_iter().enumerate() {
        result.push_node_f64(csr.node_name_raw(node as u32).to_string(), coefficient);
    }
    write_json_result(
        output,
        dataset_name,
        "LCC",
        result.to_json()?,
        "coefficient",
    )?;

    // Weight validity is an input invariant, not part of the primitive timing.
    sssp::validate_weights(csr)?;
    let start = Instant::now();
    let source = csr
        .node_id_raw(SOURCE)
        .ok_or_else(|| anyhow::anyhow!("source vertex {SOURCE} is absent"))?;
    let distances = sssp::run_raw_validated(csr, source, &params);
    timings.insert("SSSP".into(), json!(checked_elapsed(start, "SSSP")?));
    let mut result = crate::engine::graph::algo::result::AlgoResultBatch::new(
        crate::engine::graph::algo::GraphAlgorithm::Sssp,
    );
    for (node, distance) in distances.into_iter().enumerate() {
        result.push_node_f64(csr.node_name_raw(node as u32).to_string(), distance);
    }
    write_json_result(output, dataset_name, "SSSP", result.to_json()?, "distance")?;

    let start = Instant::now();
    let communities = label_propagation::run_raw(csr, &params);
    timings.insert("CDLP".into(), json!(checked_elapsed(start, "CDLP")?));
    let mut result = crate::engine::graph::algo::result::AlgoResultBatch::new(
        crate::engine::graph::algo::GraphAlgorithm::LabelPropagation,
    );
    for (node, community) in communities.into_iter().enumerate() {
        result.push_node_i64(csr.node_name_raw(node as u32).to_string(), community);
    }
    write_json_result(
        output,
        dataset_name,
        "CDLP",
        result.to_json()?,
        "community_id",
    )?;

    let summary = json!({
        "system": "NodeDB Origin Embedded",
        "dataset": dataset_name,
        "load_seconds": load_seconds,
        "prepare_seconds": prepare_seconds,
        "algorithms": timings,
    });
    fs::write(
        output.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    // The canonical summary is already durable; an opt-in sidecar failure is
    // deliberately reported afterward and never changes measured durations.
    if let (Some(path), Some(diagnostics)) = (diagnostics_path.as_deref(), diagnostics.as_ref()) {
        let database_bytes = fs::metadata(database).ok().map(|metadata| metadata.len());
        diagnostics.write(
            path,
            dataset_name,
            raw_load_seconds,
            load_seconds,
            database_bytes,
        )?;
    }
    Ok(())
}

fn checked_elapsed(start: Instant, operation: &str) -> anyhow::Result<f64> {
    let seconds = start.elapsed().as_secs_f64();
    if seconds > OPERATION_TIMEOUT_SECONDS {
        anyhow::bail!(
            "{operation} took {seconds:.3}s and exceeded the {OPERATION_TIMEOUT_SECONDS:.0}-second operation timeout"
        );
    }
    Ok(seconds)
}

fn import_vertices<const PROFILED: bool>(
    store: &EdgeStore,
    path: &Path,
    diagnostics: Option<&mut LoadDiagnostics>,
) -> anyhow::Result<()> {
    let started = PROFILED.then(Instant::now);
    let reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut batch: Vec<NodeSurrogateRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut surrogate = 1u32;
    for line in reader.lines() {
        let node = line?.trim().to_string();
        if node.is_empty() {
            continue;
        }
        batch.push((DATABASE, TENANT, node, surrogate));
        surrogate = surrogate
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("dataset exceeds the u32 node identity domain"))?;
        if batch.len() == BATCH_SIZE {
            store.import_node_surrogates(&batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.import_node_surrogates(&batch)?;
    }
    if PROFILED {
        let diagnostics = diagnostics.expect("profiled vertex import requires diagnostics");
        diagnostics.vertex_count = surrogate.saturating_sub(1) as usize;
        diagnostics.vertex_parse_write_seconds = started
            .expect("profiled vertex import has a clock")
            .elapsed()
            .as_secs_f64();
    }
    Ok(())
}

fn import_edges(store: &EdgeStore, path: &Path) -> anyhow::Result<()> {
    import_edges_with_batch_size::<false>(store, path, BATCH_SIZE, None)
}

fn import_edges_profiled(
    store: &EdgeStore,
    path: &Path,
    diagnostics: &mut LoadDiagnostics,
) -> anyhow::Result<()> {
    import_edges_with_batch_size::<true>(store, path, BATCH_SIZE, Some(diagnostics))
}

#[derive(Default)]
struct EdgeImportMeasurements {
    producer_wall_seconds: f64,
    producer_active_seconds: f64,
    edge_count: usize,
    encoded_value_bytes: u64,
    storage: crate::engine::graph::edge_store::snapshot::DeferredImportProfile,
}

fn import_edges_with_batch_size<const PROFILED: bool>(
    store: &EdgeStore,
    path: &Path,
    batch_size: usize,
    diagnostics: Option<&mut LoadDiagnostics>,
) -> anyhow::Result<()> {
    enum ImportMessage {
        Batch(Vec<EdgeImportRecord>),
        Finish,
    }

    debug_assert_eq!(PROFILED, diagnostics.is_some());
    let pipeline_started = PROFILED.then(Instant::now);
    assert!(batch_size > 0, "edge import batch size must be positive");
    let measurements = std::thread::scope(|scope| {
        let (work_tx, work_rx) = std::sync::mpsc::sync_channel(1);
        let (reuse_tx, reuse_rx) = std::sync::mpsc::sync_channel(1);
        let importer = scope.spawn(move || -> anyhow::Result<_> {
            let mut storage =
                crate::engine::graph::edge_store::snapshot::DeferredImportProfile::default();
            while let Ok(message) = work_rx.recv() {
                match message {
                    ImportMessage::Batch(mut batch) => {
                        if PROFILED {
                            storage.merge(store.import_edge_pairs_deferred_profiled(&mut batch)?);
                        } else {
                            store.import_edge_pairs_deferred(&mut batch)?;
                        }
                        batch.clear();
                        // One returned buffer is enough to keep the producer
                        // pipelined. Drop extras instead of blocking after the
                        // producer has submitted its final partial batch.
                        let _ = reuse_tx.try_send(batch);
                    }
                    ImportMessage::Finish => {
                        if PROFILED {
                            storage.merge(store.flush_deferred_imports_profiled()?);
                        } else {
                            store.flush_deferred_imports()?;
                        }
                        return Ok(storage);
                    }
                }
            }
            Ok(storage)
        });

        let producer = (|| -> anyhow::Result<EdgeImportMeasurements> {
            let producer_started = PROFILED.then(Instant::now);
            let reader = BufReader::with_capacity(1 << 20, File::open(path)?);
            let mut batch: Vec<EdgeImportRecord> = Vec::with_capacity(batch_size);
            let mut spare = Some(Vec::with_capacity(batch_size));
            let mut last_system_from = 0i64;
            let mut measurements = EdgeImportMeasurements::default();
            let mut batch_started = PROFILED.then(Instant::now);
            for line in reader.lines() {
                let line = line?;
                let mut fields = line.split_whitespace();
                let source = fields
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing edge source"))?;
                let destination = fields
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing edge destination"))?;
                let weight: f64 = fields
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing edge weight"))?
                    .parse()?;
                if !weight.is_finite() || weight < 0.0 {
                    anyhow::bail!("invalid edge weight {weight}");
                }
                if fields.next().is_some() {
                    anyhow::bail!("edge has extra fields: {line}");
                }

                let system_from = last_system_from.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("dataset exceeds the nonnegative i64 edge-version domain")
                })?;
                last_system_from = system_from;
                let mut properties = Vec::with_capacity(18);
                nodedb_query::msgpack_scan::write_map_header(&mut properties, 1);
                nodedb_query::msgpack_scan::write_kv_f64(&mut properties, "weight", weight);
                let value = EdgeValuePayload::new(0, i64::MAX, properties).encode()?;
                if PROFILED {
                    measurements.edge_count += 1;
                    measurements.encoded_value_bytes += value.len() as u64;
                }
                batch.push((
                    DATABASE,
                    TENANT,
                    versioned_edge_key(COLLECTION, source, LABEL, destination, system_from)?,
                    versioned_edge_key(COLLECTION, destination, LABEL, source, system_from)?,
                    value,
                ));
                if batch.len() == batch_size {
                    if let Some(started) = batch_started {
                        measurements.producer_active_seconds += started.elapsed().as_secs_f64();
                    }
                    work_tx
                        .send(ImportMessage::Batch(batch))
                        .map_err(|_| anyhow::anyhow!("edge import worker stopped"))?;
                    batch = if let Some(spare) = spare.take() {
                        spare
                    } else {
                        reuse_rx
                            .recv()
                            .map_err(|_| anyhow::anyhow!("edge import worker stopped"))?
                    };
                    batch_started = PROFILED.then(Instant::now);
                }
            }
            if !batch.is_empty() {
                if let Some(started) = batch_started {
                    measurements.producer_active_seconds += started.elapsed().as_secs_f64();
                }
                work_tx
                    .send(ImportMessage::Batch(batch))
                    .map_err(|_| anyhow::anyhow!("edge import worker stopped"))?;
            }
            work_tx
                .send(ImportMessage::Finish)
                .map_err(|_| anyhow::anyhow!("edge import worker stopped"))?;
            if let Some(started) = producer_started {
                measurements.producer_wall_seconds = started.elapsed().as_secs_f64();
            }
            Ok(measurements)
        })();
        drop(work_tx);

        let imported = importer
            .join()
            .map_err(|_| anyhow::anyhow!("edge import worker panicked"))?;
        match (producer, imported) {
            (_, Err(error)) => Err(error),
            (Err(error), _) => Err(error),
            (Ok(mut measurements), Ok(storage)) => {
                measurements.storage = storage;
                Ok(measurements)
            }
        }
    })?;

    if let (Some(diagnostics), Some(started)) = (diagnostics, pipeline_started) {
        diagnostics.producer_wall_seconds = measurements.producer_wall_seconds;
        diagnostics.producer_batch_active_seconds = measurements.producer_active_seconds;
        diagnostics.pipeline_wall_seconds = started.elapsed().as_secs_f64();
        diagnostics.edge_count = measurements.edge_count;
        diagnostics.encoded_value_bytes = measurements.encoded_value_bytes;
        diagnostics.deferred_import = measurements.storage;
    }
    Ok(())
}

fn bfs_depths(csr: &nodedb_graph::CsrIndex, source: &str) -> anyhow::Result<Vec<i64>> {
    let start = csr
        .node_id_raw(source)
        .ok_or_else(|| anyhow::anyhow!("source vertex {source} is absent"))?;
    Ok(csr.bfs_both_distances_raw(start))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipelined_edge_import_crosses_batches_and_reopens_durably() {
        let dir = tempfile::tempdir().unwrap();
        for (case, contents, expected) in [
            ("exact", "1 2 1.0\n2 3 2.0\n3 4 3.0\n4 1 4.0\n", 4),
            (
                "partial",
                "1 2 1.0\n2 3 2.0\n3 4 3.0\n4 5 4.0\n5 1 5.0\n",
                5,
            ),
        ] {
            let input = dir.path().join(format!("{case}.e"));
            fs::write(&input, contents).unwrap();
            let database = dir.path().join(format!("{case}.redb"));
            {
                let store = EdgeStore::open(&database).unwrap();
                import_edges_with_batch_size::<false>(&store, &input, 2, None).unwrap();
            }

            let reopened = EdgeStore::open(&database).unwrap();
            let edges = reopened.scan_all_edges_decoded(None).unwrap();
            assert_eq!(edges.len(), expected);
            assert_eq!(
                reopened
                    .neighbors_in(0, TENANT, COLLECTION, "1", Some(LABEL))
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    #[test]
    fn pipelined_edge_import_returns_after_producer_error() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("invalid.e");
        fs::write(&input, "1 2 1.0\n2 3 2.0\nmalformed\n").unwrap();
        let store = EdgeStore::open(&dir.path().join("invalid.redb")).unwrap();
        let error = import_edges_with_batch_size::<false>(&store, &input, 2, None).unwrap_err();
        assert!(error.to_string().contains("missing edge destination"));
    }
}
