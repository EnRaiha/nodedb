// SPDX-License-Identifier: BUSL-1.1

//! Product-owned embedded Graphalytics runner support.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use nodedb_types::{DatabaseId, TenantId};
use serde_json::{Map, Value, json};

use crate::engine::graph::algo::{label_propagation, lcc, pagerank, sssp, wcc};
use crate::engine::graph::algo::params::AlgoParams;
use crate::engine::graph::csr::rebuild::rebuild_sharded_from_store;
use crate::engine::graph::edge_store::{
    EdgeImportRecord, EdgeStore, EdgeValuePayload, NodeSurrogateRecord, versioned_edge_key,
};

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

    let load_start = Instant::now();
    let store = EdgeStore::open_with_cache_size(database, EDGE_STORE_CACHE_BYTES)?;
    import_vertices(&store, &vertices)?;
    import_edges(&store, &edges)?;
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
    let result = pagerank::run(csr, &params);
    timings.insert("PR".into(), json!(checked_elapsed(start, "PR")?));
    write_json_result(output, dataset_name, "PR", result.to_json()?, "rank")?;

    let start = Instant::now();
    let result = wcc::run(csr);
    timings.insert("WCC".into(), json!(checked_elapsed(start, "WCC")?));
    write_json_result(output, dataset_name, "WCC", result.to_json()?, "component_id")?;

    let start = Instant::now();
    let depths = bfs_depths(csr, SOURCE)?;
    timings.insert("BFS".into(), json!(checked_elapsed(start, "BFS")?));
    write_depths(output, dataset_name, csr, &depths)?;

    let start = Instant::now();
    let result = lcc::run(csr, usize::MAX, usize::MAX);
    timings.insert("LCC".into(), json!(checked_elapsed(start, "LCC")?));
    write_json_result(output, dataset_name, "LCC", result.to_json()?, "coefficient")?;

    let start = Instant::now();
    let result = sssp::run(csr, &params)?;
    timings.insert("SSSP".into(), json!(checked_elapsed(start, "SSSP")?));
    write_json_result(output, dataset_name, "SSSP", result.to_json()?, "distance")?;

    let start = Instant::now();
    let result = label_propagation::run(csr, &params);
    timings.insert("CDLP".into(), json!(checked_elapsed(start, "CDLP")?));
    write_json_result(output, dataset_name, "CDLP", result.to_json()?, "community_id")?;

    let summary = json!({
        "system": "NodeDB Origin Embedded",
        "dataset": dataset_name,
        "load_seconds": load_seconds,
        "prepare_seconds": prepare_seconds,
        "algorithms": timings,
    });
    fs::write(output.join("summary.json"), serde_json::to_vec_pretty(&summary)?)?;
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

fn import_vertices(store: &EdgeStore, path: &Path) -> anyhow::Result<()> {
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
    Ok(())
}

fn import_edges(store: &EdgeStore, path: &Path) -> anyhow::Result<()> {
    let reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut batch: Vec<EdgeImportRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut system_from = 1i64;
    for line in reader.lines() {
        let line = line?;
        let mut fields = line.split_whitespace();
        let source = fields.next().ok_or_else(|| anyhow::anyhow!("missing edge source"))?;
        let destination = fields.next().ok_or_else(|| anyhow::anyhow!("missing edge destination"))?;
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

        let mut properties = Vec::with_capacity(18);
        nodedb_query::msgpack_scan::write_map_header(&mut properties, 1);
        nodedb_query::msgpack_scan::write_kv_f64(&mut properties, "weight", weight);
        let value = EdgeValuePayload::new(0, i64::MAX, properties).encode()?;
        batch.push((
            DATABASE,
            TENANT,
            versioned_edge_key(COLLECTION, source, LABEL, destination, system_from)?,
            versioned_edge_key(COLLECTION, destination, LABEL, source, system_from)?,
            value,
        ));
        system_from += 1;
        if batch.len() == BATCH_SIZE {
            store.import_edge_pairs_deferred(&mut batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.import_edge_pairs_deferred(&mut batch)?;
    }
    store.flush_deferred_imports()?;
    Ok(())
}

fn bfs_depths(csr: &nodedb_graph::CsrIndex, source: &str) -> anyhow::Result<Vec<i64>> {
    let start = csr
        .node_id_raw(source)
        .ok_or_else(|| anyhow::anyhow!("source vertex {source} is absent"))?;
    let mut depths = vec![-1; csr.node_count()];
    depths[start as usize] = 0;
    let mut queue = VecDeque::from([start]);
    while let Some(node) = queue.pop_front() {
        let next_depth = depths[node as usize] + 1;
        for (_, neighbor) in csr
            .iter_out_edges_raw(node)
            .chain(csr.iter_in_edges_raw(node))
        {
            if depths[neighbor as usize] == -1 {
                depths[neighbor as usize] = next_depth;
                queue.push_back(neighbor);
            }
        }
    }
    Ok(depths)
}

fn write_json_result(
    output: &Path,
    dataset: &str,
    algorithm: &str,
    bytes: Vec<u8>,
    value_key: &str,
) -> anyhow::Result<()> {
    let rows: Vec<Value> = serde_json::from_slice(&bytes)?;
    let mut writer = BufWriter::new(File::create(output.join(format!("{dataset}-{algorithm}")))?);
    for row in rows {
        let node = row["node_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("algorithm result lacks node_id"))?;
        let value = &row[value_key];
        if value.is_null() {
            writeln!(writer, "{node} Infinity")?;
        } else if let Some(number) = value.as_f64() {
            writeln!(writer, "{node} {number}")?;
        } else if let Some(number) = value.as_i64() {
            writeln!(writer, "{node} {number}")?;
        } else {
            anyhow::bail!("algorithm result has invalid {value_key}: {value}");
        }
    }
    Ok(())
}

fn write_depths(
    output: &Path,
    dataset: &str,
    csr: &nodedb_graph::CsrIndex,
    depths: &[i64],
) -> anyhow::Result<()> {
    let mut writer = BufWriter::new(File::create(output.join(format!("{dataset}-BFS")))?);
    for (node, depth) in depths.iter().enumerate() {
        writeln!(writer, "{} {depth}", csr.node_name_raw(node as u32))?;
    }
    Ok(())
}
