// SPDX-License-Identifier: BUSL-1.1

//! Graphalytics text-result materialization outside algorithm timing.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use serde_json::Value;

pub(super) fn write_json_result(
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

pub(super) fn write_depths(
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
