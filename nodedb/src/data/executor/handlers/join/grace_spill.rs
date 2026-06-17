// SPDX-License-Identifier: BUSL-1.1

//! Streaming partition-spiller for the grace-hash join.
//!
//! Where [`super::grace_partitioner::grace_join_in_memory`] takes OWNED Vecs of
//! every row (whole build + probe side in RAM at once), this module bounds
//! memory: rows are fed ONE AT A TIME via [`PartitionedSpiller::push_build`] /
//! [`PartitionedSpiller::push_probe`], and the moment a partition's in-memory
//! buffer exceeds `per_partition_budget` it is drained to a
//! [`SpillPartitionWriter`] on disk. From that point the partition's rows live
//! on NVMe, never in RAM, so the full build/probe side is NEVER all resident.
//!
//! `finish_and_probe` reads each spilled partition back via [`UringReader`],
//! reconstructs `(id, value)` docs (the id is always empty — see below), builds
//! a per-partition [`HashIndex`], and probes it. Results are unioned and the
//! GLOBAL `limit` is applied ONCE at the end — identical to the in-memory
//! reference.
//!
//! ## Why partitioning is correct
//!
//! Identical reasoning to `grace_partitioner`: [`partition_hash`] hashes the
//! SAME extracted key value bytes that [`HashIndex`] memcmp-matches on, with a
//! fixed seed, so any pair that *could* match co-locates in one partition.
//! Cross / keyless joins are cartesian products and run as a single partition.
//!
//! ## Why the doc id is dropped
//!
//! Join output is produced by `merge_join_docs_binary`, which only ever reads
//! the VALUE bytes of each side — the `String` id is never emitted. Storing
//! `(String::new(), value)` therefore loses nothing and avoids framing the id
//! through every spill file.
//!
//! ## Platform fallback
//!
//! Spilling requires io_uring ([`SpillPartitionWriter::create`] returns `None`
//! off-Linux or when uring is unavailable). When `create` returns `None` the
//! partition simply stays in memory: correctness is preserved, only the
//! memory bound is lost. This is a platform capability gap, not an error.

use std::path::PathBuf;

use super::grace_partitioner::{GraceSpec, partition_hash};
use super::hash::{HashIndex, ProbeParams, probe_hash_index};
use super::spill::{SpillPartitionWriter, parse_framed_rows};
use crate::data::io::uring_reader::UringReader;

/// io_uring read budget per spill file (matches `spill.rs`'s round-trip tests).
const SPILL_READ_QUEUE_DEPTH: u32 = 8;
const SPILL_READ_CONCURRENCY: usize = 4;
const SPILL_READ_MAX_BYTES: usize = 8 * 1024 * 1024;

/// One join side's streaming partition state: P in-memory buffers, their running
/// byte totals, and a lazily-created spill writer per partition.
///
/// A partition is "in-memory" while its `spiller` is `None` and "spilled" once
/// the writer is `Some` — after which `buffers[p]` is empty and all further rows
/// for that partition append straight to disk.
struct SideState {
    /// Per-partition in-memory rows: `(empty id, value bytes)`.
    buffers: Vec<Vec<(String, Vec<u8>)>>,
    /// Per-partition running in-memory byte total (value bytes only).
    bytes: Vec<usize>,
    /// Per-partition spill writer — `None` until the partition starts spilling.
    spillers: Vec<Option<SpillPartitionWriter>>,
}

impl SideState {
    fn new(partitions: usize) -> Self {
        Self {
            buffers: vec![vec![]; partitions],
            bytes: vec![0; partitions],
            // `SpillPartitionWriter` is not `Clone`, so `vec![None; n]` won't
            // compile here — build the Vec with an iterator instead.
            spillers: (0..partitions).map(|_| None).collect(),
        }
    }
}

/// Streaming grace-hash partition spiller.
///
/// `!Send` — holds [`SpillPartitionWriter`]s, which wrap `!Send` / TPC-owned
/// io_uring writers. Lives entirely inside one Data-Plane core.
// Consumed by the grace-join spill integration (build-side stream + over-budget spill).
#[allow(dead_code)]
pub(super) struct PartitionedSpiller {
    /// Number of partitions (P ≥ 1; forced to 1 for cross / keyless joins).
    partitions: usize,
    /// In-memory buffer spills once it exceeds this many bytes. `0` = never
    /// spill (pure in-memory path, e.g. for non-Linux or tiny inputs).
    per_partition_budget: usize,
    /// Directory spill files are written into.
    spill_dir: PathBuf,
    /// Build-side join-key fields (owned; passed directly to `partition_hash`).
    build_keys: Vec<String>,
    /// Probe-side join-key fields (owned; positionally aligned with build_keys).
    probe_keys: Vec<String>,
    /// Join type string (e.g. "inner", "left", "cross").
    join_type: String,
    /// Global output row limit; applied once after all partitions are probed.
    limit: usize,
    /// Collection/alias prefix for the probe (left) side columns.
    probe_collection: String,
    /// Collection/alias prefix for the index (right/build) side columns.
    index_collection: String,
    /// Whether unmatched build-side rows are emitted (right/full outer joins).
    emit_unmatched_right: bool,
    build: SideState,
    probe: SideState,
}

// Consumed by the grace-join spill integration (build-side stream + over-budget spill).
#[allow(dead_code)]
impl PartitionedSpiller {
    /// Create a spiller for one join.
    ///
    /// `partitions` is clamped to 1 when the join is a cartesian product
    /// (`spec.join_type == "cross"`) or keyless (either key list empty): hash-
    /// partitioning by key would break the cross product, so everything must
    /// share one partition.
    pub(super) fn new(
        spec: &GraceSpec,
        partitions: usize,
        per_partition_budget: usize,
        spill_dir: PathBuf,
    ) -> Self {
        let build_keys: Vec<String> = spec.build_keys.iter().map(|s| s.to_string()).collect();
        let probe_keys: Vec<String> = spec.probe_keys.iter().map(|s| s.to_string()).collect();

        let partitions = if spec.join_type == "cross"
            || build_keys.is_empty()
            || probe_keys.is_empty()
            || partitions == 0
        {
            1
        } else {
            partitions
        };

        Self {
            partitions,
            per_partition_budget,
            spill_dir,
            build_keys,
            probe_keys,
            join_type: spec.join_type.to_string(),
            limit: spec.limit,
            probe_collection: spec.probe_collection.to_string(),
            index_collection: spec.index_collection.to_string(),
            emit_unmatched_right: spec.emit_unmatched_right,
            build: SideState::new(partitions),
            probe: SideState::new(partitions),
        }
    }

    /// Feed one build-side (RIGHT / index) row's raw msgpack value bytes.
    pub(super) fn push_build(&mut self, value: &[u8]) -> crate::Result<()> {
        let p = (partition_hash(value, &self.build_keys) % self.partitions as u64) as usize;
        push_row(
            &mut self.build,
            p,
            value,
            self.per_partition_budget,
            &self.spill_dir,
            "build",
        )
    }

    /// Feed one probe-side (LEFT) row's raw msgpack value bytes.
    pub(super) fn push_probe(&mut self, value: &[u8]) -> crate::Result<()> {
        let p = (partition_hash(value, &self.probe_keys) % self.partitions as u64) as usize;
        push_row(
            &mut self.probe,
            p,
            value,
            self.per_partition_budget,
            &self.spill_dir,
            "probe",
        )
    }

    /// Consume the spiller: materialize each partition's build + probe docs
    /// (from RAM or by reading back its spill file), build a per-partition
    /// [`HashIndex`], probe it, and union the results.
    ///
    /// The per-partition probe runs with `usize::MAX` — NEVER the real `limit`
    /// (else up to P×limit rows). The GLOBAL `limit` is applied ONCE, after the
    /// loop, via `results.truncate(limit)`. Same rule as `grace_join_in_memory`.
    pub(super) fn finish_and_probe(self) -> crate::Result<Vec<Vec<u8>>> {
        // Destructure self to move all fields out at once — no clones needed.
        let PartitionedSpiller {
            partitions,
            join_type,
            limit,
            probe_collection,
            index_collection,
            emit_unmatched_right,
            build_keys,
            probe_keys,
            build,
            probe,
            ..
        } = self;

        // Build &str views once (per-partition, not per-row).
        let build_key_refs: Vec<&str> = build_keys.iter().map(String::as_str).collect();
        let probe_key_refs: Vec<&str> = probe_keys.iter().map(String::as_str).collect();
        let mut build_buffers = build.buffers;
        let mut build_spillers = build.spillers;
        let mut probe_buffers = probe.buffers;
        let mut probe_spillers = probe.spillers;

        let mut results: Vec<Vec<u8>> = Vec::new();
        for i in 0..partitions {
            let build_docs = materialize_partition(
                build_spillers[i].take(),
                std::mem::take(&mut build_buffers[i]),
            )?;
            let probe_docs = materialize_partition(
                probe_spillers[i].take(),
                std::mem::take(&mut probe_buffers[i]),
            )?;

            let index = HashIndex::build(&build_docs, &build_key_refs);
            let mut part = probe_hash_index(&ProbeParams {
                probe_docs: &probe_docs,
                index: &index,
                index_docs: &build_docs,
                probe_keys: &probe_key_refs,
                join_type: &join_type,
                limit: usize::MAX,
                probe_collection: &probe_collection,
                index_collection: &index_collection,
                emit_unmatched_right,
            });
            results.append(&mut part);
        }

        results.truncate(limit);
        Ok(results)
    }
}

/// Push one row into `side` partition `p`, spilling the partition if it grows
/// past `budget` (and a writer can be created).
fn push_row(
    side: &mut SideState,
    p: usize,
    value: &[u8],
    budget: usize,
    spill_dir: &std::path::Path,
    side_tag: &str,
) -> crate::Result<()> {
    // Already spilling → append straight to disk, no RAM growth.
    if let Some(w) = side.spillers[p].as_mut() {
        w.append_row(value)?;
        return Ok(());
    }

    // In-memory path.
    side.buffers[p].push((String::new(), value.to_vec()));
    side.bytes[p] += value.len();

    // Budget == 0 means "never spill" (pure in-memory).
    if budget == 0 || side.bytes[p] <= budget {
        return Ok(());
    }

    // Over budget — try to start spilling this partition.
    let path = spill_dir.join(format!("p{p}.{side_tag}.spill"));
    match SpillPartitionWriter::create(&path) {
        Some(mut w) => {
            // Drain everything currently buffered into the writer, then free RAM.
            for (_, row) in side.buffers[p].drain(..) {
                w.append_row(&row)?;
            }
            side.bytes[p] = 0;
            side.spillers[p] = Some(w);
        }
        None => {
            // io_uring unavailable (e.g. non-Linux): keep the partition in
            // memory. Correctness holds; only the memory bound is lost. This is
            // a platform capability gap, not an error — fall through silently.
        }
    }
    Ok(())
}

/// Materialize one partition's docs: read back its spill file if it was spilled,
/// otherwise take the already-resident in-memory buffer.
///
/// Spilled rows are reconstructed as `(String::new(), value)` — the id is never
/// used by `merge_join_docs_binary`, so an empty id is correct.
fn materialize_partition(
    spiller: Option<SpillPartitionWriter>,
    in_mem: Vec<(String, Vec<u8>)>,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    let Some(writer) = spiller else {
        return Ok(in_mem);
    };

    let path = writer.finish()?;
    // If we spilled, io_uring was available on the write path, so a reader must
    // be obtainable on the read path; treat absence as a hard error rather than
    // silently dropping the partition's rows.
    let mut reader = UringReader::with_config(
        SPILL_READ_QUEUE_DEPTH,
        SPILL_READ_CONCURRENCY,
        SPILL_READ_MAX_BYTES,
    )
    .ok_or_else(|| crate::Error::Storage {
        engine: "spill".into(),
        detail: format!(
            "UringReader unavailable while reading spilled partition back from {}",
            path.display()
        ),
    })?;

    let bufs = reader.read_files(&[path.as_path()]);
    // `read_files` returns one buffer per path (empty Vec on a failed/zero-size
    // read — a zero-row partition reads back as no rows, which is correct).
    let buf = bufs.into_iter().next().unwrap_or_default();
    let docs: Vec<(String, Vec<u8>)> = parse_framed_rows(&buf)
        .map(|r| (String::new(), r.to_vec()))
        .collect();
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A side of a join: `(doc_id, raw msgpack bytes)` pairs.
    type DocSet = Vec<(String, Vec<u8>)>;

    /// Build a msgpack map fixture via the same helper the other join tests use
    /// (`nodedb_types::json_to_msgpack`), NOT serde_json directly.
    fn msgpack_row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).unwrap()
    }

    /// Sort a result set so it can be compared as a MULTISET (duplicates
    /// preserved, order irrelevant).
    fn as_multiset(mut rows: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        rows.sort();
        rows
    }

    /// Single-key fixtures: matches, non-matches, DUPLICATE keys (count must be
    /// preserved), and a row MISSING the key field on each side.
    fn single_key_fixtures() -> (DocSet, DocSet) {
        let build = vec![
            (
                "b1".into(),
                msgpack_row(&[("k", serde_json::json!(1)), ("rv", serde_json::json!("r1"))]),
            ),
            (
                "b2".into(),
                msgpack_row(&[
                    ("k", serde_json::json!(1)),
                    ("rv", serde_json::json!("r1dup")),
                ]),
            ), // dup key 1
            (
                "b3".into(),
                msgpack_row(&[("k", serde_json::json!(2)), ("rv", serde_json::json!("r2"))]),
            ),
            (
                "b4".into(),
                msgpack_row(&[("k", serde_json::json!(9)), ("rv", serde_json::json!("r9"))]),
            ), // no match
            (
                "b5".into(),
                msgpack_row(&[("rv", serde_json::json!("r-nokey"))]),
            ), // missing key
        ];
        let probe = vec![
            (
                "p1".into(),
                msgpack_row(&[("k", serde_json::json!(1)), ("lv", serde_json::json!("l1"))]),
            ),
            (
                "p2".into(),
                msgpack_row(&[
                    ("k", serde_json::json!(1)),
                    ("lv", serde_json::json!("l1dup")),
                ]),
            ), // dup key 1
            (
                "p3".into(),
                msgpack_row(&[("k", serde_json::json!(2)), ("lv", serde_json::json!("l2"))]),
            ),
            (
                "p4".into(),
                msgpack_row(&[("k", serde_json::json!(7)), ("lv", serde_json::json!("l7"))]),
            ), // no match
            (
                "p5".into(),
                msgpack_row(&[("lv", serde_json::json!("l-nokey"))]),
            ), // missing key
        ];
        (build, probe)
    }

    const ALL_JOIN_TYPES: [&str; 7] = ["inner", "left", "right", "full", "semi", "anti", "cross"];

    /// Drive a `PartitionedSpiller` end to end for one join, returning its
    /// output. Each side's rows are fed one at a time via push_*.
    fn run_spiller(
        build: &[(String, Vec<u8>)],
        probe: &[(String, Vec<u8>)],
        partitions: usize,
        per_partition_budget: usize,
        spill_dir: PathBuf,
        spec: &GraceSpec,
    ) -> Vec<Vec<u8>> {
        let mut spiller =
            PartitionedSpiller::new(spec, partitions, per_partition_budget, spill_dir);
        for (_, v) in build {
            spiller.push_build(v).unwrap();
        }
        for (_, v) in probe {
            spiller.push_probe(v).unwrap();
        }
        spiller.finish_and_probe().unwrap()
    }

    // Round-trip + spill exercises io_uring → gate on Linux, mirroring spill.rs.
    #[cfg(target_os = "linux")]
    mod io_tests {
        use super::super::super::grace_partitioner::grace_join_in_memory;
        use super::*;

        /// budget = 1 forces EVERY partition to spill; assert the spilling path
        /// is multiset-equivalent to the in-memory reference for every join
        /// type and a couple of partition counts.
        #[test]
        fn spilling_matches_in_memory_reference_all_join_types() {
            let (build, probe) = single_key_fixtures();
            let build_keys = ["k"];
            let probe_keys = ["k"];

            for jt in ALL_JOIN_TYPES {
                let spec = GraceSpec {
                    build_keys: &build_keys,
                    probe_keys: &probe_keys,
                    join_type: jt,
                    limit: usize::MAX,
                    probe_collection: "l",
                    index_collection: "r",
                    emit_unmatched_right: true,
                };
                // partitions=4 matches the reference's own clamp rules
                let want =
                    as_multiset(grace_join_in_memory(build.clone(), probe.clone(), 4, &spec));

                for p in [1usize, 4] {
                    let dir = tempfile::tempdir().unwrap();
                    let got = run_spiller(
                        &build,
                        &probe,
                        p,
                        /* per_partition_budget */ 1, // force spill
                        dir.path().to_path_buf(),
                        &spec,
                    );
                    assert_eq!(
                        want,
                        as_multiset(got),
                        "SPILLING join_type={jt} partitions={p} multiset mismatch"
                    );
                }
            }
        }

        /// Composite-key spilling equivalence for inner + left.
        #[test]
        fn spilling_matches_reference_composite_key() {
            let build = vec![
                (
                    "b1".into(),
                    msgpack_row(&[
                        ("a", serde_json::json!(1)),
                        ("b", serde_json::json!("x")),
                        ("rv", serde_json::json!("r1")),
                    ]),
                ),
                (
                    "b2".into(),
                    msgpack_row(&[
                        ("a", serde_json::json!(1)),
                        ("b", serde_json::json!("y")),
                        ("rv", serde_json::json!("r2")),
                    ]),
                ),
                (
                    "b3".into(),
                    msgpack_row(&[
                        ("a", serde_json::json!(1)),
                        ("b", serde_json::json!("x")),
                        ("rv", serde_json::json!("r1dup")),
                    ]),
                ), // dup composite
            ];
            let probe = vec![
                (
                    "p1".into(),
                    msgpack_row(&[
                        ("a", serde_json::json!(1)),
                        ("b", serde_json::json!("x")),
                        ("lv", serde_json::json!("l1")),
                    ]),
                ),
                (
                    "p2".into(),
                    msgpack_row(&[
                        ("a", serde_json::json!(5)),
                        ("b", serde_json::json!("z")),
                        ("lv", serde_json::json!("nomatch")),
                    ]),
                ),
            ];
            let build_keys = ["a", "b"];
            let probe_keys = ["a", "b"];

            for jt in ["inner", "left"] {
                let spec = GraceSpec {
                    build_keys: &build_keys,
                    probe_keys: &probe_keys,
                    join_type: jt,
                    limit: usize::MAX,
                    probe_collection: "l",
                    index_collection: "r",
                    emit_unmatched_right: true,
                };
                let want =
                    as_multiset(grace_join_in_memory(build.clone(), probe.clone(), 4, &spec));
                for p in [1usize, 4] {
                    let dir = tempfile::tempdir().unwrap();
                    let got = run_spiller(&build, &probe, p, 1, dir.path().to_path_buf(), &spec);
                    assert_eq!(
                        want,
                        as_multiset(got),
                        "SPILLING composite join_type={jt} partitions={p}"
                    );
                }
            }
        }

        /// Non-spilling path (huge budget) must also match the reference — this
        /// confirms the pure in-memory branch of the spiller is equivalent.
        #[test]
        fn non_spilling_matches_in_memory_reference() {
            let (build, probe) = single_key_fixtures();
            let build_keys = ["k"];
            let probe_keys = ["k"];

            for jt in ALL_JOIN_TYPES {
                let spec = GraceSpec {
                    build_keys: &build_keys,
                    probe_keys: &probe_keys,
                    join_type: jt,
                    limit: usize::MAX,
                    probe_collection: "l",
                    index_collection: "r",
                    emit_unmatched_right: true,
                };
                let want =
                    as_multiset(grace_join_in_memory(build.clone(), probe.clone(), 4, &spec));
                for p in [1usize, 4] {
                    let dir = tempfile::tempdir().unwrap();
                    let got = run_spiller(
                        &build,
                        &probe,
                        p,
                        /* per_partition_budget */ 64 * 1024 * 1024, // never spill
                        dir.path().to_path_buf(),
                        &spec,
                    );
                    assert_eq!(
                        want,
                        as_multiset(got),
                        "NON-SPILLING join_type={jt} partitions={p} multiset mismatch"
                    );
                }
            }
        }
    }
}
