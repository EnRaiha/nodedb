// SPDX-License-Identifier: BUSL-1.1

//! In-memory grace partitioner for the hash join.
//!
//! This is the building block that an upcoming integration step will wire into
//! `execute_hash_join` with spill + cap removal. It does NOT touch
//! `execute_hash_join` and adds NO spill — it is purely in-memory and produces
//! results IDENTICAL (as a multiset) to today's single-index hash join for
//! every join type.
//!
//! ## Why partitioning is correct
//!
//! Today's join matches rows by byte-equality of the extracted key ranges
//! (see `hash.rs::HashIndex::probe` — equal byte ranges via memcmp). The
//! partitioner here hashes those SAME extracted value bytes with a FIXED-SEED
//! hasher, so two rows whose key bytes are equal always produce the same
//! `partition_hash` and therefore land in the same partition. Any pair that
//! *could* match is never separated across partitions. Hash collisions between
//! non-matching rows are harmless: the per-partition `probe_hash_index` still
//! memcmp-rejects them exactly as the un-partitioned path does.

use std::hash::Hasher;

use super::hash::{HashIndex, ProbeParams, extract_join_key_range, probe_hash_index};

/// Stable, fixed-seed partition hash over a document's join-key value bytes.
///
/// Mirrors `hash_join_key`'s extraction and missing-field handling EXACTLY:
/// - present field → hash the raw extracted value bytes (`doc[start..end]`);
/// - missing field → hash the `0xc0` (msgpack NIL) sentinel.
///
/// Only the VALUE bytes are hashed (never the field name), in `keys` order.
///
/// Uses [`std::hash::DefaultHasher`] (deterministic, fixed keys) rather than
/// `RandomState`: build and probe sides MUST hash identically across calls so
/// that equal-key rows co-locate in the same partition.
// Consumed by the grace-join integration (build-side spill + scan-cap removal).
#[allow(dead_code)]
pub(super) fn partition_hash(doc: &[u8], keys: &[&str]) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    for key in keys {
        if let Some((start, end)) = extract_join_key_range(doc, key) {
            hasher.write(&doc[start..end]);
        } else {
            // Missing field — hash the same NIL sentinel `hash_join_key` uses.
            hasher.write_u8(0xc0);
        }
    }
    hasher.finish()
}

/// In-memory grace join: partition both inputs by `partition_hash`, then run
/// the reference `probe_hash_index` per partition and union the results.
///
/// Consumes OWNED Vecs and drains them into partition buffers BY MOVE (zero
/// clone in the hot path). In the integration step these owned Vecs come
/// straight from `execute_hash_join`.
///
/// `build_docs` is the RIGHT (index) side and `build_keys` its join-key fields;
/// `probe_docs` is the LEFT side and `probe_keys` its join-key fields,
/// positionally aligned with `build_keys`.
///
/// `limit` is the GLOBAL output limit. It is applied ONCE, after unioning all
/// partitions, via `results.truncate(limit)`. The per-partition probe is run
/// with `usize::MAX` so that no single partition prematurely truncates — using
/// the real limit per partition could emit up to `P × limit` rows.
///
/// ## Correctness notes
///
/// - Degenerate / cross joins are NOT partitioned. A cross join (or a keyless
///   join) is a cartesian product: every left row must still see every right
///   row, so hash-partitioning by key would break it. These run as a single
///   partition.
/// - Per-partition unmatched-right tracking is correct WITHOUT cross-partition
///   aggregation: a right row can only be matched by probe rows that hash to
///   its partition, so if it is unmatched within its partition it is globally
///   unmatched. `probe_hash_index` therefore emits exactly the right set of
///   unmatched-right rows per partition.
/// - `HashIndex`'s internal `doc_index` is relative to the slice passed to
///   `build`; we pass the same partition slice as `index_docs` to
///   `probe_hash_index`, so the indices align.
// Consumed by the grace-join integration (build-side spill + scan-cap removal).
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub(super) fn grace_join_in_memory(
    build_docs: Vec<(String, Vec<u8>)>,
    probe_docs: Vec<(String, Vec<u8>)>,
    build_keys: &[&str],
    probe_keys: &[&str],
    join_type: &str,
    partitions: usize,
    limit: usize,
    probe_collection: &str,
    index_collection: &str,
    emit_unmatched_right: bool,
) -> Vec<Vec<u8>> {
    // Degenerate / cross → single partition. Partitioning a cross/keyless join
    // by hash would break the cartesian product (every left row must see every
    // right row), so run the whole thing through one `HashIndex` / probe.
    if join_type == "cross" || build_keys.is_empty() || probe_keys.is_empty() || partitions <= 1 {
        let index = HashIndex::build(&build_docs, build_keys);
        return probe_hash_index(&ProbeParams {
            probe_docs: &probe_docs,
            index: &index,
            index_docs: &build_docs,
            probe_keys,
            join_type,
            limit,
            probe_collection,
            index_collection,
            emit_unmatched_right,
        });
    }

    // Drain both inputs into `partitions` buffers BY MOVE — no clone.
    let mut build_part: Vec<Vec<(String, Vec<u8>)>> = vec![vec![]; partitions];
    let mut probe_part: Vec<Vec<(String, Vec<u8>)>> = vec![vec![]; partitions];

    for row in build_docs {
        let idx = (partition_hash(&row.1, build_keys) % partitions as u64) as usize;
        build_part[idx].push(row);
    }
    for row in probe_docs {
        let idx = (partition_hash(&row.1, probe_keys) % partitions as u64) as usize;
        probe_part[idx].push(row);
    }

    // Probe each partition independently. Use `usize::MAX` as the per-partition
    // limit — NEVER the real limit (else up to P×limit rows). Truncate once,
    // globally, after unioning.
    let mut results: Vec<Vec<u8>> = Vec::new();
    for i in 0..partitions {
        let index = HashIndex::build(&build_part[i], build_keys);
        let mut part_results = probe_hash_index(&ProbeParams {
            probe_docs: &probe_part[i],
            index: &index,
            index_docs: &build_part[i],
            probe_keys,
            join_type,
            limit: usize::MAX,
            probe_collection,
            index_collection,
            emit_unmatched_right,
        });
        results.append(&mut part_results);
    }

    results.truncate(limit);
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A side of a join: `(doc_id, raw msgpack bytes)` pairs — the shape
    /// `execute_hash_join` materializes and `grace_join_in_memory` consumes.
    type DocSet = Vec<(String, Vec<u8>)>;

    /// Build a msgpack map fixture using the same helper the existing join
    /// tests use (`nodedb_types::json_to_msgpack`), NOT serde_json directly.
    fn msgpack_row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).unwrap()
    }

    /// Sort a result set so it can be compared as a MULTISET (duplicates must
    /// be preserved, order must not matter).
    fn as_multiset(mut rows: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        rows.sort();
        rows
    }

    /// Reference: the un-partitioned single-index probe — exactly what
    /// `execute_hash_join` does today.
    #[allow(clippy::too_many_arguments)]
    fn reference(
        build_docs: &[(String, Vec<u8>)],
        probe_docs: &[(String, Vec<u8>)],
        build_keys: &[&str],
        probe_keys: &[&str],
        join_type: &str,
        limit: usize,
        probe_collection: &str,
        index_collection: &str,
    ) -> Vec<Vec<u8>> {
        let index = HashIndex::build(build_docs, build_keys);
        probe_hash_index(&ProbeParams {
            probe_docs,
            index: &index,
            index_docs: build_docs,
            probe_keys,
            join_type,
            limit,
            probe_collection,
            index_collection,
            emit_unmatched_right: true,
        })
    }

    /// Single-key fixtures: matches, non-matches, duplicate keys (count must be
    /// preserved), and a row MISSING the key field on each side.
    fn single_key_fixtures() -> (DocSet, DocSet) {
        // Build (RIGHT) side.
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
        // Probe (LEFT) side.
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

    #[test]
    fn multiset_equivalence_all_join_types_all_partition_counts() {
        let (build, probe) = single_key_fixtures();
        let build_keys = ["k"];
        let probe_keys = ["k"];

        for jt in ALL_JOIN_TYPES {
            let reference_rows = reference(
                &build,
                &probe,
                &build_keys,
                &probe_keys,
                jt,
                usize::MAX,
                "l",
                "r",
            );
            let want = as_multiset(reference_rows);

            for p in [1usize, 2, 4, 8] {
                let candidate = grace_join_in_memory(
                    build.clone(),
                    probe.clone(),
                    &build_keys,
                    &probe_keys,
                    jt,
                    p,
                    usize::MAX,
                    "l",
                    "r",
                    true,
                );
                assert_eq!(
                    want,
                    as_multiset(candidate),
                    "join_type={jt} partitions={p} multiset mismatch"
                );
            }
        }
    }

    #[test]
    fn composite_key_equivalence_inner_and_left() {
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
                    ("a", serde_json::json!(2)),
                    ("b", serde_json::json!("x")),
                    ("rv", serde_json::json!("r3")),
                ]),
            ),
            (
                "b4".into(),
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
                    ("a", serde_json::json!(2)),
                    ("b", serde_json::json!("x")),
                    ("lv", serde_json::json!("l3")),
                ]),
            ),
            (
                "p3".into(),
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
            let want = as_multiset(reference(
                &build,
                &probe,
                &build_keys,
                &probe_keys,
                jt,
                usize::MAX,
                "l",
                "r",
            ));
            for p in [1usize, 2, 4, 8] {
                let candidate = grace_join_in_memory(
                    build.clone(),
                    probe.clone(),
                    &build_keys,
                    &probe_keys,
                    jt,
                    p,
                    usize::MAX,
                    "l",
                    "r",
                    true,
                );
                assert_eq!(
                    want,
                    as_multiset(candidate),
                    "composite join_type={jt} partitions={p}"
                );
            }
        }
    }

    #[test]
    fn empty_build_docs_matches_reference() {
        let (_, probe) = single_key_fixtures();
        let build: Vec<(String, Vec<u8>)> = Vec::new();
        let build_keys = ["k"];
        let probe_keys = ["k"];

        for jt in ALL_JOIN_TYPES {
            let want = as_multiset(reference(
                &build,
                &probe,
                &build_keys,
                &probe_keys,
                jt,
                usize::MAX,
                "l",
                "r",
            ));
            for p in [1usize, 2, 4, 8] {
                let candidate = grace_join_in_memory(
                    build.clone(),
                    probe.clone(),
                    &build_keys,
                    &probe_keys,
                    jt,
                    p,
                    usize::MAX,
                    "l",
                    "r",
                    true,
                );
                assert_eq!(
                    want,
                    as_multiset(candidate),
                    "empty build join_type={jt} p={p}"
                );
            }
        }
    }

    #[test]
    fn empty_probe_docs_matches_reference() {
        let (build, _) = single_key_fixtures();
        let probe: Vec<(String, Vec<u8>)> = Vec::new();
        let build_keys = ["k"];
        let probe_keys = ["k"];

        for jt in ALL_JOIN_TYPES {
            let want = as_multiset(reference(
                &build,
                &probe,
                &build_keys,
                &probe_keys,
                jt,
                usize::MAX,
                "l",
                "r",
            ));
            for p in [1usize, 2, 4, 8] {
                let candidate = grace_join_in_memory(
                    build.clone(),
                    probe.clone(),
                    &build_keys,
                    &probe_keys,
                    jt,
                    p,
                    usize::MAX,
                    "l",
                    "r",
                    true,
                );
                assert_eq!(
                    want,
                    as_multiset(candidate),
                    "empty probe join_type={jt} p={p}"
                );
            }
        }
    }

    #[test]
    fn limit_truncation_caps_output() {
        let (build, probe) = single_key_fixtures();
        let build_keys = ["k"];
        let probe_keys = ["k"];

        // Reference (unbounded) inner-join count, to pick a smaller limit.
        let full = reference(
            &build,
            &probe,
            &build_keys,
            &probe_keys,
            "inner",
            usize::MAX,
            "l",
            "r",
        );
        assert!(full.len() >= 2, "fixture must produce >= 2 inner rows");
        let limit = full.len() - 1;

        for p in [1usize, 2, 4, 8] {
            let candidate = grace_join_in_memory(
                build.clone(),
                probe.clone(),
                &build_keys,
                &probe_keys,
                "inner",
                p,
                limit,
                "l",
                "r",
                true,
            );
            assert_eq!(candidate.len(), limit, "limit truncation p={p}");
        }
    }

    #[test]
    fn partition_hash_is_stable_for_equal_key_bytes() {
        // Same key value bytes → identical partition hash across calls, even
        // when surrounding fields differ. This is the co-location invariant.
        let a = msgpack_row(&[("k", serde_json::json!(42)), ("x", serde_json::json!("a"))]);
        let b = msgpack_row(&[
            ("k", serde_json::json!(42)),
            ("y", serde_json::json!("different")),
        ]);
        let keys = ["k"];
        assert_eq!(partition_hash(&a, &keys), partition_hash(&b, &keys));

        // Missing key on both sides hashes the NIL sentinel identically.
        let m1 = msgpack_row(&[("other", serde_json::json!(1))]);
        let m2 = msgpack_row(&[("nope", serde_json::json!(2))]);
        assert_eq!(partition_hash(&m1, &keys), partition_hash(&m2, &keys));
    }
}
