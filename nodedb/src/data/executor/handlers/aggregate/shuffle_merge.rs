// SPDX-License-Identifier: BUSL-1.1

//! Distributed GROUP BY shuffle CONSUMER (`QueryOp::ShuffleAggregateConsume`).
//!
//! Reads a node-local staged frame file of partial-state rows
//! (`{<gb cols>, "__agg_state": <bytes>}`, one msgpack row per
//! `[u32 LE len][row-bytes]` frame — the same format `FrameStreamReader`
//! reads), re-derives each group key byte-identically to the accumulate path,
//! decodes and `merge_from`s the partial `GroupState`s into a consolidated
//! per-group map, then runs the shared `finalize_groups` tail.
//!
//! The merge core is factored into the free function [`merge_state_frames`] so
//! it is unit-testable without a live Data-Plane harness; the handler simply
//! pairs it with `finalize_groups`.

use std::collections::HashMap;
use std::path::Path;

use super::state_emit::AGG_STATE_FIELD;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::accum::GroupState;
use crate::data::executor::handlers::join::FrameStreamReader;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_query::msgpack_scan;

/// Merge every partial-state frame in `state_path` into a consolidated
/// `HashMap<group_key, GroupState>`.
///
/// For each frame row:
/// - the group key is rebuilt via `msgpack_scan::build_group_key(row,
///   group_by)` — byte-identical to the producer's accumulate-side key, so
///   matching groups from different producers collide on the same map entry;
/// - the `__agg_state` field bytes are decoded into a partial `GroupState` and
///   `merge_from`'d into the entry (or inserted as the first state).
///
/// A frame row missing `__agg_state`, or carrying a non-binary / undecodable
/// state, is a HARD error — never a silent drop — mirroring the frame-reader
/// truncation contract.
pub(in crate::data::executor) fn merge_state_frames(
    state_path: &Path,
    group_by: &[String],
    // Accepted for an explicit spec-count contract at the call site and API
    // symmetry with `accumulate_groups`; the decoded states already carry one
    // accumulator per spec, so the merge needs no per-spec dispatch.
    _aggregates: &[AggregateSpec],
) -> crate::Result<HashMap<String, GroupState>> {
    let mut merged: HashMap<String, GroupState> = HashMap::new();
    let mut reader = FrameStreamReader::open(state_path)?;

    while let Some(row) = reader.next_row()? {
        // Byte-identical group key reconstruction.
        let key = msgpack_scan::build_group_key(&row, group_by);

        // Extract the `__agg_state` binary field.
        let (val_start, _val_end) = msgpack_scan::extract_field(&row, 0, AGG_STATE_FIELD)
            .ok_or_else(|| crate::Error::Codec {
                detail: format!("shuffle-aggregate row missing `{AGG_STATE_FIELD}` field"),
            })?;
        let mut off = val_start;
        let state_bytes =
            msgpack_scan::read_bin_advance(&row, &mut off).ok_or_else(|| crate::Error::Codec {
                detail: format!("shuffle-aggregate `{AGG_STATE_FIELD}` is not binary"),
            })?;

        // GroupState was serialized via serde (sonic_rs JSON) on the producer.
        let state: GroupState =
            sonic_rs::from_slice(state_bytes).map_err(|e| crate::Error::Codec {
                detail: format!("shuffle-aggregate partial-state decode: {e}"),
            })?;

        match merged.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                o.get_mut().merge_from(state);
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(state);
            }
        }
    }

    Ok(merged)
}

impl CoreLoop {
    /// Execute a `ShuffleAggregateConsume`: merge the staged partial states,
    /// then finalize / HAVING / sort / LIMIT via the shared finalize tail.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_shuffle_aggregate(
        &mut self,
        task: &ExecutionTask,
        state_path: &str,
        group_by: &[String],
        aggregates: &[AggregateSpec],
        having: &[u8],
        limit: usize,
        sort_keys: &[(String, bool)],
    ) -> Response {
        let merged = match merge_state_frames(Path::new(state_path), group_by, aggregates) {
            Ok(m) => m,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Plain GROUP BY: no sub-groups in the shuffle path.
        match self.finalize_groups(
            merged,
            HashMap::new(),
            group_by,
            aggregates,
            having,
            limit,
            &[],
            &[],
            sort_keys,
        ) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write as _;

    use super::merge_state_frames;
    use crate::data::executor::handlers::accum::GroupState;
    use nodedb_physical::physical_plan::AggregateSpec;
    use nodedb_types::Value;

    fn make_spec(func: &str, field: &str) -> AggregateSpec {
        AggregateSpec {
            function: func.to_string(),
            field: field.to_string(),
            alias: format!("{func}({field})"),
            user_alias: None,
            expr: None,
        }
    }

    /// Build a `{g: <group>, v: <value>}` document. `value: None` omits `v`
    /// (so `count(v)` ignores it while `count(*)` still counts the row).
    fn make_doc(group: &str, value: Option<i64>) -> Vec<u8> {
        let mut map: HashMap<String, Value> = HashMap::new();
        map.insert("g".to_string(), Value::String(group.to_string()));
        if let Some(v) = value {
            map.insert("v".to_string(), Value::Integer(v));
        }
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode doc")
    }

    /// Accumulate `docs` into per-group `GroupState`s, keyed exactly the way the
    /// producer keys them (`build_group_key`), and emit one framed partial-state
    /// row per group — the `[u32 LE len][msgpack {g, __agg_state}]` format the
    /// consumer reads.
    fn write_partial_state_frames(
        path: &std::path::Path,
        specs: &[AggregateSpec],
        group_by: &[String],
        docs: &[Vec<u8>],
    ) {
        let mut groups: HashMap<String, GroupState> = HashMap::new();
        for doc in docs {
            let key = nodedb_query::msgpack_scan::build_group_key(doc, group_by);
            groups
                .entry(key)
                .or_insert_with(|| GroupState::new(specs))
                .feed(specs, doc);
        }

        let mut f = std::fs::File::create(path).expect("create frame file");
        for (key, state) in groups {
            // Recover the GROUP BY column values from the JSON-array key (same
            // as the production producer) and re-emit them as flat row fields.
            let parts: Vec<serde_json::Value> = sonic_rs::from_str(&key).expect("key json");
            let mut row: HashMap<String, Value> = HashMap::new();
            for (i, field) in group_by.iter().enumerate() {
                let jv = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                row.insert(field.clone(), Value::from(jv));
            }
            let state_bytes = sonic_rs::to_vec(&state).expect("state json");
            row.insert(
                super::AGG_STATE_FIELD.to_string(),
                Value::Bytes(state_bytes),
            );

            let row_bytes =
                nodedb_types::value_to_msgpack(&Value::Object(row)).expect("encode row");
            let len = u32::try_from(row_bytes.len()).expect("row fits u32");
            f.write_all(&len.to_le_bytes()).expect("write len");
            f.write_all(&row_bytes).expect("write body");
        }
        f.flush().expect("flush");
    }

    /// Single-pass reference: accumulate the UNION of all docs in one
    /// `GroupState` per key and finalize.
    fn reference_finalized(
        specs: &[AggregateSpec],
        group_by: &[String],
        docs: &[Vec<u8>],
    ) -> HashMap<String, Vec<Value>> {
        let mut map: HashMap<String, GroupState> = HashMap::new();
        for doc in docs {
            let key = nodedb_query::msgpack_scan::build_group_key(doc, group_by);
            map.entry(key)
                .or_insert_with(|| GroupState::new(specs))
                .feed(specs, doc);
        }
        map.into_iter()
            .map(|(k, s)| (k, s.finalize(specs).into_iter().map(|(_, v)| v).collect()))
            .collect()
    }

    /// Two disjoint producers that SHARE overlapping group keys: the cross-node
    /// combiner must merge their partial states to equal a single-pass aggregate
    /// over the union. Exercises additive (count/sum), Kahan (avg), Welford
    /// (stddev_pop), min/max, and set-union (count_distinct) merge paths.
    #[test]
    fn shuffle_combiner_equals_single_pass_over_union() {
        let specs = vec![
            make_spec("count", "*"),
            make_spec("count", "v"),
            make_spec("sum", "v"),
            make_spec("avg", "v"),
            make_spec("min", "v"),
            make_spec("max", "v"),
            make_spec("stddev_pop", "v"),
            make_spec("count_distinct", "v"),
        ];
        let group_by = vec!["g".to_string()];

        // Producer A and B both touch groups "x" and "y" (overlap) and B also
        // adds a group "z" only it sees. A null `v` exercises count(*) vs
        // count(v) divergence.
        let docs_a: Vec<Vec<u8>> = vec![
            make_doc("x", Some(1)),
            make_doc("x", Some(2)),
            make_doc("y", Some(10)),
            make_doc("x", None),
            make_doc("y", Some(10)), // duplicate value → count_distinct check
        ];
        let docs_b: Vec<Vec<u8>> = vec![
            make_doc("x", Some(3)),
            make_doc("y", Some(20)),
            make_doc("y", Some(30)),
            make_doc("z", Some(7)),
            make_doc("z", Some(7)),
        ];

        let dir = tempfile::tempdir().expect("tempdir");
        let path_a = dir.path().join("producer_a.frames");
        let path_b = dir.path().join("producer_b.frames");
        write_partial_state_frames(&path_a, &specs, &group_by, &docs_a);
        write_partial_state_frames(&path_b, &specs, &group_by, &docs_b);

        // Concatenate both producers' frame files into one staged stream (the
        // shuffle receive path appends every producer's frames to one file).
        let combined = dir.path().join("combined.frames");
        {
            let mut out = std::fs::File::create(&combined).expect("create combined");
            for p in [&path_a, &path_b] {
                let bytes = std::fs::read(p).expect("read producer frames");
                out.write_all(&bytes).expect("append");
            }
            out.flush().expect("flush combined");
        }

        // Merge the partial states, finalize.
        let merged = merge_state_frames(&combined, &group_by, &specs).expect("merge");
        let got: HashMap<String, Vec<Value>> = merged
            .into_iter()
            .map(|(k, s)| (k, s.finalize(&specs).into_iter().map(|(_, v)| v).collect()))
            .collect();

        // Reference: single pass over the union of both doc sets.
        let mut union = docs_a.clone();
        union.extend(docs_b.clone());
        let expected = reference_finalized(&specs, &group_by, &union);

        assert_eq!(
            got.len(),
            expected.len(),
            "group count must match the single-pass reference"
        );
        for (k, ev) in &expected {
            let gv = got
                .get(k)
                .unwrap_or_else(|| panic!("merged result missing group {k}"));
            assert_eq!(
                gv, ev,
                "aggregate values for group {k} must equal the single-pass union aggregate"
            );
        }
    }
}
