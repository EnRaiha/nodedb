// SPDX-License-Identifier: BUSL-1.1

//! Spill-to-disk manager for the schemaless GROUP BY aggregation path.
//!
//! Uses `String`-keyed `GroupState` accumulators, spilling to temp files when
//! the in-memory group count exceeds `cap` or the memory governor signals
//! pressure. All spill runs are k-way merged at `finalize()` time via
//! [`super::core::SpillCore`].
//!
//! # Composite sub-group keys
//!
//! When the caller has an outer + sub-group structure it flattens the keys
//! using ASCII Unit Separator (U+001F, byte 0x1F) as a delimiter:
//! `format!("{outer}\x1F{sub}")`.  U+001F cannot appear in JSON-encoded
//! string values, so the composite key is unambiguous.  `finalize()` returns
//! the flat map; the caller is responsible for splitting on `\x1F` to
//! reconstruct the outer/sub structure.

use std::collections::HashMap;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use super::core::SpillCore;
use crate::data::executor::handlers::accum::GroupState;
use nodedb_physical::physical_plan::AggregateSpec;

/// Spill-to-disk manager for the schemaless GROUP BY path.
pub(in crate::data::executor::handlers) struct GroupBySpiller {
    core: SpillCore<String, GroupState>,
    in_mem: HashMap<String, GroupState>,
    cap: usize,
    governor: Option<Arc<nodedb_mem::MemoryGovernor>>,
    reservation: Option<nodedb_mem::BudgetGuard>,
    bytes_estimate: usize,
    feed_counter: u64,
}

impl GroupBySpiller {
    pub(in crate::data::executor::handlers) fn new(
        spill_dir: PathBuf,
        cap: usize,
        governor: Option<Arc<nodedb_mem::MemoryGovernor>>,
    ) -> crate::Result<Self> {
        Ok(Self {
            core: SpillCore::new(spill_dir)?,
            in_mem: HashMap::new(),
            cap: cap.max(1),
            governor,
            reservation: None,
            bytes_estimate: 0,
            feed_counter: 0,
        })
    }

    #[cfg(test)]
    pub(in crate::data::executor::handlers) fn spilled_runs(&self) -> u64 {
        self.core.spilled_runs
    }

    /// Feed one document into the accumulator for the given group key.
    ///
    /// Triggers a disk spill when the in-memory map is full or governor
    /// pressure is detected (checked every 10 000 feeds).
    pub(in crate::data::executor::handlers) fn feed(
        &mut self,
        key: String,
        aggregates: &[AggregateSpec],
        doc: &[u8],
    ) -> crate::Result<()> {
        self.feed_counter += 1;

        if self.feed_counter.is_multiple_of(10_000) {
            let estimated_growth = mem::size_of::<GroupState>() * 10_000;
            if let Some(ref gov) = self.governor {
                match gov.reserve(nodedb_mem::EngineId::Query, estimated_growth) {
                    Ok(guard) => {
                        self.bytes_estimate = self.bytes_estimate.saturating_add(estimated_growth);
                        self.reservation = Some(guard);
                    }
                    Err(_) => {
                        self.spill_current_run()?;
                    }
                }
            }
        }

        if let Some(state) = self.in_mem.get_mut(&key) {
            state.feed(aggregates, doc)?;
        } else {
            if self.in_mem.len() >= self.cap {
                self.spill_current_run()?;
            }
            let key_len = key.len();
            let mut state = GroupState::new(aggregates);
            state.feed(aggregates, doc)?;
            self.in_mem.insert(key, state);
            self.bytes_estimate = self
                .bytes_estimate
                .saturating_add(mem::size_of::<GroupState>() + key_len);
        }

        Ok(())
    }

    fn spill_current_run(&mut self) -> crate::Result<()> {
        if self.in_mem.is_empty() {
            return Ok(());
        }
        self.core.flush_run(self.in_mem.drain())?;
        self.bytes_estimate = 0;
        self.reservation = None;
        Ok(())
    }

    /// Merge all spill runs and remaining in-memory groups into a consolidated
    /// `HashMap<String, GroupState>`.
    pub(in crate::data::executor::handlers) fn finalize(
        mut self,
    ) -> crate::Result<HashMap<String, GroupState>> {
        self.core
            .merge(&mut self.in_mem, self.cap, |dst, src| dst.merge_from(src))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crate::data::executor::handlers::accum::GroupState;
    use nodedb_physical::physical_plan::AggregateSpec;
    use nodedb_types::Value;

    use super::GroupBySpiller;

    fn make_spec(func: &str, field: &str) -> AggregateSpec {
        AggregateSpec {
            function: func.to_string(),
            field: field.to_string(),
            alias: format!("{func}({field})"),
            user_alias: None,
            expr: None,
        }
    }

    fn make_doc_i64(field: &str, value: i64) -> Vec<u8> {
        let mut map = std::collections::HashMap::new();
        map.insert(field.to_string(), Value::Integer(value));
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode doc")
    }

    fn temp_spill_dir(suffix: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nodedb_groupby_spill_test_{suffix}_{}",
            std::process::id()
        ));
        p
    }

    fn reference_agg(
        specs: &[AggregateSpec],
        groups: &[(String, Vec<u8>)],
    ) -> HashMap<String, Vec<Value>> {
        let mut map: HashMap<String, GroupState> = HashMap::new();
        for (key, doc) in groups {
            map.entry(key.clone())
                .or_insert_with(|| GroupState::new(specs))
                .feed(specs, doc)
                .unwrap();
        }
        map.into_iter()
            .map(|(k, s)| (k, s.finalize(specs).into_iter().map(|(_, v)| v).collect()))
            .collect()
    }

    fn finalize_to_values(
        result: HashMap<String, GroupState>,
        specs: &[AggregateSpec],
    ) -> HashMap<String, Vec<Value>> {
        result
            .into_iter()
            .map(|(k, s)| (k, s.finalize(specs).into_iter().map(|(_, v)| v).collect()))
            .collect()
    }

    #[test]
    fn roundtrip_no_spill() {
        let specs = vec![make_spec("count", "*"), make_spec("sum", "v")];
        let spill_dir = temp_spill_dir("no_spill");

        let groups: Vec<(String, Vec<u8>)> = (0..50)
            .map(|i| {
                let key = format!("g{}", i % 5);
                let doc = make_doc_i64("v", i);
                (key, doc)
            })
            .collect();

        let mut spiller = GroupBySpiller::new(spill_dir, 1000, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        assert_eq!(spiller.spilled_runs(), 0, "no spill expected");

        let result = spiller.finalize().unwrap();
        let got = finalize_to_values(result, &specs);
        let expected = reference_agg(&specs, &groups);
        assert_eq!(got.len(), expected.len());
        for (k, ev) in &expected {
            let gv = got.get(k).expect("key missing");
            assert_eq!(gv[0], ev[0], "count mismatch for key {k}");
        }
    }

    #[test]
    fn single_spill_run() {
        let specs = vec![make_spec("count", "*")];
        let spill_dir = temp_spill_dir("single_spill");

        // cap=5, insert 6 unique keys → exactly one spill run.
        let groups: Vec<(String, Vec<u8>)> = (0..6)
            .map(|i| (format!("g{i}"), make_doc_i64("v", i)))
            .collect();

        let mut spiller = GroupBySpiller::new(spill_dir, 5, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        assert!(
            spiller.spilled_runs() >= 1,
            "expected at least one spill run"
        );

        let result = spiller.finalize().unwrap();
        assert_eq!(
            result.len(),
            6,
            "all 6 unique groups must survive spill+merge"
        );
    }

    #[test]
    fn many_spill_runs() {
        let specs = vec![make_spec("count", "*")];
        let spill_dir = temp_spill_dir("many_spill");

        // cap=10, 50 unique keys → ~5 spill runs.
        let groups: Vec<(String, Vec<u8>)> = (0..50)
            .map(|i| (format!("g{i}"), make_doc_i64("v", i)))
            .collect();

        let mut spiller = GroupBySpiller::new(spill_dir, 10, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        assert!(spiller.spilled_runs() >= 4, "expected multiple spill runs");

        let result = spiller.finalize().unwrap();
        assert_eq!(result.len(), 50, "all 50 groups must be present");
    }

    #[test]
    fn spill_preserves_counts() {
        let specs = vec![make_spec("count", "*")];
        let spill_dir = temp_spill_dir("count_preserve");

        // 3 groups, each receiving 10 docs, but cap=2 forces spills.
        let groups: Vec<(String, Vec<u8>)> = (0..30)
            .map(|i| (format!("g{}", i % 3), make_doc_i64("v", i)))
            .collect();

        let reference = reference_agg(&specs, &groups);

        let mut spiller = GroupBySpiller::new(spill_dir, 2, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        let result = spiller.finalize().unwrap();
        let got = finalize_to_values(result, &specs);

        for (k, ev) in &reference {
            let gv = got.get(k).expect("key missing");
            assert_eq!(gv[0], ev[0], "count mismatch for key {k}");
        }
    }

    #[test]
    fn merge_correctness_count() {
        let specs = vec![make_spec("count", "*")];
        let spill_dir = temp_spill_dir("corr_count");

        let groups: Vec<(String, Vec<u8>)> = (0..20)
            .map(|i| (format!("g{}", i % 4), make_doc_i64("v", i)))
            .collect();

        let reference = reference_agg(&specs, &groups);

        let mut spiller = GroupBySpiller::new(spill_dir, 2, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        let result = spiller.finalize().unwrap();
        let got = finalize_to_values(result, &specs);

        assert_eq!(got, reference);
    }

    #[test]
    fn merge_correctness_sum_avg() {
        let specs = vec![make_spec("sum", "v"), make_spec("avg", "v")];
        let spill_dir = temp_spill_dir("corr_sumavg");

        let groups: Vec<(String, Vec<u8>)> = (0..24)
            .map(|i| (format!("g{}", i % 4), make_doc_i64("v", i)))
            .collect();

        let reference = reference_agg(&specs, &groups);

        let mut spiller = GroupBySpiller::new(spill_dir, 2, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        let result = spiller.finalize().unwrap();
        let got = finalize_to_values(result, &specs);

        for (k, ev) in &reference {
            let gv = got.get(k).expect("key missing");
            for (i, (e, g)) in ev.iter().zip(gv.iter()).enumerate() {
                match (e, g) {
                    (Value::Float(ef), Value::Float(gf)) => {
                        assert!(
                            (ef - gf).abs() < 1e-6,
                            "agg[{i}] mismatch for key {k}: expected {ef}, got {gf}"
                        );
                    }
                    _ => assert_eq!(e, g, "agg[{i}] mismatch for key {k}"),
                }
            }
        }
    }

    #[test]
    fn merge_correctness_min_max() {
        let specs = vec![make_spec("min", "v"), make_spec("max", "v")];
        let spill_dir = temp_spill_dir("corr_minmax");

        let groups: Vec<(String, Vec<u8>)> = (0..20)
            .map(|i| (format!("g{}", i % 4), make_doc_i64("v", i)))
            .collect();

        let reference = reference_agg(&specs, &groups);

        let mut spiller = GroupBySpiller::new(spill_dir, 2, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        let result = spiller.finalize().unwrap();
        let got = finalize_to_values(result, &specs);

        assert_eq!(got, reference);
    }

    #[test]
    fn merge_correctness_welford_variance() {
        let specs = vec![make_spec("variance", "v")];
        let spill_dir = temp_spill_dir("corr_welford");

        let groups: Vec<(String, Vec<u8>)> = (1..=20i64)
            .map(|i| ("all".to_string(), make_doc_i64("v", i)))
            .collect();

        let reference = reference_agg(&specs, &groups);

        let mut spiller = GroupBySpiller::new(spill_dir, 2, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        let result = spiller.finalize().unwrap();
        let got = finalize_to_values(result, &specs);

        let Value::Float(ev) = reference["all"][0] else {
            panic!("expected float");
        };
        let Value::Float(gv) = got["all"][0] else {
            panic!("expected float");
        };
        let rel = (ev - gv).abs() / ev.abs().max(1e-12);
        assert!(rel < 1e-6, "Welford variance: expected {ev}, got {gv}");
    }

    #[test]
    fn merge_correctness_hll() {
        let specs = vec![make_spec("approx_count_distinct", "v")];
        let spill_dir = temp_spill_dir("corr_hll");

        let groups: Vec<(String, Vec<u8>)> = (0..200i64)
            .map(|i| ("g".to_string(), make_doc_i64("v", i)))
            .collect();

        let mut spiller = GroupBySpiller::new(spill_dir, 5, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        let result = spiller.finalize().unwrap();
        let got = finalize_to_values(result, &specs);

        let Value::Integer(count) = got["g"][0] else {
            panic!("expected integer");
        };
        // HLL is approximate; within 10% of 200.
        assert!(
            (180..=220).contains(&count),
            "HLL approx count: got {count}, expected ~200"
        );
    }

    #[test]
    fn merge_correctness_tdigest() {
        let specs = vec![make_spec("approx_percentile", "0.5:v")];
        let spill_dir = temp_spill_dir("corr_tdigest");

        let groups: Vec<(String, Vec<u8>)> = (0..200i64)
            .map(|i| ("g".to_string(), make_doc_i64("v", i)))
            .collect();

        let mut spiller = GroupBySpiller::new(spill_dir, 5, None).unwrap();
        for (key, doc) in &groups {
            spiller.feed(key.clone(), &specs, doc).unwrap();
        }
        let result = spiller.finalize().unwrap();
        let got = finalize_to_values(result, &specs);

        let Value::Float(p50) = got["g"][0] else {
            panic!("expected float");
        };
        assert!((80.0..120.0).contains(&p50), "TDigest p50: got {p50}");
    }
}
