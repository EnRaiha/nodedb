// SPDX-License-Identifier: BUSL-1.1

//! Round-trip tests: feed → finalize, and feed → split → merge → finalize.

use super::state::AggAccum;
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

/// Build a minimal bare-msgpack doc with one integer field.
///
/// Uses `value_to_msgpack` (not `zerompk::to_msgpack_vec`) so the output
/// is a standard msgpack map that `extract_field` / `map_header` can scan.
fn make_doc_i64(field: &str, value: i64) -> Vec<u8> {
    let mut map = std::collections::HashMap::new();
    map.insert(field.to_string(), Value::Integer(value));
    nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode doc")
}

fn make_doc_f64(field: &str, value: f64) -> Vec<u8> {
    let mut map = std::collections::HashMap::new();
    map.insert(field.to_string(), Value::Float(value));
    nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode doc")
}

fn make_doc_str(field: &str, value: &str) -> Vec<u8> {
    let mut map = std::collections::HashMap::new();
    map.insert(field.to_string(), Value::String(value.to_string()));
    nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode doc")
}

#[test]
fn merge_from_count() {
    let spec = make_spec("count", "*");
    let docs_a: Vec<Vec<u8>> = (0..5).map(|_| make_doc_i64("x", 1)).collect();
    let docs_b: Vec<Vec<u8>> = (0..7).map(|_| make_doc_i64("x", 2)).collect();

    let mut combined = AggAccum::new(&spec);
    for d in &docs_a {
        combined.feed(&spec, d).unwrap();
    }
    for d in &docs_b {
        combined.feed(&spec, d).unwrap();
    }

    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    assert_eq!(
        combined.finalize(&spec),
        a.finalize(&spec),
        "merge_from count"
    );
}

#[test]
fn merge_from_sum_avg() {
    let sum_spec = make_spec("sum", "v");
    let avg_spec = make_spec("avg", "v");

    let vals_a: Vec<f64> = [1.0, 2.0, 3.0].into();
    let vals_b: Vec<f64> = [4.0, 5.0, 6.0].into();

    let docs_a: Vec<Vec<u8>> = vals_a.iter().map(|&v| make_doc_f64("v", v)).collect();
    let docs_b: Vec<Vec<u8>> = vals_b.iter().map(|&v| make_doc_f64("v", v)).collect();

    // Combined baseline.
    let mut combined_sum = AggAccum::new(&sum_spec);
    let mut combined_avg = AggAccum::new(&avg_spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined_sum.feed(&sum_spec, d).unwrap();
        combined_avg.feed(&avg_spec, d).unwrap();
    }

    // Merge path.
    let mut a_sum = AggAccum::new(&sum_spec);
    let mut a_avg = AggAccum::new(&avg_spec);
    for d in &docs_a {
        a_sum.feed(&sum_spec, d).unwrap();
        a_avg.feed(&avg_spec, d).unwrap();
    }
    let mut b_sum = AggAccum::new(&sum_spec);
    let mut b_avg = AggAccum::new(&avg_spec);
    for d in &docs_b {
        b_sum.feed(&sum_spec, d).unwrap();
        b_avg.feed(&avg_spec, d).unwrap();
    }
    a_sum.merge_from(b_sum);
    a_avg.merge_from(b_avg);

    let Value::Float(cs) = combined_sum.finalize(&sum_spec) else {
        panic!("expected float");
    };
    let Value::Float(ms) = a_sum.finalize(&sum_spec) else {
        panic!("expected float");
    };
    assert!((cs - ms).abs() < 1e-9, "sum mismatch: {cs} vs {ms}");

    let Value::Float(ca) = combined_avg.finalize(&avg_spec) else {
        panic!("expected float");
    };
    let Value::Float(ma) = a_avg.finalize(&avg_spec) else {
        panic!("expected float");
    };
    assert!((ca - ma).abs() < 1e-9, "avg mismatch: {ca} vs {ma}");
}

#[test]
fn merge_from_sum_avg_distinct() {
    let sum_spec = make_spec("sum_distinct", "v");
    let avg_spec = make_spec("avg_distinct", "v");

    // Overlapping value sets: distinct union is {1,2,3,4,5} → sum 15, avg 3.
    let docs_a: Vec<Vec<u8>> = [1i64, 2, 2, 3]
        .iter()
        .map(|&v| make_doc_i64("v", v))
        .collect();
    let docs_b: Vec<Vec<u8>> = [3i64, 4, 4, 5]
        .iter()
        .map(|&v| make_doc_i64("v", v))
        .collect();

    // Combined baseline.
    let mut combined_sum = AggAccum::new(&sum_spec);
    let mut combined_avg = AggAccum::new(&avg_spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined_sum.feed(&sum_spec, d).unwrap();
        combined_avg.feed(&avg_spec, d).unwrap();
    }

    // Spilled-run merge path.
    let mut a_sum = AggAccum::new(&sum_spec);
    let mut a_avg = AggAccum::new(&avg_spec);
    for d in &docs_a {
        a_sum.feed(&sum_spec, d).unwrap();
        a_avg.feed(&avg_spec, d).unwrap();
    }
    let mut b_sum = AggAccum::new(&sum_spec);
    let mut b_avg = AggAccum::new(&avg_spec);
    for d in &docs_b {
        b_sum.feed(&sum_spec, d).unwrap();
        b_avg.feed(&avg_spec, d).unwrap();
    }
    a_sum.merge_from(b_sum);
    a_avg.merge_from(b_avg);

    assert_eq!(combined_sum.finalize(&sum_spec), Value::Float(15.0));
    assert_eq!(combined_avg.finalize(&avg_spec), Value::Float(3.0));
    assert_eq!(
        a_sum.finalize(&sum_spec),
        Value::Float(15.0),
        "sum_distinct merge"
    );
    assert_eq!(
        a_avg.finalize(&avg_spec),
        Value::Float(3.0),
        "avg_distinct merge"
    );
}

#[test]
fn merge_from_min_max() {
    let min_spec = make_spec("min", "v");
    let max_spec = make_spec("max", "v");

    let docs_a: Vec<Vec<u8>> = [3i64, 1, 7].iter().map(|&v| make_doc_i64("v", v)).collect();
    let docs_b: Vec<Vec<u8>> = [2i64, 9, 4].iter().map(|&v| make_doc_i64("v", v)).collect();

    let mut combined_min = AggAccum::new(&min_spec);
    let mut combined_max = AggAccum::new(&max_spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined_min.feed(&min_spec, d).unwrap();
        combined_max.feed(&max_spec, d).unwrap();
    }

    let mut a_min = AggAccum::new(&min_spec);
    let mut a_max = AggAccum::new(&max_spec);
    for d in &docs_a {
        a_min.feed(&min_spec, d).unwrap();
        a_max.feed(&max_spec, d).unwrap();
    }
    let mut b_min = AggAccum::new(&min_spec);
    let mut b_max = AggAccum::new(&max_spec);
    for d in &docs_b {
        b_min.feed(&min_spec, d).unwrap();
        b_max.feed(&max_spec, d).unwrap();
    }
    a_min.merge_from(b_min);
    a_max.merge_from(b_max);

    assert_eq!(
        combined_min.finalize(&min_spec),
        a_min.finalize(&min_spec),
        "min"
    );
    assert_eq!(
        combined_max.finalize(&max_spec),
        a_max.finalize(&max_spec),
        "max"
    );
}

#[test]
fn merge_from_welford() {
    let spec = make_spec("variance", "v");

    let vals_a: Vec<f64> = (1..=10).map(|i| i as f64).collect();
    let vals_b: Vec<f64> = (11..=20).map(|i| i as f64).collect();
    let docs_a: Vec<Vec<u8>> = vals_a.iter().map(|&v| make_doc_f64("v", v)).collect();
    let docs_b: Vec<Vec<u8>> = vals_b.iter().map(|&v| make_doc_f64("v", v)).collect();

    let mut combined = AggAccum::new(&spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined.feed(&spec, d).unwrap();
    }

    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    let Value::Float(cv) = combined.finalize(&spec) else {
        panic!("expected float");
    };
    let Value::Float(mv) = a.finalize(&spec) else {
        panic!("expected float");
    };
    let rel = (cv - mv).abs() / cv.abs().max(1e-12);
    assert!(
        rel < 1e-9,
        "Welford merge variance: {cv} vs {mv} (rel={rel})"
    );
}

#[test]
fn merge_from_count_distinct() {
    let spec = make_spec("count_distinct", "v");

    let docs_a: Vec<Vec<u8>> = ["a", "b", "c"]
        .iter()
        .map(|&s| make_doc_str("v", s))
        .collect();
    let docs_b: Vec<Vec<u8>> = ["c", "d", "e"]
        .iter()
        .map(|&s| make_doc_str("v", s))
        .collect();

    let mut combined = AggAccum::new(&spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined.feed(&spec, d).unwrap();
    }
    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    assert_eq!(
        combined.finalize(&spec),
        a.finalize(&spec),
        "count_distinct"
    );
}

#[test]
fn merge_from_hll() {
    let spec = make_spec("approx_count_distinct", "v");

    let docs_a: Vec<Vec<u8>> = (0..500u64).map(|i| make_doc_i64("v", i as i64)).collect();
    let docs_b: Vec<Vec<u8>> = (500..1000u64)
        .map(|i| make_doc_i64("v", i as i64))
        .collect();

    let mut combined = AggAccum::new(&spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined.feed(&spec, d).unwrap();
    }

    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    let Value::Integer(cv) = combined.finalize(&spec) else {
        panic!("expected int");
    };
    let Value::Integer(mv) = a.finalize(&spec) else {
        panic!("expected int");
    };
    // HLL is approximate; require within 5% of expected 1000.
    let diff = (cv - mv).abs() as f64 / 1000.0;
    assert!(diff < 0.05, "HLL merge: combined={cv}, merged={mv}");
}

#[test]
fn merge_from_tdigest() {
    let spec = make_spec("approx_percentile", "0.5:v");

    let docs_a: Vec<Vec<u8>> = (0..100).map(|i| make_doc_f64("v", i as f64)).collect();
    let docs_b: Vec<Vec<u8>> = (100..200).map(|i| make_doc_f64("v", i as f64)).collect();

    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    // p50 of 0..200 should be close to 100.
    let Value::Float(p50) = a.finalize(&spec) else {
        panic!("expected float");
    };
    assert!((50.0..150.0).contains(&p50), "TDigest merge p50={p50}");
}

#[test]
fn merge_from_topk() {
    let spec = make_spec("approx_topk", "3:v");
    // TopK is heuristic; just verify the top item count is right.
    let docs_a: Vec<Vec<u8>> = (0..50).map(|i| make_doc_i64("v", i % 3)).collect();
    let docs_b: Vec<Vec<u8>> = (0..50).map(|i| make_doc_i64("v", i % 3)).collect();

    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    let Value::Array(arr) = a.finalize(&spec) else {
        panic!("expected array");
    };
    assert_eq!(arr.len(), 3, "TopK should return k=3 items");
}

#[test]
fn merge_from_array_agg() {
    let spec = make_spec("array_agg", "v");

    let docs_a: Vec<Vec<u8>> = [1i64, 2, 3].iter().map(|&v| make_doc_i64("v", v)).collect();
    let docs_b: Vec<Vec<u8>> = [4i64, 5, 6].iter().map(|&v| make_doc_i64("v", v)).collect();

    let mut combined = AggAccum::new(&spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined.feed(&spec, d).unwrap();
    }

    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    assert_eq!(combined.finalize(&spec), a.finalize(&spec), "array_agg");
}

#[test]
fn merge_from_string_agg() {
    let spec = make_spec("string_agg", "v");

    let docs_a: Vec<Vec<u8>> = ["hello", "world"]
        .iter()
        .map(|&s| make_doc_str("v", s))
        .collect();
    let docs_b: Vec<Vec<u8>> = ["foo", "bar"]
        .iter()
        .map(|&s| make_doc_str("v", s))
        .collect();

    let mut combined = AggAccum::new(&spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined.feed(&spec, d).unwrap();
    }

    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    assert_eq!(combined.finalize(&spec), a.finalize(&spec), "string_agg");
}

#[test]
fn merge_from_percentile_cont() {
    let spec = make_spec("percentile_cont", "0.5:v");

    let docs_a: Vec<Vec<u8>> = [1.0f64, 3.0, 5.0]
        .iter()
        .map(|&v| make_doc_f64("v", v))
        .collect();
    let docs_b: Vec<Vec<u8>> = [2.0f64, 4.0, 6.0]
        .iter()
        .map(|&v| make_doc_f64("v", v))
        .collect();

    let mut combined = AggAccum::new(&spec);
    for d in docs_a.iter().chain(docs_b.iter()) {
        combined.feed(&spec, d).unwrap();
    }

    let mut a = AggAccum::new(&spec);
    for d in &docs_a {
        a.feed(&spec, d).unwrap();
    }
    let mut b = AggAccum::new(&spec);
    for d in &docs_b {
        b.feed(&spec, d).unwrap();
    }
    a.merge_from(b);

    assert_eq!(
        combined.finalize(&spec),
        a.finalize(&spec),
        "percentile_cont"
    );
}
