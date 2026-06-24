// SPDX-License-Identifier: BUSL-1.1

//! Shared graph algorithm utilities.

use std::cmp::Ordering;

use crate::engine::graph::csr::CsrIndex;

/// Collect undirected neighbors of a node (out + in, deduplicated).
pub fn undirected_neighbors(csr: &CsrIndex, node: u32) -> Vec<u32> {
    let mut neighbors: Vec<u32> = csr.iter_out_edges_raw(node).map(|(_, dst)| dst).collect();
    for (_, src) in csr.iter_in_edges_raw(node) {
        if !neighbors.contains(&src) {
            neighbors.push(src);
        }
    }
    neighbors
}

/// Descending comparator for f64 scores that always places NaN **last**.
///
/// Finite values are ordered largest-first (descending). NaN is treated as
/// less than every finite value, so it sinks to the end of a sorted slice.
/// Two NaN values compare as `Equal` for a stable, deterministic sort.
///
/// Use this instead of `b.total_cmp(&a)` for output ranking: `total_cmp`
/// treats NaN as the maximum value, which would incorrectly sort it to the
/// front of a descending ranking.
pub(crate) fn cmp_desc_nan_last(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,    // both NaN → tie, stays put
        (true, false) => Ordering::Greater, // a is NaN → a after b (sinks)
        (false, true) => Ordering::Less,    // b is NaN → a before b (floats up)
        (false, false) => b.total_cmp(&a),  // both finite → descending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmp_desc_nan_last_finite_descending() {
        let mut scores = vec![0.1f64, 0.9, 0.5];
        scores.sort_by(|&a, &b| cmp_desc_nan_last(a, b));
        assert_eq!(scores, vec![0.9, 0.5, 0.1]);
    }

    #[test]
    fn cmp_desc_nan_last_nan_sinks() {
        let mut scores = [f64::NAN, 0.9, 0.5, 0.1];
        scores.sort_by(|&a, &b| cmp_desc_nan_last(a, b));
        // Finite values first in descending order, NaN last.
        assert!(!scores[0].is_nan());
        assert!(!scores[1].is_nan());
        assert!(!scores[2].is_nan());
        assert!(scores[3].is_nan());
        assert!(scores[0] > scores[1]);
        assert!(scores[1] > scores[2]);
    }

    #[test]
    fn cmp_desc_nan_last_both_nan_equal() {
        assert_eq!(cmp_desc_nan_last(f64::NAN, f64::NAN), Ordering::Equal);
    }

    #[test]
    fn cmp_desc_nan_last_nan_vs_finite() {
        // NaN after any finite value.
        assert_eq!(cmp_desc_nan_last(f64::NAN, 0.0), Ordering::Greater);
        assert_eq!(cmp_desc_nan_last(0.0, f64::NAN), Ordering::Less);
        assert_eq!(
            cmp_desc_nan_last(f64::NAN, f64::NEG_INFINITY),
            Ordering::Greater
        );
    }
}
