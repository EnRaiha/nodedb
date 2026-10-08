// SPDX-License-Identifier: Apache-2.0

//! Window frame bound resolution.
//!
//! Given the current row position, the partition length, the frame spec, and
//! (for RANGE/GROUPS) order-by values, this module resolves a frame to a
//! concrete `[start_idx, end_idx]` inclusive range into the partition's index
//! array.

use nodedb_types::Value;

use super::helpers::as_f64;
use super::spec::{FrameBound, WindowFrame};
use crate::value_ops::sort_peers;

/// Resolve the frame for row at position `pos` in a partition of `len` rows.
///
/// Returns `(start_idx, end_idx)` — both are inclusive indices into the
/// sorted partition slice. Guaranteed: `start_idx <= end_idx`, both within
/// `0..len`.
///
/// Arguments:
/// - `frame`: the `WindowFrame` from the spec.
/// - `pos`: current row's position within the partition (0-based).
/// - `len`: total number of rows in the partition.
/// - `order_values`: the ORDER BY column value for each row in partition order
///   (needed for RANGE numeric offsets). Empty slice is fine when no numeric
///   offsets are used.
/// - `peer_groups`: for each row, its zero-based peer-group index (needed for
///   GROUPS mode). Computed once per partition by `build_peer_groups`.
///   Empty slice is fine when mode != "groups".
pub(super) fn evaluate_frame_bounds(
    frame: &WindowFrame,
    pos: usize,
    len: usize,
    order_values: &[Value],
    peer_groups: &[usize],
) -> (usize, usize) {
    match frame.mode.as_str() {
        "rows" => rows_bounds(&frame.start, &frame.end, pos, len),
        "range" => range_bounds(&frame.start, &frame.end, pos, len, order_values),
        "groups" => groups_bounds(&frame.start, &frame.end, pos, len, peer_groups),
        // Unrecognised mode treated as full-partition — the planner must have
        // validated the mode; reaching here with an unknown value is a bug.
        _ => (0, len.saturating_sub(1)),
    }
}

/// Build a per-row peer-group index array for a partition.
///
/// Two rows are in the same peer group when their order-by values are peers
/// (see [`sort_peers`]). The returned vec has the same length as the
/// partition; each element is the zero-based group index of that row.
pub(super) fn build_peer_groups(order_values: &[Value]) -> Vec<usize> {
    let mut groups = Vec::with_capacity(order_values.len());
    let mut current_group = 0usize;
    for (i, val) in order_values.iter().enumerate() {
        if i > 0 && !sort_peers(val, &order_values[i - 1]) {
            current_group += 1;
        }
        groups.push(current_group);
    }
    groups
}

// ── ROWS ─────────────────────────────────────────────────────────────────────

fn rows_bounds(start: &FrameBound, end: &FrameBound, pos: usize, len: usize) -> (usize, usize) {
    let start_idx = rows_bound_to_idx(start, pos, len, true);
    let end_idx = rows_bound_to_idx(end, pos, len, false);
    (start_idx.min(end_idx), start_idx.max(end_idx))
}

fn rows_bound_to_idx(bound: &FrameBound, pos: usize, len: usize, _is_start: bool) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(n) => pos.saturating_sub(*n as usize),
        FrameBound::CurrentRow => pos,
        FrameBound::Following(n) => (pos + *n as usize).min(len.saturating_sub(1)),
        FrameBound::UnboundedFollowing => len.saturating_sub(1),
    }
}

// ── RANGE ─────────────────────────────────────────────────────────────────────

fn range_bounds(
    start: &FrameBound,
    end: &FrameBound,
    pos: usize,
    len: usize,
    order_values: &[Value],
) -> (usize, usize) {
    let current_val = order_values.get(pos).and_then(as_f64);

    let start_idx = range_bound_to_idx(start, pos, len, order_values, current_val, true);
    let end_idx = range_bound_to_idx(end, pos, len, order_values, current_val, false);
    (start_idx.min(end_idx), start_idx.max(end_idx))
}

fn range_bound_to_idx(
    bound: &FrameBound,
    pos: usize,
    len: usize,
    order_values: &[Value],
    current_val: Option<f64>,
    is_start: bool,
) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::UnboundedFollowing => len.saturating_sub(1),
        FrameBound::CurrentRow => {
            // Peer-aware: for start bound, go back to first peer;
            // for end bound, advance to last peer.
            let peers = |other: usize| match (order_values.get(other), order_values.get(pos)) {
                (Some(a), Some(b)) => sort_peers(a, b),
                (None, None) => true,
                _ => false,
            };
            if is_start {
                // Scan backward to find the first peer of the current row.
                let mut idx = pos;
                while idx > 0 && peers(idx - 1) {
                    idx -= 1;
                }
                idx
            } else {
                // Scan forward to find the last peer of the current row.
                let mut idx = pos;
                while idx + 1 < len && peers(idx + 1) {
                    idx += 1;
                }
                idx
            }
        }
        FrameBound::Preceding(n) => {
            let threshold = match current_val {
                Some(cv) => cv - *n as f64,
                // No numeric order value: fall back to current row position.
                None => return pos,
            };
            // First row whose value >= threshold.
            let mut idx = 0;
            for (i, v) in order_values.iter().enumerate() {
                if as_f64(v).is_some_and(|fv| fv >= threshold) {
                    idx = i;
                    break;
                }
                idx = i + 1;
            }
            idx.min(len.saturating_sub(1))
        }
        FrameBound::Following(n) => {
            let threshold = match current_val {
                Some(cv) => cv + *n as f64,
                None => return pos,
            };
            // Last row whose value <= threshold.
            let mut idx = pos;
            for (i, v) in order_values.iter().enumerate().skip(pos) {
                if as_f64(v).is_none_or(|fv| fv > threshold) {
                    break;
                }
                idx = i;
            }
            idx.min(len.saturating_sub(1))
        }
    }
}

// ── GROUPS ────────────────────────────────────────────────────────────────────

fn groups_bounds(
    start: &FrameBound,
    end: &FrameBound,
    pos: usize,
    len: usize,
    peer_groups: &[usize],
) -> (usize, usize) {
    let current_group = peer_groups.get(pos).copied().unwrap_or(0);
    let max_group = peer_groups.last().copied().unwrap_or(0);

    let start_group = groups_bound_to_group(start, current_group, max_group, true);
    let end_group = groups_bound_to_group(end, current_group, max_group, false);

    // Convert group numbers to row indices.
    let start_idx = peer_groups
        .iter()
        .position(|&g| g == start_group)
        .unwrap_or(0);
    let end_idx = peer_groups
        .iter()
        .rposition(|&g| g == end_group)
        .unwrap_or(len.saturating_sub(1));

    (start_idx, end_idx)
}

fn groups_bound_to_group(
    bound: &FrameBound,
    current_group: usize,
    max_group: usize,
    _is_start: bool,
) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::UnboundedFollowing => max_group,
        FrameBound::CurrentRow => current_group,
        FrameBound::Preceding(n) => current_group.saturating_sub(*n as usize),
        FrameBound::Following(n) => (current_group + *n as usize).min(max_group),
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::spec::{FrameBound, WindowFrame};

    fn range_frame(start: FrameBound, end: FrameBound) -> WindowFrame {
        WindowFrame {
            mode: "range".into(),
            start,
            end,
        }
    }

    fn rows_frame(start: FrameBound, end: FrameBound) -> WindowFrame {
        WindowFrame {
            mode: "rows".into(),
            start,
            end,
        }
    }

    fn groups_frame(start: FrameBound, end: FrameBound) -> WindowFrame {
        WindowFrame {
            mode: "groups".into(),
            start,
            end,
        }
    }

    fn num_vals(ns: &[i64]) -> Vec<Value> {
        ns.iter().map(|&n| Value::Integer(n)).collect()
    }

    // ROWS ───────────────────────────────────────────────────────────────────

    #[test]
    fn rows_1_preceding_1_following() {
        let frame = rows_frame(FrameBound::Preceding(1), FrameBound::Following(1));
        assert_eq!(evaluate_frame_bounds(&frame, 0, 5, &[], &[]), (0, 1));
        assert_eq!(evaluate_frame_bounds(&frame, 2, 5, &[], &[]), (1, 3));
        assert_eq!(evaluate_frame_bounds(&frame, 4, 5, &[], &[]), (3, 4));
    }

    #[test]
    fn rows_unbounded_preceding_current() {
        let frame = rows_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow);
        assert_eq!(evaluate_frame_bounds(&frame, 0, 5, &[], &[]), (0, 0));
        assert_eq!(evaluate_frame_bounds(&frame, 3, 5, &[], &[]), (0, 3));
    }

    #[test]
    fn rows_current_unbounded_following() {
        let frame = rows_frame(FrameBound::CurrentRow, FrameBound::UnboundedFollowing);
        assert_eq!(evaluate_frame_bounds(&frame, 0, 5, &[], &[]), (0, 4));
        assert_eq!(evaluate_frame_bounds(&frame, 4, 5, &[], &[]), (4, 4));
    }

    // RANGE ──────────────────────────────────────────────────────────────────

    #[test]
    fn range_unbounded_preceding_current_row_with_ties() {
        // Values: [1, 1, 2, 3] — both pos=0 and pos=1 share value 1.
        let vals = num_vals(&[1, 1, 2, 3]);
        let frame = range_frame(FrameBound::UnboundedPreceding, FrameBound::CurrentRow);
        // pos 0: start=0, end=last peer at val 1 = idx 1
        assert_eq!(evaluate_frame_bounds(&frame, 0, 4, &vals, &[]), (0, 1));
        // pos 1: same group
        assert_eq!(evaluate_frame_bounds(&frame, 1, 4, &vals, &[]), (0, 1));
        // pos 2: val 2, no peer
        assert_eq!(evaluate_frame_bounds(&frame, 2, 4, &vals, &[]), (0, 2));
    }

    #[test]
    fn range_numeric_preceding_following() {
        // Values: [10, 20, 30, 40, 50]
        let vals = num_vals(&[10, 20, 30, 40, 50]);
        // RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING
        let frame = range_frame(FrameBound::Preceding(10), FrameBound::Following(10));
        // pos=1 (val=20): includes vals in [10, 30] → indices 0..=2
        let (s, e) = evaluate_frame_bounds(&frame, 1, 5, &vals, &[]);
        assert!(s == 0 && e == 2, "got ({s},{e})");
    }

    // GROUPS ─────────────────────────────────────────────────────────────────

    #[test]
    fn build_peer_groups_basic() {
        let vals = num_vals(&[1, 1, 2, 3, 3]);
        let pg = build_peer_groups(&vals);
        assert_eq!(pg, vec![0, 0, 1, 2, 2]);
    }

    #[test]
    fn groups_1_preceding_1_following() {
        // Values: [1, 1, 2, 3, 3] → groups [0, 0, 1, 2, 2]
        let vals = num_vals(&[1, 1, 2, 3, 3]);
        let pg = build_peer_groups(&vals);
        let frame = groups_frame(FrameBound::Preceding(1), FrameBound::Following(1));

        // pos=0 (group 0): frame spans groups max(0-1,0)=0 to min(0+1,2)=1
        //   → rows with group 0 or 1 → idx 0..=2
        let (s, e) = evaluate_frame_bounds(&frame, 0, 5, &vals, &pg);
        assert_eq!((s, e), (0, 2), "pos=0");

        // pos=2 (group 1): frame spans groups 0 to 2 → idx 0..=4
        let (s, e) = evaluate_frame_bounds(&frame, 2, 5, &vals, &pg);
        assert_eq!((s, e), (0, 4), "pos=2");

        // pos=4 (group 2): frame spans groups 1 to 2 → idx 2..=4
        let (s, e) = evaluate_frame_bounds(&frame, 4, 5, &vals, &pg);
        assert_eq!((s, e), (2, 4), "pos=4");
    }
}
