// SPDX-License-Identifier: Apache-2.0

//! Aggregate window functions (sum, count, avg, min, max, first_value, last_value)
//! and frame-bound resolution for the Value-native evaluator.

use std::collections::HashMap;

use nodedb_types::Value;

use super::spec::{FrameBound, WindowFrame, WindowFuncSpec};
use super::value_eval::{WindowError, cmp_values, eval_arg_for_row, order_keys_equal_v, set_cell};
use crate::simd_agg;

pub(super) fn apply_v_aggregate(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let use_running = spec.frame.mode == "range"
        && matches!(spec.frame.start, FrameBound::UnboundedPreceding)
        && matches!(spec.frame.end, FrameBound::CurrentRow);

    if use_running {
        apply_v_running_aggregate(rows, indices, column_index, spec, write_col)
    } else {
        apply_v_per_row_aggregate(rows, indices, column_index, spec, write_col)
    }
}

fn eval_arg(
    spec: &WindowFuncSpec,
    row: &[Value],
    column_index: &HashMap<String, usize>,
) -> Result<Value, WindowError> {
    match spec.args.first() {
        Some(expr) => Ok(eval_arg_for_row(expr, row, column_index)?),
        None => Ok(Value::Null),
    }
}

fn apply_v_running_aggregate(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let len = indices.len();
    if len == 0 {
        return Ok(());
    }

    let mut running_sum = 0.0f64;
    let mut running_count = 0u64;
    let mut running_min: Option<f64> = None;
    let mut running_max: Option<f64> = None;
    let mut peer_start = 0usize;

    for pos in 0..len {
        let i = indices[pos];
        let val = match rows.get(i) {
            Some(row) => eval_arg(spec, row, column_index)?,
            None => Value::Null,
        };

        if let Some(n) = val.as_f64() {
            running_sum += n;
            running_count += 1;
            running_min = Some(running_min.map_or(n, |m: f64| m.min(n)));
            running_max = Some(running_max.map_or(n, |m: f64| m.max(n)));
        } else if spec.func_name == "count" {
            running_count += 1;
        }

        let is_last_in_group = pos + 1 == len
            || !order_keys_equal_v(rows, i, indices[pos + 1], column_index, &spec.order_by)?;

        if is_last_in_group {
            let first_val = match rows.get(indices[0]) {
                Some(row) => eval_arg(spec, row, column_index)?,
                None => Value::Null,
            };
            let last_val = match rows.get(indices[pos]) {
                Some(row) => eval_arg(spec, row, column_index)?,
                None => Value::Null,
            };

            let result = match spec.func_name.as_str() {
                "sum" => Value::Float(running_sum),
                "count" => Value::Integer(running_count as i64),
                "avg" => {
                    if running_count > 0 {
                        Value::Float(running_sum / running_count as f64)
                    } else {
                        Value::Null
                    }
                }
                "min" => running_min.map(Value::Float).unwrap_or(Value::Null),
                "max" => running_max.map(Value::Float).unwrap_or(Value::Null),
                "first_value" => first_val,
                "last_value" => last_val,
                _ => Value::Null,
            };

            for &peer_idx in &indices[peer_start..=pos] {
                set_cell(rows, peer_idx, write_col, result.clone());
            }
            peer_start = pos + 1;
        }
    }
    Ok(())
}

fn apply_v_per_row_aggregate(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    write_col: usize,
) -> Result<(), WindowError> {
    let len = indices.len();
    if len == 0 {
        return Ok(());
    }

    let order_expr = spec.order_by.first().map(|(expr, _)| expr);
    let order_values: Vec<Value> = indices
        .iter()
        .map(|&i| match (order_expr, rows.get(i)) {
            (Some(expr), Some(row)) => Ok(eval_arg_for_row(expr, row, column_index)?),
            _ => Ok(Value::Null),
        })
        .collect::<Result<Vec<_>, WindowError>>()?;

    let peer_groups: Vec<usize> = if spec.frame.mode == "groups" {
        build_v_peer_groups(&order_values)
    } else {
        Vec::new()
    };

    let all_vals: Vec<Option<f64>> = indices
        .iter()
        .map(|&i| match rows.get(i) {
            Some(row) => Ok(eval_arg(spec, row, column_index)?.as_f64()),
            None => Ok(None),
        })
        .collect::<Result<Vec<_>, WindowError>>()?;

    let results: Vec<Value> = (0..len)
        .map(|pos| {
            let (start_idx, end_idx) =
                evaluate_v_frame_bounds(&spec.frame, pos, len, &order_values, &peer_groups);
            aggregate_v_slice(
                &all_vals,
                indices,
                rows,
                column_index,
                spec,
                start_idx,
                end_idx,
            )
        })
        .collect::<Result<Vec<_>, WindowError>>()?;

    for (pos, result) in results.into_iter().enumerate() {
        set_cell(rows, indices[pos], write_col, result);
    }
    Ok(())
}

fn aggregate_v_slice(
    all_vals: &[Option<f64>],
    indices: &[usize],
    rows: &[Vec<Value>],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
    start_idx: usize,
    end_idx: usize,
) -> Result<Value, WindowError> {
    let slice_vals: Vec<f64> = all_vals[start_idx..=end_idx]
        .iter()
        .filter_map(|v| *v)
        .collect();
    let slice_count = end_idx - start_idx + 1;

    let result = match spec.func_name.as_str() {
        "sum" => {
            let rt = simd_agg::ts_runtime();
            Value::Float((rt.sum_f64)(&slice_vals))
        }
        "count" => Value::Integer(slice_count as i64),
        "avg" => {
            if slice_vals.is_empty() {
                Value::Null
            } else {
                let rt = simd_agg::ts_runtime();
                Value::Float((rt.sum_f64)(&slice_vals) / slice_vals.len() as f64)
            }
        }
        "min" => {
            if slice_vals.is_empty() {
                Value::Null
            } else {
                let rt = simd_agg::ts_runtime();
                Value::Float((rt.min_f64)(&slice_vals))
            }
        }
        "max" => {
            if slice_vals.is_empty() {
                Value::Null
            } else {
                let rt = simd_agg::ts_runtime();
                Value::Float((rt.max_f64)(&slice_vals))
            }
        }
        "first_value" => match indices.get(start_idx).and_then(|&i| rows.get(i)) {
            Some(row) => eval_arg_for_row(
                spec.args
                    .first()
                    .unwrap_or(&crate::expr::types::SqlExpr::Literal(Value::Null)),
                row,
                column_index,
            )?,
            None => Value::Null,
        },
        "last_value" => match indices.get(end_idx).and_then(|&i| rows.get(i)) {
            Some(row) => eval_arg_for_row(
                spec.args
                    .first()
                    .unwrap_or(&crate::expr::types::SqlExpr::Literal(Value::Null)),
                row,
                column_index,
            )?,
            None => Value::Null,
        },
        _ => Value::Null,
    };
    Ok(result)
}

fn build_v_peer_groups(order_values: &[Value]) -> Vec<usize> {
    let mut groups = Vec::with_capacity(order_values.len());
    let mut current_group = 0usize;
    for (i, val) in order_values.iter().enumerate() {
        if i > 0
            && !matches!(
                cmp_values(val, &order_values[i - 1]),
                std::cmp::Ordering::Equal
            )
        {
            current_group += 1;
        }
        groups.push(current_group);
    }
    groups
}

pub(super) fn evaluate_v_frame_bounds(
    frame: &WindowFrame,
    pos: usize,
    len: usize,
    order_values: &[Value],
    peer_groups: &[usize],
) -> (usize, usize) {
    match frame.mode.as_str() {
        "rows" => v_rows_bounds(&frame.start, &frame.end, pos, len),
        "range" => v_range_bounds(&frame.start, &frame.end, pos, len, order_values),
        "groups" => v_groups_bounds(&frame.start, &frame.end, pos, len, peer_groups),
        _ => (0, len.saturating_sub(1)),
    }
}

fn v_rows_bounds(start: &FrameBound, end: &FrameBound, pos: usize, len: usize) -> (usize, usize) {
    let s = v_rows_bound_to_idx(start, pos, len);
    let e = v_rows_bound_to_idx(end, pos, len);
    (s.min(e), s.max(e))
}

fn v_rows_bound_to_idx(bound: &FrameBound, pos: usize, len: usize) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(n) => pos.saturating_sub(*n as usize),
        FrameBound::CurrentRow => pos,
        FrameBound::Following(n) => (pos + *n as usize).min(len.saturating_sub(1)),
        FrameBound::UnboundedFollowing => len.saturating_sub(1),
    }
}

fn v_range_bounds(
    start: &FrameBound,
    end: &FrameBound,
    pos: usize,
    len: usize,
    order_values: &[Value],
) -> (usize, usize) {
    let current_val = order_values.get(pos).and_then(|v| v.as_f64());
    let s = v_range_bound_to_idx(start, pos, len, order_values, current_val, true);
    let e = v_range_bound_to_idx(end, pos, len, order_values, current_val, false);
    (s.min(e), s.max(e))
}

fn v_range_bound_to_idx(
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
            if is_start {
                let mut idx = pos;
                while idx > 0
                    && matches!(
                        cmp_values(
                            order_values.get(idx - 1).unwrap_or(&Value::Null),
                            order_values.get(pos).unwrap_or(&Value::Null),
                        ),
                        std::cmp::Ordering::Equal
                    )
                {
                    idx -= 1;
                }
                idx
            } else {
                let mut idx = pos;
                while idx + 1 < len
                    && matches!(
                        cmp_values(
                            order_values.get(idx + 1).unwrap_or(&Value::Null),
                            order_values.get(pos).unwrap_or(&Value::Null),
                        ),
                        std::cmp::Ordering::Equal
                    )
                {
                    idx += 1;
                }
                idx
            }
        }
        FrameBound::Preceding(n) => {
            let threshold = match current_val {
                Some(cv) => cv - *n as f64,
                None => return pos,
            };
            let mut idx = 0;
            for (i, v) in order_values.iter().enumerate() {
                if v.as_f64().is_some_and(|fv| fv >= threshold) {
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
            let mut idx = pos;
            for (i, v) in order_values.iter().enumerate().skip(pos) {
                if v.as_f64().is_none_or(|fv| fv > threshold) {
                    break;
                }
                idx = i;
            }
            idx.min(len.saturating_sub(1))
        }
    }
}

fn v_groups_bounds(
    start: &FrameBound,
    end: &FrameBound,
    pos: usize,
    len: usize,
    peer_groups: &[usize],
) -> (usize, usize) {
    let current_group = peer_groups.get(pos).copied().unwrap_or(0);
    let max_group = peer_groups.last().copied().unwrap_or(0);
    let start_group = v_groups_bound_to_group(start, current_group, max_group);
    let end_group = v_groups_bound_to_group(end, current_group, max_group);
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

fn v_groups_bound_to_group(bound: &FrameBound, current_group: usize, max_group: usize) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::UnboundedFollowing => max_group,
        FrameBound::CurrentRow => current_group,
        FrameBound::Preceding(n) => current_group.saturating_sub(*n as usize),
        FrameBound::Following(n) => (current_group + *n as usize).min(max_group),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::types::SqlExpr;

    fn col(name: &str) -> SqlExpr {
        SqlExpr::Column(name.to_string())
    }

    fn ci(names: &[&str]) -> HashMap<String, usize> {
        names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.to_string(), i))
            .collect()
    }

    fn rows_v(vals: &[i64]) -> Vec<Vec<Value>> {
        vals.iter().map(|&v| vec![Value::Integer(v)]).collect()
    }

    fn agg_spec(func: &str, frame: WindowFrame, order_by: Vec<(SqlExpr, bool)>) -> WindowFuncSpec {
        WindowFuncSpec {
            alias: format!("w_{func}"),
            func_name: func.to_string(),
            args: vec![col("v")],
            partition_by: vec![],
            order_by,
            frame,
        }
    }

    /// Drive `apply_v_aggregate` over the whole single-partition row set the
    /// same way `evaluate_window_functions_value` does (push a Null cell, then
    /// fill it), returning the produced column.
    fn run_agg(rows: &mut [Vec<Value>], cols: &HashMap<String, usize>, spec: &WindowFuncSpec) {
        let write_col = rows.first().map(|r| r.len()).unwrap_or(0);
        for row in rows.iter_mut() {
            row.push(Value::Null);
        }
        let indices: Vec<usize> = (0..rows.len()).collect();
        apply_v_aggregate(rows, &indices, cols, spec, write_col).unwrap();
    }

    fn frame(mode: &str, start: FrameBound, end: FrameBound) -> WindowFrame {
        WindowFrame {
            mode: mode.into(),
            start,
            end,
        }
    }

    #[test]
    fn running_sum_is_cumulative() {
        // Default frame (range, unbounded preceding → current row) with a
        // strictly increasing order key → cumulative sum.
        let cols = ci(&["v"]);
        let mut rows = rows_v(&[1, 2, 3]);
        let s = agg_spec("sum", WindowFrame::default(), vec![(col("v"), true)]);
        run_agg(&mut rows, &cols, &s);
        let got: Vec<f64> = rows.iter().map(|r| r[1].as_f64().unwrap()).collect();
        assert_eq!(got, vec![1.0, 3.0, 6.0]);
    }

    #[test]
    fn running_sum_shares_value_across_peers() {
        // Tied order keys form a peer group; the running aggregate assigns the
        // group's running total to every peer.
        let cols = ci(&["v"]);
        let mut rows = rows_v(&[5, 5, 9]);
        let s = agg_spec("sum", WindowFrame::default(), vec![(col("v"), true)]);
        run_agg(&mut rows, &cols, &s);
        let got: Vec<f64> = rows.iter().map(|r| r[1].as_f64().unwrap()).collect();
        assert_eq!(got, vec![10.0, 10.0, 19.0]);
    }

    #[test]
    fn rows_frame_sliding_sum() {
        let cols = ci(&["v"]);
        let mut rows = rows_v(&[10, 20, 30]);
        let s = agg_spec(
            "sum",
            frame("rows", FrameBound::Preceding(1), FrameBound::CurrentRow),
            vec![(col("v"), true)],
        );
        run_agg(&mut rows, &cols, &s);
        let got: Vec<f64> = rows.iter().map(|r| r[1].as_f64().unwrap()).collect();
        assert_eq!(got, vec![10.0, 30.0, 50.0]);
    }

    #[test]
    fn rows_frame_count_and_avg() {
        let cols = ci(&["v"]);
        let f = frame(
            "rows",
            FrameBound::UnboundedPreceding,
            FrameBound::CurrentRow,
        );

        let mut rows = rows_v(&[4, 8, 12]);
        let cnt = agg_spec("count", f.clone(), vec![(col("v"), true)]);
        run_agg(&mut rows, &cols, &cnt);
        let counts: Vec<i64> = rows
            .iter()
            .map(|r| match r[1] {
                Value::Integer(n) => n,
                _ => panic!("count must be integer"),
            })
            .collect();
        assert_eq!(counts, vec![1, 2, 3]);

        let mut rows = rows_v(&[4, 8, 12]);
        let avg = agg_spec("avg", f, vec![(col("v"), true)]);
        run_agg(&mut rows, &cols, &avg);
        let avgs: Vec<f64> = rows.iter().map(|r| r[1].as_f64().unwrap()).collect();
        assert_eq!(avgs, vec![4.0, 6.0, 8.0]);
    }

    #[test]
    fn rows_frame_min_max() {
        let cols = ci(&["v"]);
        let f = frame(
            "rows",
            FrameBound::UnboundedPreceding,
            FrameBound::UnboundedFollowing,
        );

        let mut rows = rows_v(&[3, 1, 2]);
        let mn = agg_spec("min", f.clone(), vec![]);
        run_agg(&mut rows, &cols, &mn);
        assert!((rows[0][1].as_f64().unwrap() - 1.0).abs() < 1e-9);

        let mut rows = rows_v(&[3, 1, 2]);
        let mx = agg_spec("max", f, vec![]);
        run_agg(&mut rows, &cols, &mx);
        assert!((rows[0][1].as_f64().unwrap() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn first_and_last_value() {
        let cols = ci(&["v"]);
        let f = frame(
            "rows",
            FrameBound::UnboundedPreceding,
            FrameBound::UnboundedFollowing,
        );

        let mut rows = rows_v(&[7, 8, 9]);
        let fv = agg_spec("first_value", f.clone(), vec![]);
        run_agg(&mut rows, &cols, &fv);
        assert_eq!(rows[2][1].as_f64().unwrap() as i64, 7);

        let mut rows = rows_v(&[7, 8, 9]);
        let lv = agg_spec("last_value", f, vec![]);
        run_agg(&mut rows, &cols, &lv);
        assert_eq!(rows[0][1].as_f64().unwrap() as i64, 9);
    }

    #[test]
    fn rows_bounds_resolution() {
        let order = vec![];
        let groups = vec![];
        let f = frame("rows", FrameBound::Preceding(1), FrameBound::Following(1));
        // Middle of a 5-row partition → window [pos-1, pos+1].
        assert_eq!(evaluate_v_frame_bounds(&f, 2, 5, &order, &groups), (1, 3));
        // First row clamps the preceding bound to 0.
        assert_eq!(evaluate_v_frame_bounds(&f, 0, 5, &order, &groups), (0, 1));
        // Last row clamps the following bound to len-1.
        assert_eq!(evaluate_v_frame_bounds(&f, 4, 5, &order, &groups), (3, 4));
    }

    #[test]
    fn range_bounds_expand_over_peers() {
        // RANGE with CURRENT ROW spans the whole peer group of equal keys.
        let order = vec![Value::Integer(10), Value::Integer(10), Value::Integer(20)];
        let f = frame(
            "range",
            FrameBound::UnboundedPreceding,
            FrameBound::CurrentRow,
        );
        // pos 0 (key 10) → end extends across both 10s.
        assert_eq!(evaluate_v_frame_bounds(&f, 0, 3, &order, &[]), (0, 1));
        // pos 2 (key 20) → spans everything up to and including itself.
        assert_eq!(evaluate_v_frame_bounds(&f, 2, 3, &order, &[]), (0, 2));
    }

    #[test]
    fn groups_bounds_resolution() {
        // Peer groups: [g0, g0, g1, g2]. GROUPS 1 PRECEDING → CURRENT ROW at
        // pos 2 (group 1) covers groups 0..=1 → indices 0..=1.
        let peer_groups = vec![0usize, 0, 1, 2];
        let order = vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ];
        let f = frame("groups", FrameBound::Preceding(1), FrameBound::CurrentRow);
        assert_eq!(
            evaluate_v_frame_bounds(&f, 2, 4, &order, &peer_groups),
            (0, 2)
        );
    }
}
