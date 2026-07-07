// SPDX-License-Identifier: BUSL-1.1

//! Tiered grouping strategies: dispatch, direct-index, FxHash, generic,
//! time-bucket, and integer-to-string key resolution.

use rustc_hash::FxHashMap;

use super::super::columnar_agg::AggAccum;
use super::super::columnar_memtable::{ColumnData, ColumnType};
use super::types::{GroupedAggResult, ResolvedSchema, accumulate_row, for_each_set_bit};

const DIRECT_INDEX_MAX_CARDINALITY: u32 = 65536;

/// Integer group key for local (per-source) grouping.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) enum IntGroupKey {
    None,
    SingleU32(u32),
    Multi(Vec<u64>),
}

/// Row-level scan inputs shared by every grouping strategy: the resolved
/// schema, per-column data, the row-selection bitmask, row count, and
/// aggregate count. Bundled so the top-level dispatch/bucket entry points
/// stay within clippy's argument budget.
#[derive(Clone, Copy)]
pub(super) struct GroupedScanInputs<'a> {
    pub resolved: &'a ResolvedSchema,
    pub columns: &'a [Option<&'a ColumnData>],
    pub mask: &'a [u64],
    pub row_count: usize,
    pub num_aggs: usize,
}

pub(super) fn dispatch_grouping<'a>(
    inputs: GroupedScanInputs<'a>,
    group_by: &[String],
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
    timestamps: Option<&[i64]>,
    bucket_interval_ms: i64,
) -> GroupedAggResult {
    let GroupedScanInputs {
        resolved,
        columns,
        mask,
        row_count,
        num_aggs,
    } = inputs;
    let has_bucket = bucket_interval_ms > 0 && timestamps.is_some();

    if has_bucket && let Some(ts) = timestamps {
        return aggregate_with_bucket(inputs, ts, bucket_interval_ms, sym_lookup);
    }

    let local_groups = if group_by.is_empty() {
        aggregate_no_group(resolved, columns, mask, row_count, num_aggs)
    } else if group_by.len() == 1 && resolved.group_cols[0].1 == ColumnType::Symbol {
        let (col_idx, _) = resolved.group_cols[0];
        if let Some(data) = columns[col_idx] {
            let sym_ids = data.as_symbols();
            let cardinality = sym_lookup(col_idx).map(|d| d.len() as u32).unwrap_or(0);
            if cardinality <= DIRECT_INDEX_MAX_CARDINALITY && cardinality > 0 {
                aggregate_direct_index(
                    resolved,
                    columns,
                    mask,
                    row_count,
                    num_aggs,
                    sym_ids,
                    cardinality,
                )
            } else {
                aggregate_hash_u32(resolved, columns, mask, row_count, num_aggs, sym_ids)
            }
        } else {
            aggregate_hash_generic(resolved, columns, mask, row_count, num_aggs)
        }
    } else if row_count > 100_000 {
        aggregate_two_level(resolved, columns, mask, row_count, num_aggs)
    } else {
        aggregate_hash_generic(resolved, columns, mask, row_count, num_aggs)
    };

    // Resolve integer keys to strings.
    let mut result = GroupedAggResult::new(num_aggs);
    for (int_key, accums) in local_groups {
        let str_key = resolve_group_key(&int_key, resolved, sym_lookup);
        let entry = result
            .groups
            .entry(str_key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        for (i, a) in accums.iter().enumerate() {
            entry[i].merge(a);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Grouping strategies
// ---------------------------------------------------------------------------

fn aggregate_no_group(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut accums: Vec<AggAccum> = (0..num_aggs).map(|_| AggAccum::default()).collect();
    for_each_set_bit(mask, row_count, |row_idx| {
        accumulate_row(&mut accums, resolved, columns, row_idx);
    });
    vec![(IntGroupKey::None, accums)]
}

fn aggregate_direct_index(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
    sym_ids: &[u32],
    cardinality: u32,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let card = cardinality as usize;
    let mut table: Vec<Vec<AggAccum>> = (0..card)
        .map(|_| (0..num_aggs).map(|_| AggAccum::default()).collect())
        .collect();

    for_each_set_bit(mask, row_count, |row_idx| {
        let id = sym_ids[row_idx] as usize;
        if id < card {
            accumulate_row(&mut table[id], resolved, columns, row_idx);
        }
    });

    table
        .into_iter()
        .enumerate()
        .filter(|(_, accums)| accums.iter().any(|a| a.count > 0))
        .map(|(id, accums)| (IntGroupKey::SingleU32(id as u32), accums))
        .collect()
}

fn aggregate_hash_u32(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
    sym_ids: &[u32],
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut groups: FxHashMap<u32, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let id = sym_ids[row_idx];
        let accums = groups
            .entry(id)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    groups
        .into_iter()
        .map(|(id, accums)| (IntGroupKey::SingleU32(id), accums))
        .collect()
}

fn aggregate_hash_generic(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut groups: FxHashMap<Vec<u64>, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let key = build_generic_key(resolved, columns, row_idx);
        let accums = groups
            .entry(key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    groups
        .into_iter()
        .map(|(key, accums)| (IntGroupKey::Multi(key), accums))
        .collect()
}

/// Two-level aggregation for high-cardinality GROUP BY (2M+ keys).
///
/// Phase 1: Partition rows into buckets by hash prefix (top 8 bits → 256 buckets).
/// Phase 2: Aggregate each bucket independently (small HashMap, cache-friendly).
/// Phase 3: Flatten all buckets into the final result.
fn aggregate_two_level(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    const NUM_BUCKETS: usize = 256;

    // Phase 1: Build key for each row, hash it, and partition into buckets by top 8 bits.
    // The key is stored alongside the row index so Phase 2 does not recompute it.
    let mut buckets: Vec<Vec<(usize, Vec<u64>)>> = (0..NUM_BUCKETS).map(|_| Vec::new()).collect();
    for_each_set_bit(mask, row_count, |row_idx| {
        let key = build_generic_key(resolved, columns, row_idx);
        let hash = fx_hash_key(&key);
        let bucket = (hash >> 56) as usize;
        buckets[bucket].push((row_idx, key));
    });

    // Phase 2 + 3: Aggregate each bucket independently and flatten.
    let mut all_results: Vec<(IntGroupKey, Vec<AggAccum>)> = Vec::new();
    for bucket_rows in buckets {
        if bucket_rows.is_empty() {
            continue;
        }
        let mut groups: FxHashMap<Vec<u64>, Vec<AggAccum>> = FxHashMap::default();
        for (row_idx, key) in bucket_rows {
            let accums = groups
                .entry(key)
                .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
            accumulate_row(accums, resolved, columns, row_idx);
        }
        all_results.extend(groups.into_iter().map(|(k, a)| (IntGroupKey::Multi(k), a)));
    }

    all_results
}

/// Time-bucket aggregation with integer keys.
///
/// Packs (bucket_ts, group_key_parts...) into a `Vec<u64>`:
/// - `parts[0]` = bucket_ts as u64
/// - `parts[1..]` = group column values (symbol IDs, i64, f64 bits)
///
/// Resolves to string keys with "bucket_ts\0group1\0group2" format
/// so `emit_grouped_results` can parse them.
fn aggregate_with_bucket<'a>(
    inputs: GroupedScanInputs<'a>,
    timestamps: &[i64],
    bucket_interval_ms: i64,
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) -> GroupedAggResult {
    let GroupedScanInputs {
        resolved,
        columns,
        mask,
        row_count,
        num_aggs,
    } = inputs;
    let key_len = 1 + resolved.group_cols.len(); // bucket + group columns

    let mut groups: FxHashMap<Vec<u64>, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let bucket =
            super::super::time_bucket::time_bucket(bucket_interval_ms, timestamps[row_idx]);

        let mut key = Vec::with_capacity(key_len);
        key.push(bucket as u64);

        // Pack group-by columns as integers.
        for &(col_idx, ty) in &resolved.group_cols {
            let part = columns[col_idx]
                .map(|data| match ty {
                    ColumnType::Symbol => {
                        if let ColumnData::Symbol(ids) = data {
                            ids[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Int64 => {
                        if let ColumnData::Int64(v) = data {
                            v[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Float64 => {
                        if let ColumnData::Float64(v) = data {
                            v[row_idx].to_bits()
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Timestamp => {
                        if let ColumnData::Timestamp(v) = data {
                            v[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                })
                .unwrap_or(u64::MAX);
            key.push(part);
        }

        let accums = groups
            .entry(key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    // Resolve integer keys to string keys: "bucket_ts\0group1\0group2"
    let mut result = GroupedAggResult::new(num_aggs);
    for (key_parts, accums) in groups {
        let bucket_ts = key_parts[0] as i64;
        let mut str_key = bucket_ts.to_string();

        for (i, &part) in key_parts[1..].iter().enumerate() {
            str_key.push('\0');
            if i < resolved.group_cols.len() {
                let (col_idx, ty) = resolved.group_cols[i];
                match ty {
                    ColumnType::Symbol => {
                        if let Some(dict) = sym_lookup(col_idx)
                            && let Some(name) = dict.get(part as u32)
                        {
                            str_key.push_str(name);
                        }
                    }
                    ColumnType::Int64 | ColumnType::Timestamp => {
                        use std::fmt::Write;
                        let _ = write!(str_key, "{}", part as i64);
                    }
                    ColumnType::Float64 => {
                        use std::fmt::Write;
                        let _ = write!(str_key, "{}", f64::from_bits(part));
                    }
                }
            }
        }

        let entry = result
            .groups
            .entry(str_key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        for (i, a) in accums.iter().enumerate() {
            entry[i].merge(a);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

/// FxHash-style multiplicative hash over a slice of u64 values.
///
/// Used by `aggregate_two_level` to assign rows to buckets in Phase 1.
#[inline]
fn fx_hash_key(key: &[u64]) -> u64 {
    let mut hash: u64 = 0;
    for &v in key {
        hash = hash.wrapping_mul(0x517cc1b727220a95).wrapping_add(v);
    }
    hash
}

fn build_generic_key(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    row_idx: usize,
) -> Vec<u64> {
    resolved
        .group_cols
        .iter()
        .map(|&(idx, ty)| {
            columns[idx]
                .map(|data| match ty {
                    ColumnType::Symbol => {
                        if let ColumnData::Symbol(ids) = data {
                            ids[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Int64 => {
                        if let ColumnData::Int64(vals) = data {
                            vals[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Float64 => {
                        if let ColumnData::Float64(vals) = data {
                            vals[row_idx].to_bits()
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Timestamp => {
                        if let ColumnData::Timestamp(vals) = data {
                            vals[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                })
                .unwrap_or(u64::MAX)
        })
        .collect()
}

fn resolve_group_key<'a>(
    key: &IntGroupKey,
    resolved: &ResolvedSchema,
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) -> String {
    match key {
        IntGroupKey::None => String::new(),
        IntGroupKey::SingleU32(id) => {
            let col_idx = resolved.group_cols[0].0;
            if resolved.group_cols[0].1 == ColumnType::Symbol {
                sym_lookup(col_idx)
                    .and_then(|d: &nodedb_types::timeseries::SymbolDictionary| d.get(*id))
                    .unwrap_or("")
                    .to_string()
            } else {
                id.to_string()
            }
        }
        IntGroupKey::Multi(parts) => {
            let mut s = String::with_capacity(parts.len() * 16);
            for (i, &part) in parts.iter().enumerate() {
                if i > 0 {
                    s.push('\0');
                }
                let (col_idx, ty) = resolved.group_cols[i];
                match ty {
                    ColumnType::Symbol => {
                        if let Some(dict) = sym_lookup(col_idx)
                            && let Some(name) = dict.get(part as u32)
                        {
                            s.push_str(name);
                        }
                    }
                    ColumnType::Int64 | ColumnType::Timestamp => {
                        use std::fmt::Write;
                        let _ = write!(s, "{}", part as i64);
                    }
                    ColumnType::Float64 => {
                        use std::fmt::Write;
                        let _ = write!(s, "{}", f64::from_bits(part));
                    }
                }
            }
            s
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::columnar_memtable::ColumnType;
    use super::super::types::AggColInfo;
    use super::super::types::ResolvedSchema;
    use super::*;

    fn make_resolved_count(col_types: &[(usize, ColumnType)]) -> ResolvedSchema {
        ResolvedSchema {
            group_cols: col_types.to_vec(),
            agg_cols: vec![AggColInfo::CountStar],
            ts_idx: 0,
        }
    }

    /// Build a full-set mask for `row_count` rows.
    fn full_mask(row_count: usize) -> Vec<u64> {
        let words = row_count.div_ceil(64);
        let mut mask = vec![u64::MAX; words];
        let rem = row_count % 64;
        if rem != 0 {
            *mask.last_mut().unwrap() = (1u64 << rem) - 1;
        }
        mask
    }

    /// Collect aggregation results into a sorted Vec of (key, count) for comparison.
    fn collect_counts(results: Vec<(IntGroupKey, Vec<AggAccum>)>) -> Vec<(Vec<u64>, u64)> {
        let mut out: Vec<(Vec<u64>, u64)> = results
            .into_iter()
            .map(|(key, accums)| {
                let k = match key {
                    IntGroupKey::Multi(v) => v,
                    IntGroupKey::SingleU32(v) => vec![v as u64],
                    IntGroupKey::None => vec![],
                };
                let count = accums.first().map(|a| a.count).unwrap_or(0);
                (k, count)
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[test]
    fn two_level_matches_generic_multi_column() {
        // Build two Int64 columns with 8 distinct combinations repeated many times.
        let n = 200_000usize;
        let col_a: Vec<i64> = (0..n).map(|i| (i % 4) as i64).collect();
        let col_b: Vec<i64> = (0..n).map(|i| (i % 2) as i64).collect();

        let data_a = ColumnData::Int64(col_a);
        let data_b = ColumnData::Int64(col_b);
        let columns: Vec<Option<&ColumnData>> = vec![Some(&data_a), Some(&data_b)];

        let resolved = make_resolved_count(&[(0, ColumnType::Int64), (1, ColumnType::Int64)]);
        let mask = full_mask(n);

        // num_aggs = 1 so AggAccum::count tracks row membership.
        let generic = aggregate_hash_generic(&resolved, &columns, &mask, n, 1);
        let two_level = aggregate_two_level(&resolved, &columns, &mask, n, 1);

        assert_eq!(
            collect_counts(generic),
            collect_counts(two_level),
            "two-level and generic must produce identical group counts"
        );
    }

    #[test]
    fn fx_hash_key_deterministic() {
        let key = vec![1u64, 2, 3, 4];
        assert_eq!(fx_hash_key(&key), fx_hash_key(&key));
        assert_ne!(fx_hash_key(&[1, 2]), fx_hash_key(&[2, 1]));
    }
}
