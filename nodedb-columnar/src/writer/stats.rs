// SPDX-License-Identifier: Apache-2.0

//! Statistics computation helpers: numeric min/max, string zone-maps, bloom filters.

use crate::error::ColumnarError;
use crate::format::{BlockStats, BloomFilter};
use crate::predicate::{BLOOM_BITS_DEFAULT, BLOOM_K_DEFAULT, bloom_insert};

/// Maximum byte length for string zone-map bounds (truncation threshold).
pub(super) const STRING_BOUND_MAX_BYTES: usize = 32;

/// Minimum distinct values in a block to justify bloom filter overhead (256 bytes).
/// Below this threshold, zone-map min/max is sufficient. Dict-encoded columns
/// with ≤16 distinct values get no bloom.
const BLOOM_DISTINCT_THRESHOLD: usize = 16;

/// The cells of one string column block, for its statistics.
pub(super) struct StringBlock<'a> {
    /// Column name, for the error a non-UTF-8 cell returns.
    pub column: &'a str,
    pub data: &'a [u8],
    pub offsets: &'a [u32],
    pub valid_slice: &'a [bool],
    pub start: usize,
    pub end: usize,
    pub null_count: u32,
    pub block_row_count: usize,
}

/// Compute `BlockStats` for a string column block.
///
/// Iterates over all non-null values in `[start, end)`, building lexicographic
/// min/max (each truncated to `STRING_BOUND_MAX_BYTES` bytes) and a 256-byte
/// bloom filter for equality-predicate fast-reject.
///
/// `Err(StringCellNotUtf8)` for a cell that is not UTF-8. The memtable push
/// stores only UTF-8, so such a cell is corruption. Bounds built from it
/// would prune rows a predicate matches.
pub(super) fn compute_string_block_stats(
    block: StringBlock<'_>,
) -> Result<BlockStats, ColumnarError> {
    let StringBlock {
        column,
        data,
        offsets,
        valid_slice,
        start,
        end,
        null_count,
        block_row_count,
    } = block;
    let mut cell_min: Option<&str> = None;
    let mut cell_max: Option<&str> = None;
    let mut distinct = std::collections::HashSet::new();
    let mut cells: Vec<&str> = Vec::with_capacity(end - start);

    // First pass: check UTF-8, compute min/max and count distinct values.
    for (&is_valid, row_idx) in valid_slice.iter().zip(start..end) {
        if !is_valid {
            continue;
        }
        let b_start = offsets[row_idx] as usize;
        let b_end = offsets[row_idx + 1] as usize;
        let raw = &data[b_start..b_end];
        let s = std::str::from_utf8(raw).map_err(|_| {
            let err = ColumnarError::StringCellNotUtf8 {
                column: column.to_string(),
                row: row_idx,
            };
            crate::diag::string_cell_not_utf8(&err);
            err
        })?;
        cells.push(s);
        // Track distinct values up to threshold+1 (stop counting early).
        if distinct.len() <= BLOOM_DISTINCT_THRESHOLD {
            distinct.insert(raw);
        }

        if cell_min.is_none_or(|cur| s < cur) {
            cell_min = Some(s);
        }
        if cell_max.is_none_or(|cur| s > cur) {
            cell_max = Some(s);
        }
    }
    // A prefix of the least cell is a lower bound. The upper bound must not
    // fall below the greatest cell, so a truncated maximum is raised.
    let str_min = cell_min.map(|s| truncate_to_char_boundary(s, STRING_BOUND_MAX_BYTES).to_owned());
    let str_max = cell_max.and_then(|s| upper_bound_prefix(s, STRING_BOUND_MAX_BYTES));

    // Only build bloom filter for high-cardinality blocks where zone maps
    // alone cannot efficiently prune. Low-cardinality columns (≤16 distinct)
    // are better served by dict encoding + integer comparison.
    let bloom_opt = if !cells.is_empty() && distinct.len() > BLOOM_DISTINCT_THRESHOLD {
        let byte_count = (BLOOM_BITS_DEFAULT as usize).div_ceil(8);
        let mut bloom = BloomFilter {
            k: BLOOM_K_DEFAULT,
            m: BLOOM_BITS_DEFAULT,
            bytes: vec![0u8; byte_count],
        };
        for s in &cells {
            bloom_insert(&mut bloom, s);
        }
        Some(bloom)
    } else {
        None
    };
    Ok(BlockStats::string_block(
        null_count,
        block_row_count as u32,
        str_min,
        str_max,
        bloom_opt,
    ))
}

/// Truncate a string to at most `max_bytes` bytes, preserving valid UTF-8 by
/// cutting only at character boundaries.
pub(super) fn truncate_to_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut boundary = max_bytes;
    while !s.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &s[..boundary]
}

/// A string of about `max_bytes` bytes that is not less than `s`.
///
/// `s` itself when it fits. Otherwise its prefix with the last character
/// raised by one, so every string with that prefix sorts below it. `None`
/// when no character of the prefix can be raised: the block then carries
/// no upper bound.
pub(super) fn upper_bound_prefix(s: &str, max_bytes: usize) -> Option<String> {
    if s.len() <= max_bytes {
        return Some(s.to_owned());
    }
    let mut chars: Vec<char> = truncate_to_char_boundary(s, max_bytes).chars().collect();
    while let Some(last) = chars.pop() {
        if let Some(next) = next_char(last) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// The character after `c` in code point order, skipping the surrogate
/// range. `None` for `char::MAX`.
fn next_char(c: char) -> Option<char> {
    let mut code = u32::from(c) + 1;
    if (0xD800..=0xDFFF).contains(&code) {
        code = 0xE000;
    }
    char::from_u32(code)
}

/// Compute min/max for i64 values, skipping nulls.
pub(super) fn numeric_min_max_i64(values: &[i64], valid: &[bool]) -> (i64, i64) {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for (v, &is_valid) in values.iter().zip(valid.iter()) {
        if is_valid {
            min = min.min(*v);
            max = max.max(*v);
        }
    }
    if min == i64::MAX {
        (0, 0) // All nulls.
    } else {
        (min, max)
    }
}

/// Compute min/max for f64 values, skipping nulls.
pub(super) fn numeric_min_max_f64(values: &[f64], valid: &[bool]) -> (f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for (v, &is_valid) in values.iter().zip(valid.iter()) {
        if is_valid {
            if *v < min {
                min = *v;
            }
            if *v > max {
                max = *v;
            }
        }
    }
    if min == f64::INFINITY {
        (0.0, 0.0)
    } else {
        (min, max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats_of(cells: &[&[u8]]) -> Result<BlockStats, ColumnarError> {
        let mut data = Vec::new();
        let mut offsets = vec![0u32];
        for cell in cells {
            data.extend_from_slice(cell);
            offsets.push(data.len() as u32);
        }
        let valid = vec![true; cells.len()];
        compute_string_block_stats(StringBlock {
            column: "s",
            data: &data,
            offsets: &offsets,
            valid_slice: &valid,
            start: 0,
            end: cells.len(),
            null_count: 0,
            block_row_count: cells.len(),
        })
    }

    #[test]
    fn a_cell_that_is_not_utf8_refuses_the_block() {
        let result = stats_of(&[b"ok", &[0xC3, 0x28]]);
        assert!(matches!(
            result,
            Err(ColumnarError::StringCellNotUtf8 { ref column, row: 1 }) if column == "s"
        ));
    }

    #[test]
    fn a_long_maximum_keeps_an_upper_bound_above_every_cell() {
        let long = "a".repeat(STRING_BOUND_MAX_BYTES + 8);
        let stats = stats_of(&[b"a", long.as_bytes()]).expect("stats");
        let max = stats.str_max.expect("max");
        assert!(max.as_str() >= long.as_str(), "{max} is below {long}");
        assert_eq!(stats.str_min.as_deref(), Some("a"));
    }

    #[test]
    fn the_upper_bound_raises_the_last_raisable_character() {
        let s = format!("{}{}", "b".repeat(STRING_BOUND_MAX_BYTES - 1), char::MAX);
        let tail = format!("{s}zzz");
        assert_eq!(
            upper_bound_prefix(&tail, STRING_BOUND_MAX_BYTES + 3),
            Some(format!("{}c", "b".repeat(STRING_BOUND_MAX_BYTES - 2)))
        );
        assert_eq!(upper_bound_prefix("short", 32), Some("short".to_string()));
        assert_eq!(
            upper_bound_prefix(&char::MAX.to_string().repeat(3), 4),
            None
        );
    }
}
