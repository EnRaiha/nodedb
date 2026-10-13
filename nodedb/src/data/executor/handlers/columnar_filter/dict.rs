// SPDX-License-Identifier: BUSL-1.1

//! DictEncoded-specific filter paths.

use crate::bridge::scan_filter::ScanFilter;

/// Evaluate a dict-encoded filter against a sparse index set.
///
/// Called from `eval_filters_sparse`.
pub(super) fn eval_dict_filter_sparse(
    ids: &[u32],
    dictionary: &[String],
    reverse: &std::collections::HashMap<String, u32>,
    valid: &[bool],
    f: &ScanFilter,
    indices: &[u32],
    mask: &mut [bool],
) -> Option<()> {
    let filter_str = f.value.as_str()?;

    match f.op.as_str() {
        "eq" => {
            if let Some(&target_id) = reverse.get(filter_str) {
                for (mi, &idx) in indices.iter().enumerate() {
                    if mask[mi] {
                        let row = idx as usize;
                        mask[mi] = row < ids.len()
                            && valid.get(row).copied().unwrap_or(false)
                            && ids[row] == target_id;
                    }
                }
            } else {
                // Value not in dictionary — no rows can match.
                for m in mask.iter_mut() {
                    *m = false;
                }
            }
        }
        "ne" => {
            if let Some(&target_id) = reverse.get(filter_str) {
                for (mi, &idx) in indices.iter().enumerate() {
                    if mask[mi] {
                        let row = idx as usize;
                        // null rows (valid=false) fail the ne predicate.
                        mask[mi] = row < ids.len()
                            && valid.get(row).copied().unwrap_or(false)
                            && ids[row] != target_id;
                    }
                }
            }
            // If value not in dict, all valid rows pass ne — mask unchanged.
        }
        "contains" => {
            let matching_ids: std::collections::HashSet<u32> = dictionary
                .iter()
                .enumerate()
                .filter(|(_, v)| v.contains(filter_str))
                .map(|(i, _)| i as u32)
                .collect();
            for (mi, &idx) in indices.iter().enumerate() {
                if mask[mi] {
                    let row = idx as usize;
                    mask[mi] = row < ids.len()
                        && valid.get(row).copied().unwrap_or(false)
                        && matching_ids.contains(&ids[row]);
                }
            }
        }
        _ => return None,
    }

    Some(())
}

/// Evaluate a dict-encoded filter using SIMD kernels, returning a bitmask.
pub(super) fn eval_dict_filter_bitmask(
    ids: &[u32],
    dictionary: &[String],
    reverse: &std::collections::HashMap<String, u32>,
    valid: &[bool],
    f: &ScanFilter,
    row_count: usize,
    rt: &nodedb_query::simd_filter::FilterSimdRuntime,
) -> Option<Vec<u64>> {
    use nodedb_query::simd_filter;

    let filter_str = f.value.as_str()?;
    let slice = &ids[..row_count.min(ids.len())];
    let valid_mask = build_validity_bitmask(valid, row_count);

    match f.op.as_str() {
        "eq" => {
            if let Some(&target_id) = reverse.get(filter_str) {
                let id_mask = (rt.eq_u32)(slice, target_id);
                Some(simd_filter::bitmask_and(&id_mask, &valid_mask))
            } else {
                // Value absent from dictionary → zero rows match.
                Some(vec![0u64; simd_filter::words_for(row_count)])
            }
        }
        "ne" => {
            if let Some(&target_id) = reverse.get(filter_str) {
                let id_mask = (rt.ne_u32)(slice, target_id);
                Some(simd_filter::bitmask_and(&id_mask, &valid_mask))
            } else {
                // Value not in dict → all valid rows pass ne.
                Some(valid_mask)
            }
        }
        "contains" => {
            // Scan dictionary once, collect matching IDs, then build bitmask.
            let matching_ids: std::collections::HashSet<u32> = dictionary
                .iter()
                .enumerate()
                .filter(|(_, v)| v.contains(filter_str))
                .map(|(i, _)| i as u32)
                .collect();

            let words = simd_filter::words_for(row_count);
            let mut bm = vec![0u64; words];
            for (i, (&id, &is_valid)) in ids.iter().zip(valid.iter()).enumerate().take(row_count) {
                if is_valid && matching_ids.contains(&id) {
                    bm[i / 64] |= 1u64 << (i % 64);
                }
            }
            Some(bm)
        }
        _ => None,
    }
}

/// Build a packed `Vec<u64>` bitmask from a `&[bool]` validity slice.
pub(super) fn build_validity_bitmask(valid: &[bool], row_count: usize) -> Vec<u64> {
    use nodedb_query::simd_filter;

    let words = simd_filter::words_for(row_count);
    let mut bm = vec![0u64; words];
    for (i, &v) in valid.iter().enumerate().take(row_count) {
        if v {
            bm[i / 64] |= 1u64 << (i % 64);
        }
    }
    bm
}
