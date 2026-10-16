// SPDX-License-Identifier: BUSL-1.1

//! The tile keys a cluster array read fans out to: one per routing bucket
//! that a box of dim ranges covers.

use nodedb_array::query::slice::DimRange;
use nodedb_array::schema::ArraySchema;
use nodedb_array::schema::DimSpec;
use nodedb_array::tile::{tile_prefix, tiles_per_dim};
use nodedb_array::types::domain::DomainBound;
use nodedb_types::array_route::prefix_bucket;

/// The most tiles a read's shard fan-out enumerates. A read covering more
/// tiles fans out to every shard.
const MAX_FAN_OUT_TILES: u128 = 4096;

/// The tile keys a read's shard fan-out covers: one key per routing bucket
/// (`prefix_bucket`), each the key of one tile in that bucket.
///
/// Every tile whose cells can match the box is enumerated, so every shard
/// that holds such a cell, or will hold one a later write lands there, is
/// covered. A transaction's read of the box then validates on each of
/// those shards. A cell routes by its tile's key (`cell_tile_prefix`), the
/// same key enumerated here.
///
/// Returns an empty vec, which fans out to every shard, when the box
/// covers more than [`MAX_FAN_OUT_TILES`] tiles, when the schema has no
/// dimension, or when a tile key fails to encode.
pub(super) fn compute_slice_tile_keys(
    schema: &ArraySchema,
    dim_ranges: &[Option<DimRange>],
    prefix_bits: u8,
) -> Vec<u64> {
    if schema.dims.is_empty() || prefix_bits == 0 || prefix_bits > 16 {
        return Vec::new();
    }
    let spans: Vec<(u64, u64)> = schema
        .dims
        .iter()
        .zip(schema.tile_extents.iter())
        .enumerate()
        .map(|(i, (dim, extent))| {
            dim_tile_span(dim, *extent, dim_ranges.get(i).and_then(Option::as_ref))
        })
        .collect();
    let tiles: u128 = spans
        .iter()
        .map(|(lo, hi)| u128::from(hi - lo) + 1)
        .fold(1u128, |acc, n| acc.saturating_mul(n));
    if tiles > MAX_FAN_OUT_TILES {
        return Vec::new();
    }

    let mut buckets: std::collections::BTreeMap<u32, u64> = std::collections::BTreeMap::new();
    let mut indices: Vec<u64> = spans.iter().map(|(lo, _)| *lo).collect();
    loop {
        let Ok(key) = tile_prefix(schema, &indices) else {
            return Vec::new();
        };
        buckets
            .entry(prefix_bucket(key, prefix_bits))
            .or_insert(key);
        // Advance the odometer over the per-dimension tile spans.
        let mut dim = 0;
        loop {
            if dim == indices.len() {
                return buckets.into_values().collect();
            }
            if indices[dim] < spans[dim].1 {
                indices[dim] += 1;
                break;
            }
            indices[dim] = spans[dim].0;
            dim += 1;
        }
    }
}

/// The inclusive tile index span along `dim` that `range` covers, the whole
/// dimension when `range` is `None`. A string dimension tiles by hash, so a
/// range on it covers every tile. A bound outside the domain is clamped to
/// the domain's tiles.
fn dim_tile_span(dim: &DimSpec, extent: u64, range: Option<&DimRange>) -> (u64, u64) {
    let last = u64::try_from(tiles_per_dim(dim, extent).saturating_sub(1)).unwrap_or(u64::MAX);
    let extent = u128::from(extent.max(1));
    let index = |bound: &DomainBound| -> Option<u64> {
        let offset: u128 = match (bound, &dim.domain.lo) {
            (DomainBound::Int64(v), DomainBound::Int64(lo))
            | (DomainBound::TimestampMs(v), DomainBound::TimestampMs(lo)) => {
                ((*v as i128) - (*lo as i128)).max(0) as u128
            }
            (DomainBound::Float64(v), DomainBound::Float64(lo)) if v.is_finite() => {
                (v - lo).max(0.0) as u128
            }
            _ => return None,
        };
        Some(u64::try_from(offset / extent).unwrap_or(u64::MAX).min(last))
    };
    match range.map(|r| (index(&r.lo), index(&r.hi))) {
        Some((Some(lo), Some(hi))) => (lo.min(hi), lo.max(hi)),
        _ => (0, last),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimType};
    use nodedb_array::types::domain::Domain;

    /// Every cell a slice can match, or a later write can land in its box,
    /// routes to a shard the slice fans out to.
    #[test]
    fn the_fan_out_covers_the_shard_of_every_cell_in_the_box() {
        use nodedb_array::types::coord::value::CoordValue;
        let schema = ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "chr".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(9)),
            ))
            .dim(DimSpec::new(
                "pos".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            ))
            .attr(AttrSpec::new("qual".to_string(), AttrType::Float64, true))
            .tile_extents(vec![1, 100])
            .build()
            .expect("schema build");
        let box_ranges = vec![
            Some(DimRange::new(DomainBound::Int64(0), DomainBound::Int64(2))),
            Some(DimRange::new(DomainBound::Int64(0), DomainBound::Int64(99))),
        ];
        let keys = compute_slice_tile_keys(&schema, &box_ranges, 8);
        assert!(!keys.is_empty(), "a three-tile slice fans out by tile");
        let fanned = nodedb_cluster::distributed_array::array_vshards_for_slice(&keys, 8, 1024)
            .expect("fan-out");
        let mut owners = std::collections::BTreeSet::new();
        for chr in 0..=2 {
            for pos in [0, 10, 20, 50, 99] {
                let coord = [CoordValue::Int64(chr), CoordValue::Int64(pos)];
                let key = nodedb_array::tile::cell_tile_prefix(&schema, &coord).expect("route");
                let owner = nodedb_cluster::distributed_array::array_vshard_for_tile(key, 8)
                    .expect("owner");
                assert!(
                    fanned.contains(&owner),
                    "cell ({chr}, {pos}) routes to vShard {owner}, outside the fan-out {fanned:?}"
                );
                owners.insert(owner);
            }
        }
        assert!(owners.len() > 1, "the slice's tiles spread over shards");
    }

    #[test]
    fn a_slice_of_more_tiles_than_the_cap_fans_out_to_every_shard() {
        let narrow = ArraySchemaBuilder::new("test_arr")
            .dim(DimSpec::new(
                "x".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(100)),
            ))
            .attr(AttrSpec::new("val".to_string(), AttrType::Float64, true))
            .tile_extents(vec![10])
            .build()
            .expect("schema build");
        let wide = ArraySchemaBuilder::new("wide")
            .dim(DimSpec::new(
                "x".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_000_000)),
            ))
            .attr(AttrSpec::new("val".to_string(), AttrType::Float64, true))
            .tile_extents(vec![1])
            .build()
            .expect("schema build");
        assert!(compute_slice_tile_keys(&wide, &[None], 8).is_empty());
        assert!(!compute_slice_tile_keys(&narrow, &[None], 8).is_empty());
    }
}
