// SPDX-License-Identifier: BUSL-1.1

//! vShard routing for array tiles.
//!
//! A tile routes by the top bits of `fmix64(tile_key)`. The tile-to-vShard
//! functions live in `nodedb_types::array_route`, shared with the Data
//! Plane. This module adds the read-side bucket filter and slice fan-out.
//!
//! Bucket-to-vShard is a fixed function of `prefix_bits`, and vShard-to-node
//! lives in `RoutingTable`. A vShard rebalance therefore moves array tiles
//! with no array-specific routing entry in the metadata log.

pub use nodedb_types::array_route::array_vshard_for_tile;
use nodedb_types::array_route::{ShardBucket, bucket_to_vshard, prefix_bucket, routing_bits};
use nodedb_types::id::VShardId;

use crate::error::Result;

/// The bucket filter a read sent to primary vShard `vshard_id` applies, so
/// the shard returns only the tiles it owns.
pub fn shard_bucket_for_vshard(vshard_id: u32, prefix_bits: u8) -> Result<ShardBucket> {
    let bits = routing_bits(prefix_bits)?;
    Ok(ShardBucket {
        bucket: vshard_id >> u32::from(VShardId::BITS - bits),
        prefix_bits: bits,
    })
}

/// The primary vShards that own the tiles in `slice_tile_keys`.
///
/// `slice_tile_keys` — tile keys covering the slice, at least one per
/// bucket the slice touches. An empty list means an unbounded slice, which
/// returns the primary vShard of every bucket.
///
/// `prefix_bits` — routing granularity (1–16).
/// `total_shards` — number of active vShards. IDs at or above it are dropped.
///
/// Returns a sorted, deduplicated list of vShard IDs.
pub fn array_vshards_for_slice(
    slice_tile_keys: &[u64],
    prefix_bits: u8,
    total_shards: u32,
) -> Result<Vec<u32>> {
    if total_shards == 0 {
        return Ok(Vec::new());
    }
    let bits = routing_bits(prefix_bits)?;

    // One primary vShard per bucket. The other vShards of a bucket serve the
    // same tiles, so a fan-out to them double-counts aggregates.
    let mut out: Vec<u32> = if slice_tile_keys.is_empty() {
        (0..(1u32 << bits))
            .map(|bucket| bucket_to_vshard(bucket, bits))
            .collect()
    } else {
        slice_tile_keys
            .iter()
            .map(|&key| bucket_to_vshard(prefix_bucket(key, bits), bits))
            .collect()
    };
    out.retain(|&vshard| vshard < total_shards);
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::VSHARD_COUNT;

    #[test]
    fn invalid_prefix_bits_are_refused() {
        assert!(shard_bucket_for_vshard(0, 0).is_err());
        assert!(shard_bucket_for_vshard(0, 17).is_err());
        assert!(array_vshards_for_slice(&[], 0, VSHARD_COUNT).is_err());
    }

    #[test]
    fn shard_bucket_admits_exactly_the_tiles_routed_to_its_vshard() {
        for bits in [1u8, 4, 8, 10, 12, 16] {
            let primaries = array_vshards_for_slice(&[], bits, VSHARD_COUNT).unwrap();
            for key in 0u64..512 {
                let owner = array_vshard_for_tile(key, bits).unwrap();
                let admitting: Vec<u32> = primaries
                    .iter()
                    .copied()
                    .filter(|&v| shard_bucket_for_vshard(v, bits).unwrap().contains(key))
                    .collect();
                assert_eq!(admitting, vec![owner], "bits={bits} key={key}");
            }
        }
    }

    #[test]
    fn slice_routing_zero_shards() {
        let shards = array_vshards_for_slice(&[0, u64::MAX], 8, 0).unwrap();
        assert!(shards.is_empty());
    }

    #[test]
    fn slice_routing_empty_keys_returns_every_primary() {
        // prefix_bits=8 (stride=4): one primary in each stride of four.
        let shards = array_vshards_for_slice(&[], 8, VSHARD_COUNT).unwrap();
        assert_eq!(shards.len(), 256);
        let buckets: Vec<u32> = shards.iter().map(|v| v >> 2).collect();
        assert_eq!(buckets, (0u32..256).collect::<Vec<_>>());
        // total_shards=4 keeps only primary 0.
        assert_eq!(array_vshards_for_slice(&[], 8, 4).unwrap(), vec![0]);
    }

    #[test]
    fn slice_routing_maps_each_key_to_its_owner() {
        let keys = [5u64, 6, 7, 1 << 40];
        let shards = array_vshards_for_slice(&keys, 8, VSHARD_COUNT).unwrap();
        let mut expected: Vec<u32> = keys
            .iter()
            .map(|&k| array_vshard_for_tile(k, 8).unwrap())
            .collect();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(shards, expected);
    }

    #[test]
    fn slice_routing_dedups_keys_of_one_bucket() {
        let key = 42u64;
        let shards = array_vshards_for_slice(&[key, key], 8, VSHARD_COUNT).unwrap();
        assert_eq!(shards, vec![array_vshard_for_tile(key, 8).unwrap()]);
    }
}
