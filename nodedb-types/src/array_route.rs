// SPDX-License-Identifier: Apache-2.0

//! Routing bucket and vShard of an array tile.
//!
//! A tile key orders tiles on a space-filling curve, so its top bits carry
//! little entropy when tile indices are small. Routing therefore takes the
//! top bits of a bijective mix of the key. Stored keys stay unchanged.
//! The cluster router, the Data Plane shard filter and the snapshot paths
//! use these functions, so a tile has one bucket and one vShard everywhere.
//!
//! The router uses at most [`VShardId::BITS`] bucket bits. Bucket `b` of `R`
//! routing bits owns the vShards `[b * stride, (b + 1) * stride)`, with
//! `stride = VShardId::COUNT >> R`. Its primary vShard is the one
//! [`bucket_to_vshard`] picks inside that range.

use serde::{Deserialize, Serialize};

use crate::id::VShardId;

/// The largest routing granularity an array declares.
pub const MAX_PREFIX_BITS: u8 = 16;

/// An array routing granularity outside `1..=MAX_PREFIX_BITS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("array routing: prefix_bits must be 1-{}, got {}", MAX_PREFIX_BITS, .0)]
pub struct PrefixBitsOutOfRange(pub u8);

/// MurmurHash3 64-bit finalizer. A bijection on `u64`: distinct keys keep
/// distinct values, and every input bit affects every output bit.
pub const fn fmix64(key: u64) -> u64 {
    let mut k = key;
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// The routing bucket of tile key `tile_key`: the top `prefix_bits` bits of
/// `fmix64(tile_key)`. Returns a value in `[0, 2^prefix_bits)`. With
/// `prefix_bits == 0` every key maps to bucket 0.
pub const fn prefix_bucket(tile_key: u64, prefix_bits: u8) -> u32 {
    let bits = if prefix_bits > 32 { 32 } else { prefix_bits };
    if bits == 0 {
        return 0;
    }
    (fmix64(tile_key) >> (64 - bits as u32)) as u32
}

/// The bucket one shard serves. A shard read keeps only the tiles whose
/// key falls in this bucket, so shards that share one Data Plane never
/// return the same tile twice.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ShardBucket {
    /// Bucket index in `[0, 2^prefix_bits)`.
    pub bucket: u32,
    /// Number of mixed key bits the bucket is taken from.
    pub prefix_bits: u8,
}

impl ShardBucket {
    /// Whether tile key `tile_key` routes to this bucket.
    pub const fn contains(&self, tile_key: u64) -> bool {
        prefix_bucket(tile_key, self.prefix_bits) == self.bucket
    }
}

/// Whether `tile_key` passes the optional shard filter. `None` keeps every
/// tile.
pub fn shard_bucket_admits(filter: Option<ShardBucket>, tile_key: u64) -> bool {
    filter.is_none_or(|b| b.contains(tile_key))
}

/// The bucket bits the router uses for `prefix_bits`: at most
/// [`VShardId::BITS`], since a finer bucket has no vShard of its own.
pub fn routing_bits(prefix_bits: u8) -> Result<u8, PrefixBitsOutOfRange> {
    if prefix_bits == 0 || prefix_bits > MAX_PREFIX_BITS {
        return Err(PrefixBitsOutOfRange(prefix_bits));
    }
    Ok(prefix_bits.min(VShardId::BITS))
}

/// The primary vShard of `bucket` at `bits` routing bits. `bits` comes from
/// [`routing_bits`], so it is at most [`VShardId::BITS`].
///
/// A Data Plane core owns `vshard % num_cores`. A primary of `bucket * stride`
/// alone is a multiple of `stride`, so every primary lands on few cores. The
/// offset inside the stride is the bucket's top bits. The low bits of a
/// primary then hold the bucket's low bits and its top bits, and a
/// power-of-two core count up to `2^bits` owns an equal share of primaries.
/// The offset stays below `stride`, so `vshard >> (VShardId::BITS - bits)`
/// recovers the bucket.
pub const fn bucket_to_vshard(bucket: u32, bits: u8) -> u32 {
    let stride_bits = (VShardId::BITS - bits) as u32;
    let bits = bits as u32;
    let top_shift = bits.saturating_sub(stride_bits);
    let offset = (bucket >> top_shift) & ((1u32 << stride_bits) - 1);
    (bucket << stride_bits) | offset
}

/// The primary vShard that owns the tile with key `tile_key`.
///
/// `prefix_bits` is the array's routing granularity (1–16). Returns a vShard
/// ID in `[0, VShardId::COUNT)`.
pub fn array_vshard_for_tile(tile_key: u64, prefix_bits: u8) -> Result<u32, PrefixBitsOutOfRange> {
    let bits = routing_bits(prefix_bits)?;
    Ok(bucket_to_vshard(prefix_bucket(tile_key, bits), bits))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmix64_is_injective_on_a_sample() {
        let mut seen = std::collections::HashSet::new();
        for k in 0u64..100_000 {
            assert!(seen.insert(fmix64(k)), "collision at {k}");
        }
        assert_eq!(fmix64(0), 0);
    }

    #[test]
    fn prefix_bucket_stays_in_range() {
        for bits in 1u8..=16 {
            for k in [0u64, 1, 2, 63, 1 << 40, u64::MAX / 3, u64::MAX] {
                assert!(prefix_bucket(k, bits) < (1u32 << bits), "bits={bits} k={k}");
            }
        }
        assert_eq!(prefix_bucket(u64::MAX, 0), 0);
    }

    #[test]
    fn small_keys_spread_over_buckets() {
        // Keys whose top bits are all zero land in most of the 256 buckets.
        let buckets: std::collections::HashSet<u32> =
            (0u64..4096).map(|k| prefix_bucket(k, 8)).collect();
        assert!(
            buckets.len() >= 240,
            "only {} of 256 buckets used",
            buckets.len()
        );
    }

    #[test]
    fn shard_bucket_partitions_keys() {
        let bits = 4u8;
        for k in 0u64..2048 {
            let owners = (0u32..(1 << bits))
                .filter(|&b| {
                    ShardBucket {
                        bucket: b,
                        prefix_bits: bits,
                    }
                    .contains(k)
                })
                .count();
            assert_eq!(owners, 1, "key {k} has {owners} owners");
        }
        assert!(shard_bucket_admits(None, 7));
    }

    #[test]
    fn tile_routing_invalid_prefix_bits() {
        assert_eq!(array_vshard_for_tile(0, 0), Err(PrefixBitsOutOfRange(0)));
        assert_eq!(array_vshard_for_tile(0, 17), Err(PrefixBitsOutOfRange(17)));
    }

    #[test]
    fn tile_routing_uses_the_mixed_bucket() {
        // prefix_bits=8: stride=4, so the vShard is the mixed bucket times 4
        // plus the bucket's top two bits.
        for key in [0u64, 1, 2, 0x0100_0000_0000_0000, u64::MAX] {
            let vshard = array_vshard_for_tile(key, 8).unwrap();
            let bucket = prefix_bucket(key, 8);
            assert_eq!(vshard, bucket * 4 + (bucket >> 6), "key={key:#x}");
            assert_eq!(vshard >> 2, bucket, "key={key:#x}");
        }
        // prefix_bits=10: stride=1, so the vShard is the mixed bucket.
        let key = 0x0040_0000_0000_0000;
        assert_eq!(
            array_vshard_for_tile(key, 10).unwrap(),
            prefix_bucket(key, 10)
        );
    }

    #[test]
    fn tile_routing_output_in_vshard_range() {
        // Every vShard returned is below VShardId::COUNT, including
        // prefix_bits above VShardId::BITS.
        for bits in 1u8..=MAX_PREFIX_BITS {
            for &key in &[0u64, 1, u64::MAX / 3, u64::MAX / 2, u64::MAX] {
                let vshard = array_vshard_for_tile(key, bits).unwrap();
                assert!(
                    vshard < VShardId::COUNT,
                    "bits={bits} key={key:#x} → vshard={vshard}"
                );
            }
        }
    }

    #[test]
    fn prefix_bits_above_vshard_bits_route_like_vshard_bits() {
        for key in [3u64, 99, 1 << 50, u64::MAX - 7] {
            let at_max = array_vshard_for_tile(key, VShardId::BITS).unwrap();
            for bits in VShardId::BITS..=MAX_PREFIX_BITS {
                assert_eq!(array_vshard_for_tile(key, bits).unwrap(), at_max);
            }
        }
    }

    #[test]
    fn small_tile_keys_spread_over_many_vshards() {
        // Keys whose top bits are all zero reach most of the 256 buckets.
        let vshards: std::collections::BTreeSet<u32> = (0u64..4096)
            .map(|key| array_vshard_for_tile(key, 8).unwrap())
            .collect();
        assert!(
            vshards.len() >= 240,
            "only {} of 256 buckets used",
            vshards.len()
        );
    }

    #[test]
    fn bucket_to_vshard_strides() {
        assert_eq!(VShardId::COUNT, 1 << VShardId::BITS);
        assert_eq!(VShardId::BITS, 10);
        assert_eq!(bucket_to_vshard(0, 1), 0);
        assert_eq!(bucket_to_vshard(1, 1), 513);
        assert_eq!(bucket_to_vshard(1, 8), 4);
        assert_eq!(bucket_to_vshard(255, 8), 1023);
        assert_eq!(bucket_to_vshard(64, 8), 257);
        assert_eq!(bucket_to_vshard(1, 10), 1);
        assert_eq!(routing_bits(16), Ok(10));
    }

    /// Every bucket's primary sits in the bucket's own stride, and the shift
    /// recovers the bucket.
    #[test]
    fn the_shift_recovers_the_bucket_of_every_primary() {
        for prefix_bits in 1u8..=MAX_PREFIX_BITS {
            let bits = routing_bits(prefix_bits).unwrap();
            let shift = u32::from(VShardId::BITS - bits);
            for bucket in 0u32..(1 << bits) {
                let vshard = bucket_to_vshard(bucket, bits);
                assert!(vshard < VShardId::COUNT, "bits={bits} bucket={bucket}");
                assert_eq!(vshard >> shift, bucket, "bits={bits} bucket={bucket}");
            }
        }
    }

    /// A core owns `vshard % num_cores`. With the default `prefix_bits`, a
    /// 4-core and an 8-core node own an equal share of the primaries.
    #[test]
    fn default_prefix_bits_put_primaries_on_every_core() {
        let bits = routing_bits(8).unwrap();
        for num_cores in [2u32, 4, 8, 16] {
            let mut per_core = vec![0u32; num_cores as usize];
            for bucket in 0u32..(1 << bits) {
                per_core[(bucket_to_vshard(bucket, bits) % num_cores) as usize] += 1;
            }
            let share = (1u32 << bits) / num_cores;
            assert!(
                per_core.iter().all(|&n| n == share),
                "{num_cores} cores: primaries per core {per_core:?}"
            );
        }
    }

    /// Every routing granularity spreads its primaries over as many cores as
    /// it has buckets, up to eight.
    #[test]
    fn every_prefix_bits_spreads_over_the_cores() {
        for prefix_bits in 1u8..=MAX_PREFIX_BITS {
            let bits = routing_bits(prefix_bits).unwrap();
            let num_cores = (1u32 << bits).min(8);
            let cores: std::collections::BTreeSet<u32> = (0u32..(1 << bits))
                .map(|bucket| bucket_to_vshard(bucket, bits) % num_cores)
                .collect();
            assert_eq!(cores.len() as u32, num_cores, "bits={bits}");
        }
    }

    #[test]
    fn shard_bucket_roundtrips_through_msgpack() {
        let b = ShardBucket {
            bucket: 41,
            prefix_bits: 8,
        };
        let bytes = zerompk::to_msgpack_vec(&b).expect("encode");
        let back: ShardBucket = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(b, back);
    }
}
