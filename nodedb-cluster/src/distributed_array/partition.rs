// SPDX-License-Identifier: BUSL-1.1

//! Cell-to-tile partitioning for distributed array writes.
//!
//! Both `coord_put` and `coord_delete` receive a flat list of cells (or
//! coord tuples) paired with their pre-computed tile keys. This module
//! groups them by owning vShard, the vShard of the key's routing bucket
//! from `array_vshard_for_tile`.
//!
//! Callers pre-attach tile keys because the query planner already
//! holds the schema and can compute them at planning time; doing the same
//! computation again inside the coordinator would require an additional
//! schema lookup that `nodedb-cluster` intentionally avoids (it carries no
//! dependency on `nodedb-array`).
//!
//! ## Output shape
//!
//! Each element arrives as one msgpack value. A bucket is a msgpack array
//! header followed by its elements' bytes, in input order. The bucket is then
//! the msgpack encoding of `Vec<element>`, the shape the shard's Data Plane
//! decodes, so the shard passes it through without a decode.
//!
//! `partition_put_cells` returns one `PutBucket` per vShard: its
//! `representative_tile_key` and `cells_msgpack`, the encoding of
//! `Vec<ArrayPutCell>`.
//!
//! `partition_delete_coords` returns one `DeleteBucket` per vShard: its
//! `representative_tile_key` and `coords_msgpack`, the encoding of
//! `Vec<ArrayDeleteCell>`.

use std::collections::HashMap;

use crate::error::{ClusterError, Result};

use super::routing::array_vshard_for_tile;

/// One bucket produced by `partition_put_cells`.
pub struct PutBucket {
    pub vshard_id: u32,
    /// Tile key of the first cell added to this bucket. The shard handler
    /// routes it to check ownership (`validate_put_routing`).
    pub representative_tile_key: u64,
    /// The msgpack array of every cell in this bucket, in input order.
    pub cells_msgpack: Vec<u8>,
}

/// One bucket produced by `partition_delete_coords`.
pub struct DeleteBucket {
    pub vshard_id: u32,
    /// Tile key of the first coord added to this bucket. The shard handler
    /// routes it to check ownership (`validate_delete_routing`).
    pub representative_tile_key: u64,
    /// The msgpack array of every coord in this bucket, in input order.
    pub coords_msgpack: Vec<u8>,
}

/// Group `(tile_key, cell_msgpack)` pairs by owning vShard.
///
/// `cells` — each element is `(tile_key, zerompk-encoded single cell)`.
/// `prefix_bits` — routing granularity (1–16).
///
/// Returns one `PutBucket` per unique owning vShard, in the order of each
/// vShard's first cell.
pub fn partition_put_cells(cells: &[(u64, Vec<u8>)], prefix_bits: u8) -> Result<Vec<PutBucket>> {
    Ok(partition_flat(cells, prefix_bits)?
        .into_iter()
        .map(|bucket| PutBucket {
            vshard_id: bucket.vshard_id,
            representative_tile_key: bucket.representative_tile_key,
            cells_msgpack: bucket.msgpack,
        })
        .collect())
}

/// Group `(tile_key, coord_msgpack)` pairs by owning vShard.
///
/// `coords` — each element is `(tile_key, zerompk-encoded single coord)`.
/// `prefix_bits` — routing granularity (1–16).
///
/// Returns one `DeleteBucket` per unique owning vShard, in the order of each
/// vShard's first coord.
pub fn partition_delete_coords(
    coords: &[(u64, Vec<u8>)],
    prefix_bits: u8,
) -> Result<Vec<DeleteBucket>> {
    Ok(partition_flat(coords, prefix_bits)?
        .into_iter()
        .map(|bucket| DeleteBucket {
            vshard_id: bucket.vshard_id,
            representative_tile_key: bucket.representative_tile_key,
            coords_msgpack: bucket.msgpack,
        })
        .collect())
}

/// One vShard's elements as one msgpack array.
struct FlatBucket {
    vshard_id: u32,
    representative_tile_key: u64,
    msgpack: Vec<u8>,
}

/// Group `(tile_key, element_msgpack)` pairs by owning vShard. Each bucket
/// is sized once and written once: no element is copied twice.
fn partition_flat(items: &[(u64, Vec<u8>)], prefix_bits: u8) -> Result<Vec<FlatBucket>> {
    struct Slot {
        vshard_id: u32,
        representative_tile_key: u64,
        count: usize,
        bytes: usize,
    }
    let mut slot_of_vshard: HashMap<u32, usize> = HashMap::new();
    let mut slots: Vec<Slot> = Vec::new();
    let mut slot_of: Vec<usize> = Vec::with_capacity(items.len());
    // Consecutive elements mostly share a vShard, so the last one answers
    // before the map.
    let mut last: Option<(u32, usize)> = None;
    for (tile_key, element) in items {
        let vshard_id = array_vshard_for_tile(*tile_key, prefix_bits)?;
        let slot = match last {
            Some((vshard, slot)) if vshard == vshard_id => slot,
            _ => *slot_of_vshard.entry(vshard_id).or_insert_with(|| {
                slots.push(Slot {
                    vshard_id,
                    representative_tile_key: *tile_key,
                    count: 0,
                    bytes: 0,
                });
                slots.len() - 1
            }),
        };
        last = Some((vshard_id, slot));
        slots[slot].count += 1;
        slots[slot].bytes += element.len();
        slot_of.push(slot);
    }
    let mut buffers: Vec<Vec<u8>> = slots
        .iter()
        .map(|slot| {
            let mut buf = Vec::with_capacity(ARRAY_HEADER_MAX + slot.bytes);
            write_array_header(&mut buf, slot.count)?;
            Ok(buf)
        })
        .collect::<Result<_>>()?;
    for ((_, element), &slot) in items.iter().zip(&slot_of) {
        buffers[slot].extend_from_slice(element);
    }
    Ok(slots
        .into_iter()
        .zip(buffers)
        .map(|(slot, msgpack)| FlatBucket {
            vshard_id: slot.vshard_id,
            representative_tile_key: slot.representative_tile_key,
            msgpack,
        })
        .collect())
}

/// The longest msgpack array header: the `array 32` marker and a `u32`.
const ARRAY_HEADER_MAX: usize = 5;

/// Write the msgpack array header for `len` elements, in the shortest form,
/// as zerompk writes it.
fn write_array_header(buf: &mut Vec<u8>, len: usize) -> Result<()> {
    if len < 16 {
        buf.push(0x90 | len as u8);
    } else if let Ok(len) = u16::try_from(len) {
        buf.push(0xdc);
        buf.extend_from_slice(&len.to_be_bytes());
    } else {
        let len = u32::try_from(len).map_err(|_| ClusterError::Codec {
            detail: format!("partition: a bucket of {len} elements exceeds a msgpack array"),
        })?;
        buf.push(0xdd);
        buf.extend_from_slice(&len.to_be_bytes());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a fake (tile_key, cell_bytes) pair. The cell is one msgpack
    // value: `tag` as a positive fixint.
    fn cell(prefix: u64, tag: u8) -> (u64, Vec<u8>) {
        (prefix, vec![tag & 0x7f])
    }

    // Build a fake coord pair.
    fn coord(prefix: u64, tag: u8) -> (u64, Vec<u8>) {
        (prefix, vec![tag & 0x7f])
    }

    // Helper: compute expected vshard for a given tile key and prefix_bits.
    fn expected_vshard(prefix: u64, prefix_bits: u8) -> u32 {
        array_vshard_for_tile(prefix, prefix_bits).unwrap()
    }

    #[test]
    fn partition_put_empty_returns_empty() {
        let buckets = partition_put_cells(&[], 8).unwrap();
        assert!(buckets.is_empty());
    }

    #[test]
    fn partition_delete_empty_returns_empty() {
        let buckets = partition_delete_coords(&[], 8).unwrap();
        assert!(buckets.is_empty());
    }

    #[test]
    fn partition_put_single_cell_one_bucket() {
        // Key 0 mixes to 0 → bucket 0 → vShard 0.
        let cells = vec![cell(0, 0xAA)];
        let buckets = partition_put_cells(&cells, 10).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].vshard_id, 0);
        assert_eq!(buckets[0].representative_tile_key, 0);
    }

    #[test]
    fn partition_put_cells_by_tile_three_buckets() {
        // With prefix_bits=10 (stride=1), each mixed bucket is its own
        // vShard. The three keys mix to three distinct buckets.
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;
        let p2 = 0x0080_0000_0000_0000u64;

        let cells = vec![
            cell(p0, 0x01),
            cell(p1, 0x02),
            cell(p0, 0x03), // same bucket as first
            cell(p2, 0x04),
            cell(p1, 0x05), // same bucket as second
        ];

        let buckets = partition_put_cells(&cells, 10).unwrap();
        assert_eq!(buckets.len(), 3);

        // p0 and p1 each own 2 cells, p2 owns 1.
        for (key, want) in [(p0, 2), (p1, 2), (p2, 1)] {
            let vshard = expected_vshard(key, 10);
            let bucket = buckets
                .iter()
                .find(|b| b.vshard_id == vshard)
                .expect("bucket for key");
            let cells: Vec<u64> = zerompk::from_msgpack(&bucket.cells_msgpack).unwrap();
            assert_eq!(cells.len(), want, "key={key:#x}");
        }
        // A bucket holds its cells in input order.
        let first = buckets
            .iter()
            .find(|b| b.vshard_id == expected_vshard(p0, 10))
            .expect("bucket for p0");
        let cells: Vec<u64> = zerompk::from_msgpack(&first.cells_msgpack).unwrap();
        assert_eq!(cells, vec![0x01, 0x03]);
    }

    /// A bucket of more than 15 elements takes the `array 16` header, and
    /// still decodes as the flat array of its elements.
    #[test]
    fn a_large_bucket_is_one_flat_msgpack_array() {
        let cells: Vec<(u64, Vec<u8>)> = (0..40u64)
            .map(|i| (0, zerompk::to_msgpack_vec(&(i * 1_000)).unwrap()))
            .collect();
        let buckets = partition_put_cells(&cells, 10).unwrap();
        assert_eq!(buckets.len(), 1);
        let decoded: Vec<u64> = zerompk::from_msgpack(&buckets[0].cells_msgpack).unwrap();
        assert_eq!(decoded, (0..40u64).map(|i| i * 1_000).collect::<Vec<_>>());
        assert_eq!(
            buckets[0].cells_msgpack,
            zerompk::to_msgpack_vec(&decoded).unwrap(),
            "the bucket is byte-identical to zerompk's own encoding"
        );
    }

    #[test]
    fn partition_delete_coords_by_tile() {
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;

        let coords = vec![coord(p0, 0xAA), coord(p1, 0xBB), coord(p0, 0xCC)];

        let buckets = partition_delete_coords(&coords, 10).unwrap();
        assert_eq!(buckets.len(), 2);
        for (key, want) in [(p0, 2), (p1, 1)] {
            let vshard = expected_vshard(key, 10);
            let bucket = buckets
                .iter()
                .find(|b| b.vshard_id == vshard)
                .expect("bucket for key");
            let coords: Vec<u64> = zerompk::from_msgpack(&bucket.coords_msgpack).unwrap();
            assert_eq!(coords.len(), want, "key={key:#x}");
        }
    }

    #[test]
    fn partition_put_invalid_prefix_bits_errors() {
        let cells = vec![cell(0, 0x01)];
        assert!(partition_put_cells(&cells, 0).is_err());
        assert!(partition_put_cells(&cells, 17).is_err());
    }
}
