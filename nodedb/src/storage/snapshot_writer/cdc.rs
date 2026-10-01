// SPDX-License-Identifier: BUSL-1.1

//! Content-defined chunk boundaries for snapshot images.
//!
//! A gear rolling hash picks each cut from the bytes before it. An
//! unchanged region of an image therefore cuts the same way in every base,
//! and its chunks keep the same content id.

use std::ops::Range;

use super::chunks::MAX_CHUNK_BYTES;

/// Chunk size bounds and the boundary mask width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdcParams {
    /// No cut before this many bytes, except at the image end.
    pub min: usize,
    /// A cut is forced at this many bytes.
    pub max: usize,
    /// A cut lands where the top `mask_bits` bits of the hash are zero. The
    /// mean chunk is about `min + 2^mask_bits` bytes.
    pub mask_bits: u32,
}

impl CdcParams {
    /// 256 KiB to 4 MiB chunks, about 1.25 MiB on average.
    pub const DEFAULT: Self = Self {
        min: 256 * 1024,
        max: 4 * 1024 * 1024,
        mask_bits: 20,
    };

    pub fn validate(&self) -> crate::Result<()> {
        if self.min == 0
            || self.min > self.max
            || self.max > MAX_CHUNK_BYTES
            || !(1..=63).contains(&self.mask_bits)
        {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "snapshot chunk bounds {self:?} are invalid: need \
                     0 < min <= max <= {MAX_CHUNK_BYTES} and mask_bits in 1..=63"
                ),
            });
        }
        Ok(())
    }

    fn mask(&self) -> u64 {
        u64::MAX << (64 - self.mask_bits)
    }
}

/// The gear table: 256 fixed pseudo-random words from splitmix64. It is part
/// of the chunk format, so it never changes.
const GEAR: [u64; 256] = gear_table();

const fn gear_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut state: u64 = 0;
    let mut i = 0;
    while i < 256 {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        table[i] = z ^ (z >> 31);
        i += 1;
    }
    table
}

/// Cut `data` into contiguous ranges that cover it in order. An empty image
/// is one empty range, so every image has at least one chunk.
///
/// One rolling hash runs over the whole image and is never reset at a cut.
/// It shifts one bit per byte, so after any byte it is a function of the 64
/// bytes ending there alone. A byte is a cut candidate when the top
/// `mask_bits` bits of that hash are zero. A candidate cuts when the chunk
/// it closes is longer than `min`. A chunk that reaches `max` bytes is cut
/// there. The candidates therefore depend on local content, never on the
/// offset or on where the previous cut fell, and the cuts after an edit
/// realign with the unedited image at the first candidate both accept.
pub fn cut_ranges(data: &[u8], params: CdcParams) -> Vec<Range<usize>> {
    if data.is_empty() {
        return std::iter::once(0..0).collect();
    }
    let mask = params.mask();
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut hash = 0u64;
    for (offset, &byte) in data.iter().enumerate() {
        hash = (hash << 1).wrapping_add(GEAR[usize::from(byte)]);
        let end = offset + 1;
        let len = end - start;
        if (len > params.min && hash & mask == 0) || len == params.max {
            ranges.push(start..end);
            start = end;
        }
    }
    if start < data.len() {
        ranges.push(start..data.len());
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    const SMALL: CdcParams = CdcParams {
        min: 64,
        max: 1024,
        mask_bits: 7,
    };

    fn noise(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    fn pieces<'a>(data: &'a [u8], ranges: &[Range<usize>]) -> Vec<&'a [u8]> {
        ranges.iter().map(|r| &data[r.clone()]).collect()
    }

    #[test]
    fn ranges_cover_the_image_within_bounds() {
        let data = noise(50_000, 7);
        let ranges = cut_ranges(&data, SMALL);
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, data.len());
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
        let (last, body) = ranges.split_last().unwrap();
        assert!(
            body.iter()
                .all(|r| r.len() > SMALL.min && r.len() <= SMALL.max)
        );
        assert!(last.len() <= SMALL.max);
        assert!(ranges.len() > 20, "{} chunks", ranges.len());
    }

    #[test]
    fn an_empty_image_is_one_empty_chunk() {
        let ranges = cut_ranges(&[], SMALL);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], 0..0);
    }

    #[test]
    fn inserted_bytes_leave_later_chunks_unchanged() {
        let data = noise(40_000, 11);
        let mut shifted = data.clone();
        shifted.splice(5_000..5_000, noise(333, 99));
        let before = pieces(&data, &cut_ranges(&data, SMALL));
        let after = pieces(&shifted, &cut_ranges(&shifted, SMALL));
        let shared = after.iter().filter(|chunk| before.contains(chunk)).count();
        assert!(
            shared + 4 >= before.len(),
            "{shared} of {} chunks survive an insertion",
            before.len()
        );
    }

    #[test]
    fn invalid_bounds_are_refused() {
        assert!(CdcParams::DEFAULT.validate().is_ok());
        for bad in [
            CdcParams { min: 0, ..SMALL },
            CdcParams { min: 2048, ..SMALL },
            CdcParams {
                max: MAX_CHUNK_BYTES + 1,
                ..SMALL
            },
            CdcParams {
                mask_bits: 0,
                ..SMALL
            },
            CdcParams {
                mask_bits: 64,
                ..SMALL
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }
}
