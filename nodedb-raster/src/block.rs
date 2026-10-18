// SPDX-License-Identifier: Apache-2.0

//! Decoded blocks: one typed, row-major sample vector per band.

use std::mem::size_of;

use nodedb_types::decode_bounds::checked_decode_capacity;
use tiff::decoder::DecodingResult;

use crate::error::RasterError;
use crate::layout::BlockWindow;
use crate::sample::{SampleFormatCode, SampleType};

/// One band's samples for one block, row-major, `width * height` long.
#[derive(Debug, Clone, PartialEq)]
pub enum BandSamples {
    U8(Vec<u8>),
    U16(Vec<u16>),
    I16(Vec<i16>),
    U32(Vec<u32>),
    F32(Vec<f32>),
    F64(Vec<f64>),
}

impl BandSamples {
    /// Number of samples.
    pub fn len(&self) -> usize {
        match self {
            BandSamples::U8(v) => v.len(),
            BandSamples::U16(v) => v.len(),
            BandSamples::I16(v) => v.len(),
            BandSamples::U32(v) => v.len(),
            BandSamples::F32(v) => v.len(),
            BandSamples::F64(v) => v.len(),
        }
    }

    /// True when the band holds no sample.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The sample type.
    pub fn sample_type(&self) -> SampleType {
        match self {
            BandSamples::U8(_) => SampleType::U8,
            BandSamples::U16(_) => SampleType::U16,
            BandSamples::I16(_) => SampleType::I16,
            BandSamples::U32(_) => SampleType::U32,
            BandSamples::F32(_) => SampleType::F32,
            BandSamples::F64(_) => SampleType::F64,
        }
    }

    /// Sample `index` widened to `f64`. Every supported type widens
    /// exactly.
    pub fn value_f64(&self, index: usize) -> Option<f64> {
        match self {
            BandSamples::U8(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::U16(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::I16(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::U32(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::F32(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::F64(v) => v.get(index).copied(),
        }
    }
}

/// One decoded block: its window and one sample vector per band.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterBlock {
    pub index: u32,
    pub window: BlockWindow,
    pub bands: Vec<BandSamples>,
}

/// What a decoded chunk must hold.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChunkShape {
    pub index: u32,
    pub pixels: usize,
    pub bands: usize,
    pub limit_bytes: usize,
}

/// Split a chunky chunk, `bands` samples per pixel, into one vector per
/// band.
pub(crate) fn chunky_bands(
    result: DecodingResult,
    shape: ChunkShape,
) -> Result<Vec<BandSamples>, RasterError> {
    match result {
        DecodingResult::U8(v) => deinterleave(v, shape, BandSamples::U8),
        DecodingResult::U16(v) => deinterleave(v, shape, BandSamples::U16),
        DecodingResult::I16(v) => deinterleave(v, shape, BandSamples::I16),
        DecodingResult::U32(v) => deinterleave(v, shape, BandSamples::U32),
        DecodingResult::F32(v) => deinterleave(v, shape, BandSamples::F32),
        DecodingResult::F64(v) => deinterleave(v, shape, BandSamples::F64),
        other => Err(unsupported(&other)),
    }
}

/// Crop a planar chunk to the block's pixels. A plane's rows are packed at
/// the block width, so the first `pixels` samples are the block.
pub(crate) fn planar_band(
    result: DecodingResult,
    shape: ChunkShape,
) -> Result<BandSamples, RasterError> {
    match result {
        DecodingResult::U8(v) => crop(v, shape).map(BandSamples::U8),
        DecodingResult::U16(v) => crop(v, shape).map(BandSamples::U16),
        DecodingResult::I16(v) => crop(v, shape).map(BandSamples::I16),
        DecodingResult::U32(v) => crop(v, shape).map(BandSamples::U32),
        DecodingResult::F32(v) => crop(v, shape).map(BandSamples::F32),
        DecodingResult::F64(v) => crop(v, shape).map(BandSamples::F64),
        other => Err(unsupported(&other)),
    }
}

fn crop<T>(mut samples: Vec<T>, shape: ChunkShape) -> Result<Vec<T>, RasterError> {
    if samples.len() < shape.pixels {
        return Err(RasterError::ShortBlock {
            index: shape.index,
            found: samples.len(),
            expected: shape.pixels,
        });
    }
    samples.truncate(shape.pixels);
    Ok(samples)
}

fn deinterleave<T: Copy>(
    samples: Vec<T>,
    shape: ChunkShape,
    wrap: fn(Vec<T>) -> BandSamples,
) -> Result<Vec<BandSamples>, RasterError> {
    let needed = shape.pixels.saturating_mul(shape.bands);
    if samples.len() < needed {
        return Err(RasterError::ShortBlock {
            index: shape.index,
            found: samples.len(),
            expected: needed,
        });
    }
    if shape.bands == 1 {
        return crop(samples, shape).map(|band| vec![wrap(band)]);
    }
    let capacity = checked_decode_capacity(
        shape.pixels,
        size_of::<T>(),
        samples.len(),
        shape.bands,
        usize::MAX,
        shape.limit_bytes,
    )
    .ok_or(RasterError::BlockTooLarge {
        needed_bytes: (needed as u64).saturating_mul(size_of::<T>() as u64),
        limit_bytes: shape.limit_bytes,
    })?;
    let bands = (0..shape.bands)
        .map(|band| {
            let mut out = Vec::with_capacity(capacity);
            let interleaved = samples.get(band..needed).unwrap_or_default();
            out.extend(interleaved.iter().step_by(shape.bands).copied());
            wrap(out)
        })
        .collect();
    Ok(bands)
}

/// The typed error for a sample type the open-time check refuses.
fn unsupported(result: &DecodingResult) -> RasterError {
    let (format, bits) = match result {
        DecodingResult::I8(_) => (2, 8),
        DecodingResult::I32(_) => (2, 32),
        DecodingResult::I64(_) => (2, 64),
        DecodingResult::U64(_) => (1, 64),
        DecodingResult::F16(_) => (3, 16),
        DecodingResult::U8(_) => (1, 8),
        DecodingResult::U16(_) => (1, 16),
        DecodingResult::I16(_) => (2, 16),
        DecodingResult::U32(_) => (1, 32),
        DecodingResult::F32(_) => (3, 32),
        DecodingResult::F64(_) => (3, 64),
    };
    RasterError::UnsupportedSampleType {
        format: SampleFormatCode(format),
        bits,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(pixels: usize, bands: usize) -> ChunkShape {
        ChunkShape {
            index: 0,
            pixels,
            bands,
            limit_bytes: 1 << 20,
        }
    }

    #[test]
    fn chunky_samples_split_per_band() {
        let result = DecodingResult::U16(vec![1, 10, 100, 2, 20, 200]);
        let bands = chunky_bands(result, shape(2, 3)).expect("splits");
        assert_eq!(
            bands,
            vec![
                BandSamples::U16(vec![1, 2]),
                BandSamples::U16(vec![10, 20]),
                BandSamples::U16(vec![100, 200]),
            ]
        );
    }

    #[test]
    fn single_band_keeps_its_buffer() {
        let result = DecodingResult::F32(vec![1.5, 2.5, 9.0]);
        let bands = chunky_bands(result, shape(2, 1)).expect("splits");
        assert_eq!(bands, vec![BandSamples::F32(vec![1.5, 2.5])]);
    }

    #[test]
    fn planar_plane_is_cropped_to_the_block() {
        let result = DecodingResult::I16(vec![-1, -2, -3, -4, 0, 0]);
        let band = planar_band(result, shape(4, 1)).expect("crops");
        assert_eq!(band, BandSamples::I16(vec![-1, -2, -3, -4]));
    }

    #[test]
    fn short_chunks_are_typed_errors() {
        assert!(matches!(
            chunky_bands(DecodingResult::U8(vec![1, 2, 3]), shape(2, 2)),
            Err(RasterError::ShortBlock {
                found: 3,
                expected: 4,
                ..
            })
        ));
        assert!(matches!(
            planar_band(DecodingResult::U8(vec![1]), shape(2, 1)),
            Err(RasterError::ShortBlock { .. })
        ));
    }

    #[test]
    fn deinterleave_respects_the_byte_cap() {
        let tight = ChunkShape {
            limit_bytes: 3,
            ..shape(2, 2)
        };
        assert!(matches!(
            chunky_bands(DecodingResult::U16(vec![1, 2, 3, 4]), tight),
            Err(RasterError::BlockTooLarge { .. })
        ));
    }

    #[test]
    fn unsupported_variants_are_typed_errors() {
        assert!(matches!(
            chunky_bands(DecodingResult::I8(vec![1]), shape(1, 1)),
            Err(RasterError::UnsupportedSampleType { bits: 8, .. })
        ));
        assert!(matches!(
            planar_band(DecodingResult::U64(vec![1]), shape(1, 1)),
            Err(RasterError::UnsupportedSampleType { bits: 64, .. })
        ));
    }

    #[test]
    fn values_widen_to_f64() {
        let band = BandSamples::I16(vec![-7, 3]);
        assert_eq!(band.value_f64(0), Some(-7.0));
        assert_eq!(band.value_f64(2), None);
        assert_eq!(band.sample_type(), SampleType::I16);
        assert_eq!(band.len(), 2);
    }
}
