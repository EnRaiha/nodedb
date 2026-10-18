// SPDX-License-Identifier: Apache-2.0

//! The block grid of IFD 0: image size, band layout, and strip or tile
//! geometry, read once at open.

use std::io::{Read, Seek};

use tiff::decoder::{ChunkType, Decoder};
use tiff::tags::{SampleFormat, Tag};

use crate::error::RasterError;
use crate::sample::SampleType;

/// `PlanarConfiguration` value of band-separate storage.
const PLANAR_SEPARATE: u16 = 2;
/// Photometric interpretations whose samples decode unchanged:
/// BlackIsZero, RGB, CMYK, YCbCr.
const RAW_PHOTOMETRICS: [u16; 4] = [1, 2, 5, 6];

/// How a raster stores its blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    /// Full-width strips of rows.
    Strips,
    /// Rectangular tiles.
    Tiles,
}

/// How a raster stores its bands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interleave {
    /// All bands of a pixel together (`PlanarConfiguration` 1).
    Chunky,
    /// One plane per band (`PlanarConfiguration` 2).
    Planar,
}

/// The pixel rectangle one block covers. Edge blocks are cropped to the
/// image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockWindow {
    pub col_off: u32,
    pub row_off: u32,
    pub width: u32,
    pub height: u32,
}

impl BlockWindow {
    /// Pixels in the window.
    pub fn pixels(&self) -> usize {
        self.width as usize * self.height as usize
    }
}

/// The geometry every block read derives from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RasterLayout {
    pub width: u32,
    pub height: u32,
    pub bands: u16,
    pub sample_type: SampleType,
    pub kind: BlockKind,
    pub interleave: Interleave,
    /// Block width as stored. A tile can extend past the image edge.
    pub block_width: u32,
    /// Block height as stored. A strip height is clamped to the image.
    pub block_height: u32,
    pub blocks_across: u32,
    pub blocks_down: u32,
}

impl RasterLayout {
    /// Read the layout of the decoder's current image.
    pub(crate) fn read<R: Read + Seek>(
        decoder: &mut Decoder<R>,
        limit_bytes: usize,
    ) -> Result<Self, RasterError> {
        let tiff = |e| RasterError::from_tiff(e, limit_bytes);
        let photometric = decoder
            .find_tag_unsigned::<u16>(Tag::PhotometricInterpretation)
            .map_err(tiff)?
            .unwrap_or(1);
        if !RAW_PHOTOMETRICS.contains(&photometric) {
            return Err(RasterError::UnsupportedPhotometric { code: photometric });
        }
        let (width, height) = decoder.dimensions().map_err(tiff)?;
        let color = decoder.colortype().map_err(tiff)?;
        let bands = decoder
            .find_tag_unsigned::<u16>(Tag::SamplesPerPixel)
            .map_err(tiff)?
            .unwrap_or(1);
        let format = decoder
            .find_tag_unsigned_vec::<u16>(Tag::SampleFormat)
            .map_err(tiff)?
            .and_then(|formats| formats.first().copied())
            .map_or(SampleFormat::Uint, SampleFormat::from_u16_exhaustive);
        let sample_type = SampleType::from_tiff(format, color.bit_depth())?;
        let interleave = match decoder
            .find_tag_unsigned::<u16>(Tag::PlanarConfiguration)
            .map_err(tiff)?
        {
            Some(PLANAR_SEPARATE) => Interleave::Planar,
            _ => Interleave::Chunky,
        };
        // A chunky pixel decodes to the color type's samples. Fewer than the
        // file stores drops bands.
        if interleave == Interleave::Chunky && color.num_samples() != bands {
            return Err(RasterError::UnsupportedBandLayout {
                samples: bands,
                decoded: color.num_samples(),
            });
        }
        // The tiff decoder refuses a zero image size, tile size, and
        // RowsPerStrip, so every block dimension here is at least 1.
        let (stored_width, stored_height) = decoder.chunk_dimensions();
        let (kind, block_width, block_height) = match decoder.get_chunk_type() {
            ChunkType::Strip => (BlockKind::Strips, width, stored_height.min(height)),
            ChunkType::Tile => (BlockKind::Tiles, stored_width, stored_height),
        };
        Ok(Self {
            width,
            height,
            bands,
            sample_type,
            kind,
            interleave,
            block_width,
            block_height,
            blocks_across: width.div_ceil(block_width),
            blocks_down: height.div_ceil(block_height),
        })
    }

    /// Blocks per band plane.
    pub fn block_count(&self) -> u32 {
        // The tiff decoder checked that the chunk offsets, a u32-countable
        // list, hold this many entries per plane.
        self.blocks_across.saturating_mul(self.blocks_down)
    }

    /// Decoded bytes of the largest block, across all bands.
    pub fn block_bytes(&self) -> u64 {
        u64::from(self.block_width)
            * u64::from(self.block_height)
            * u64::from(self.bands)
            * self.sample_type.byte_len() as u64
    }

    /// The window of block `index`, cropped to the image.
    pub fn window(&self, index: u32) -> Result<BlockWindow, RasterError> {
        let count = self.block_count();
        if index >= count {
            return Err(RasterError::BlockOutOfRange { index, count });
        }
        let col_off = (index % self.blocks_across) * self.block_width;
        let row_off = (index / self.blocks_across) * self.block_height;
        Ok(BlockWindow {
            col_off,
            row_off,
            width: self.block_width.min(self.width - col_off),
            height: self.block_height.min(self.height - row_off),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(kind: BlockKind, block: (u32, u32), image: (u32, u32)) -> RasterLayout {
        RasterLayout {
            width: image.0,
            height: image.1,
            bands: 2,
            sample_type: SampleType::U16,
            kind,
            interleave: Interleave::Chunky,
            block_width: block.0,
            block_height: block.1,
            blocks_across: image.0.div_ceil(block.0),
            blocks_down: image.1.div_ceil(block.1),
        }
    }

    #[test]
    fn edge_tiles_are_cropped() {
        let grid = layout(BlockKind::Tiles, (16, 16), (40, 24));
        assert_eq!(grid.block_count(), 6);
        let last = grid.window(5).expect("in range");
        assert_eq!(
            last,
            BlockWindow {
                col_off: 32,
                row_off: 16,
                width: 8,
                height: 8
            }
        );
        let middle = grid.window(1).expect("in range");
        assert_eq!((middle.col_off, middle.width, middle.height), (16, 16, 16));
        assert_eq!(grid.block_bytes(), 16 * 16 * 2 * 2);
    }

    #[test]
    fn strips_span_the_width() {
        let grid = layout(BlockKind::Strips, (10, 4), (10, 9));
        assert_eq!(grid.block_count(), 3);
        let last = grid.window(2).expect("in range");
        assert_eq!((last.row_off, last.width, last.height), (8, 10, 1));
    }

    #[test]
    fn out_of_range_block_is_a_typed_error() {
        let grid = layout(BlockKind::Strips, (10, 4), (10, 9));
        assert!(matches!(
            grid.window(3),
            Err(RasterError::BlockOutOfRange { index: 3, count: 3 })
        ));
    }
}
