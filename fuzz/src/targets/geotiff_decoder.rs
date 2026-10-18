//! GeoTIFF open and block decode over arbitrary bytes.
//!
//! Every outcome must be `Ok` or a typed `RasterError`. A decoded block
//! must hold exactly one window of samples per band.

use std::io::Cursor;

use nodedb_raster::{GeoTiffReader, RasterLimits};

/// Per-block decode cap. Small enough that a hostile header cannot make
/// one block a large allocation.
const BLOCK_BYTES: usize = 4 * 1024 * 1024;
/// Blocks decoded per input, so a huge declared grid stays fast.
const MAX_BLOCKS: u32 = 64;

pub fn run(data: &[u8]) {
    let Ok(mut reader) = GeoTiffReader::open(Cursor::new(data), RasterLimits::new(BLOCK_BYTES))
    else {
        return;
    };
    let _ = reader.srid();
    let bands = usize::from(reader.band_count());
    let sample_type = reader.sample_type();
    for index in 0..reader.block_count().min(MAX_BLOCKS) {
        let Ok(block) = reader.read_block(index) else {
            continue;
        };
        assert_eq!(block.bands.len(), bands, "band count of block {index}");
        for samples in &block.bands {
            assert_eq!(samples.len(), block.window.pixels(), "block {index}");
            assert_eq!(samples.sample_type(), sample_type, "block {index}");
        }
    }
}
