// SPDX-License-Identifier: Apache-2.0

//! Memory caps the reader decodes under.

use tiff::decoder::Limits;

/// Largest out-of-line IFD 0 tag value: strip offsets, GeoKeys, GDAL_NODATA.
pub(crate) const MAX_TAG_VALUE_BYTES: usize = 1024 * 1024;

/// Caps on what one decode allocates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RasterLimits {
    /// Most bytes one block decodes to, across all its bands. The same cap
    /// bounds one compressed chunk read from the file.
    pub block_bytes: usize,
}

impl RasterLimits {
    /// Limits with `block_bytes` as the per-block decoded byte cap.
    pub const fn new(block_bytes: usize) -> Self {
        Self { block_bytes }
    }

    /// The tiff decoder limits these caps imply.
    pub(crate) fn tiff_limits(self) -> Limits {
        // `Limits` is non-exhaustive: start from a constructor, then set
        // every field this tiff version has.
        let mut limits = Limits::unlimited();
        limits.decoding_buffer_size = self.block_bytes;
        limits.intermediate_buffer_size = self.block_bytes;
        limits.ifd_value_size = MAX_TAG_VALUE_BYTES;
        limits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_cap_bounds_decode_and_compressed_buffers() {
        let limits = RasterLimits::new(4096).tiff_limits();
        assert_eq!(limits.decoding_buffer_size, 4096);
        assert_eq!(limits.intermediate_buffer_size, 4096);
        assert_eq!(limits.ifd_value_size, MAX_TAG_VALUE_BYTES);
    }
}
