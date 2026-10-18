// SPDX-License-Identifier: Apache-2.0

//! Typed errors of the GeoTIFF reader.

use std::io;

use nodedb_types::geometry::UnsupportedSrid;
use tiff::TiffError;

use crate::geokeys::GeoKeyError;
use crate::sample::SampleFormatCode;

/// Every way opening or decoding a GeoTIFF can fail.
#[derive(Debug, thiserror::Error)]
pub enum RasterError {
    /// The underlying reader failed for a reason outside the file's bytes.
    #[error("GeoTIFF read failed: {0}")]
    Io(#[source] io::Error),

    /// The bytes do not form a valid TIFF: truncated, corrupt, or
    /// self-contradictory.
    #[error("malformed TIFF: {0}")]
    Malformed(#[source] TiffError),

    /// The TIFF is valid, but it uses a feature the decoder does not read.
    #[error("unsupported TIFF feature: {0}")]
    UnsupportedTiff(#[source] TiffError),

    /// One block decodes to more bytes than the caller allows.
    #[error(
        "GeoTIFF block needs {needed_bytes} decoded bytes, over the {limit_bytes}-byte limit; \
         rewrite the file with smaller strips or tiles"
    )]
    BlockTooLarge {
        needed_bytes: u64,
        limit_bytes: usize,
    },

    /// An IFD 0 tag value ends past the end of the file or exceeds the
    /// tag value cap.
    #[error(
        "GeoTIFF tag {tag} declares a {bytes}-byte value, past the {file_len}-byte file \
         or over the tag value cap"
    )]
    TagValueOutOfBounds { tag: u16, bytes: u64, file_len: u64 },

    /// The tiff decoder hit its own buffer limit.
    #[error("GeoTIFF decode exceeds the {limit_bytes}-byte buffer limit")]
    DecodeLimitExceeded { limit_bytes: usize },

    /// The compression method is not deflate, LZW, PackBits, or none.
    #[error(
        "GeoTIFF compression code {code} is not supported; \
         supported: none (1), LZW (5), deflate (8, 32946), PackBits (32773)"
    )]
    UnsupportedCompression { code: u16 },

    /// The sample format and width are outside u8, u16, i16, u32, f32, f64.
    #[error(
        "GeoTIFF samples of format {format} with {bits} bits are not supported; \
         supported: u8, u16, i16, u32, f32, f64"
    )]
    UnsupportedSampleType { format: SampleFormatCode, bits: u8 },

    /// The photometric interpretation transforms samples on decode, or
    /// stores palette indexes.
    #[error(
        "GeoTIFF photometric interpretation {code} is not supported; \
         supported: BlackIsZero (1), RGB (2), CMYK (5), YCbCr (6)"
    )]
    UnsupportedPhotometric { code: u16 },

    /// A chunky pixel decodes to fewer samples than the file stores.
    #[error(
        "GeoTIFF stores {samples} samples per pixel, but its color type decodes {decoded}; \
         write the extra bands as BlackIsZero multiband"
    )]
    UnsupportedBandLayout { samples: u16, decoded: u16 },

    /// IFD 0 is an overview, so the full-resolution image is elsewhere.
    #[error(
        "GeoTIFF IFD 0 is a reduced-resolution image; \
         the full-resolution image must be the first IFD"
    )]
    ReducedResolutionFirstImage,

    /// The GeoKey directory does not parse.
    #[error("malformed GeoKey directory: {0}")]
    GeoKeys(#[from] GeoKeyError),

    /// A georeference tag holds the wrong number of values.
    #[error("GeoTIFF tag {tag} holds {found} values, expected {expected}")]
    GeoTagLength {
        tag: &'static str,
        found: usize,
        expected: &'static str,
    },

    /// Tiepoints without a pixel scale or a transformation are ground
    /// control points.
    #[error(
        "GeoTIFF georeferences by ground control points; \
         warp it to an affine grid first"
    )]
    GroundControlPoints,

    /// A pixel scale without a tiepoint places no pixel in the world.
    #[error("GeoTIFF has ModelPixelScaleTag but no ModelTiepointTag")]
    PixelScaleWithoutTiepoint,

    /// The model transformation is not a 2D affine transform.
    #[error(
        "GeoTIFF ModelTransformationTag is not a 2D affine transform: \
         the z terms must be 0 and the last row 0 0 0 1"
    )]
    NonAffineTransformation,

    /// The geotransform holds a non-finite term or maps the plane to a line.
    #[error("GeoTIFF geotransform {transform:?} is degenerate")]
    DegenerateTransform { transform: [f64; 6] },

    /// The GeoKey model type is neither projected (1) nor geographic (2).
    #[error(
        "GeoTIFF model type {model} is not supported; supported: projected (1), geographic (2)"
    )]
    UnsupportedModelType { model: u16 },

    /// The model type names a CRS key the directory does not hold.
    #[error("GeoTIFF model type {model} needs GeoKey {key}, which is absent")]
    MissingCrsKey { model: u16, key: u16 },

    /// The CRS is user-defined, so no EPSG code names it.
    #[error("GeoTIFF CRS is user-defined (GeoKey {key} = 32767); only EPSG codes are supported")]
    UserDefinedCrs { key: u16 },

    /// The CRS EPSG code is outside the supported set.
    #[error("GeoTIFF CRS: {0}")]
    UnsupportedCrs(#[from] UnsupportedSrid),

    /// GDAL_NODATA is not a number.
    #[error("GeoTIFF GDAL_NODATA '{text}' is not a number")]
    InvalidNodata { text: String },

    /// The caller asked for a block the image does not have.
    #[error("GeoTIFF block {index} is out of range; the image has {count} blocks")]
    BlockOutOfRange { index: u32, count: u32 },

    /// The decoder returned fewer samples than the block holds.
    #[error("GeoTIFF block {index} decoded {found} samples, expected at least {expected}")]
    ShortBlock {
        index: u32,
        found: usize,
        expected: usize,
    },
}

impl RasterError {
    /// Classify a tiff decoder error. `limit_bytes` is the buffer cap the
    /// decoder ran under.
    pub(crate) fn from_tiff(error: TiffError, limit_bytes: usize) -> Self {
        match error {
            TiffError::IoError(io) => RasterError::from(io),
            TiffError::UnsupportedError(_) => RasterError::UnsupportedTiff(error),
            TiffError::LimitsExceeded => RasterError::DecodeLimitExceeded { limit_bytes },
            TiffError::FormatError(_) | TiffError::IntSizeError | TiffError::UsageError(_) => {
                RasterError::Malformed(error)
            }
        }
    }
}

impl From<io::Error> for RasterError {
    /// A short or undecodable stream is a malformed file. Every other kind
    /// is a reader error.
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::UnexpectedEof
            | io::ErrorKind::InvalidData
            | io::ErrorKind::InvalidInput => RasterError::Malformed(TiffError::IoError(error)),
            _ => RasterError::Io(error),
        }
    }
}
