// SPDX-License-Identifier: BUSL-1.1

//! The SQLSTATE of each GeoTIFF reader error.
//!
//! - A malformed or corrupt file is `22P04` (bad copy file format).
//! - A valid file that uses a feature the reader lacks is `0A000`.
//! - A block over the decode cap is `54000`.
//! - A read error outside the file's bytes is `58030`.

use nodedb_raster::RasterError;

use super::super::decode::RowError;

/// `e`, met reading `path` under a `cap`-byte decode cap, as a refusal of
/// the whole input.
pub(super) fn raster_refusal(e: &RasterError, path: &str, cap: usize) -> RowError {
    let sqlstate = sqlstate(e);
    let message = if sqlstate == "54000" {
        format!(
            "'{path}': {e}; the decode cap is {cap} bytes; \
             retile the file with `gdal_translate -co TILED=YES`"
        )
    } else {
        format!("'{path}': {e}")
    };
    RowError::new(0, sqlstate, message)
}

fn sqlstate(e: &RasterError) -> &'static str {
    match e {
        RasterError::Malformed(_)
        | RasterError::TagValueOutOfBounds { .. }
        | RasterError::GeoKeys(_)
        | RasterError::GeoTagLength { .. }
        | RasterError::PixelScaleWithoutTiepoint
        | RasterError::DegenerateTransform { .. }
        | RasterError::MissingCrsKey { .. }
        | RasterError::InvalidNodata { .. }
        | RasterError::BlockOutOfRange { .. }
        | RasterError::ShortBlock { .. } => "22P04",
        RasterError::UnsupportedTiff(_)
        | RasterError::UnsupportedCompression { .. }
        | RasterError::UnsupportedSampleType { .. }
        | RasterError::UnsupportedPhotometric { .. }
        | RasterError::UnsupportedBandLayout { .. }
        | RasterError::ReducedResolutionFirstImage
        | RasterError::GroundControlPoints
        | RasterError::NonAffineTransformation
        | RasterError::UnsupportedModelType { .. }
        | RasterError::UserDefinedCrs { .. }
        | RasterError::UnsupportedCrs(_) => "0A000",
        RasterError::BlockTooLarge { .. } | RasterError::DecodeLimitExceeded { .. } => "54000",
        RasterError::Io(_) => "58030",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_class_maps_to_its_sqlstate() {
        let cases = [
            (RasterError::PixelScaleWithoutTiepoint, "22P04"),
            (RasterError::UnsupportedCompression { code: 7 }, "0A000"),
            (RasterError::GroundControlPoints, "0A000"),
            (
                RasterError::BlockTooLarge {
                    needed_bytes: 1 << 30,
                    limit_bytes: 1 << 26,
                },
                "54000",
            ),
            (RasterError::Io(std::io::Error::other("disk gone")), "58030"),
        ];
        for (error, code) in cases {
            let refusal = raster_refusal(&error, "/data/scene.tif", 1 << 26);
            assert_eq!(refusal.sqlstate, code, "{error}");
            assert_eq!(refusal.row, 0);
        }
    }

    #[test]
    fn an_oversized_block_names_the_retile_command() {
        let error = RasterError::DecodeLimitExceeded { limit_bytes: 4096 };
        let refusal = raster_refusal(&error, "/data/scene.tif", 4096);
        assert!(
            refusal.message.contains("gdal_translate -co TILED=YES"),
            "{}",
            refusal.message
        );
    }
}
