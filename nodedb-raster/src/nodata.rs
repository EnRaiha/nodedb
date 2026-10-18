// SPDX-License-Identifier: Apache-2.0

//! The `GDAL_NODATA` tag (42113): one ASCII number shared by every band.

use crate::error::RasterError;

/// Parse a `GDAL_NODATA` value. NUL and space padding is trimmed, and
/// `nan` in any case is NaN.
pub fn parse_gdal_nodata(text: &str) -> Result<f64, RasterError> {
    let trimmed = text.trim_matches(|c: char| c == '\0' || c == ' ');
    if trimmed.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    trimmed
        .parse::<f64>()
        .map_err(|_| RasterError::InvalidNodata {
            text: trimmed.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_and_floats_parse() {
        assert_eq!(parse_gdal_nodata("-9999").ok(), Some(-9999.0));
        assert_eq!(parse_gdal_nodata("0").ok(), Some(0.0));
        assert_eq!(
            parse_gdal_nodata("-3.4028234663852886e+38").ok(),
            Some(f64::from(f32::MIN))
        );
    }

    #[test]
    fn padding_is_trimmed() {
        assert_eq!(parse_gdal_nodata(" 255\0\0").ok(), Some(255.0));
        assert_eq!(parse_gdal_nodata("\0 -1 \0").ok(), Some(-1.0));
    }

    #[test]
    fn nan_in_any_case_is_nan() {
        for text in ["nan", "NaN", "NAN", " nan\0"] {
            assert!(parse_gdal_nodata(text).is_ok_and(f64::is_nan), "{text}");
        }
    }

    #[test]
    fn non_numbers_are_refused() {
        for text in ["", "\0", "abc", "1,5", "12abc"] {
            assert!(
                matches!(
                    parse_gdal_nodata(text),
                    Err(RasterError::InvalidNodata { .. })
                ),
                "{text}"
            );
        }
    }
}
