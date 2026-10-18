// SPDX-License-Identifier: Apache-2.0

//! The closed set of sample types the reader decodes.

use std::fmt;

use tiff::tags::SampleFormat;

use crate::error::RasterError;

/// A TIFF `SampleFormat` tag value, kept for error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleFormatCode(pub u16);

impl fmt::Display for SampleFormatCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            1 => f.write_str("unsigned integer (1)"),
            2 => f.write_str("signed integer (2)"),
            3 => f.write_str("IEEE float (3)"),
            4 => f.write_str("undefined (4)"),
            code => write!(f, "code {code}"),
        }
    }
}

/// One band sample type. Every band of a raster shares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SampleType {
    U8,
    U16,
    I16,
    U32,
    F32,
    F64,
}

impl SampleType {
    /// The sample type for a TIFF sample format and bit width. Any other
    /// pair fails with [`RasterError::UnsupportedSampleType`].
    pub fn from_tiff(format: SampleFormat, bits: u8) -> Result<Self, RasterError> {
        match (format, bits) {
            (SampleFormat::Uint, 8) => Ok(SampleType::U8),
            (SampleFormat::Uint, 16) => Ok(SampleType::U16),
            (SampleFormat::Int, 16) => Ok(SampleType::I16),
            (SampleFormat::Uint, 32) => Ok(SampleType::U32),
            (SampleFormat::IEEEFP, 32) => Ok(SampleType::F32),
            (SampleFormat::IEEEFP, 64) => Ok(SampleType::F64),
            _ => Err(RasterError::UnsupportedSampleType {
                format: SampleFormatCode(format.to_u16()),
                bits,
            }),
        }
    }

    /// Bytes per sample.
    pub const fn byte_len(self) -> usize {
        match self {
            SampleType::U8 => 1,
            SampleType::U16 | SampleType::I16 => 2,
            SampleType::U32 | SampleType::F32 => 4,
            SampleType::F64 => 8,
        }
    }

    /// The lower-case Rust name, such as `u16`.
    pub const fn name(self) -> &'static str {
        match self {
            SampleType::U8 => "u8",
            SampleType::U16 => "u16",
            SampleType::I16 => "i16",
            SampleType::U32 => "u32",
            SampleType::F32 => "f32",
            SampleType::F64 => "f64",
        }
    }
}

impl fmt::Display for SampleType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_pairs_map_to_their_type() {
        assert_eq!(
            SampleType::from_tiff(SampleFormat::Uint, 8).ok(),
            Some(SampleType::U8)
        );
        assert_eq!(
            SampleType::from_tiff(SampleFormat::Int, 16).ok(),
            Some(SampleType::I16)
        );
        assert_eq!(
            SampleType::from_tiff(SampleFormat::IEEEFP, 64).ok(),
            Some(SampleType::F64)
        );
    }

    #[test]
    fn unsupported_pairs_are_typed_errors() {
        for (format, bits) in [
            (SampleFormat::Int, 8),
            (SampleFormat::Uint, 64),
            (SampleFormat::Int, 32),
            (SampleFormat::IEEEFP, 16),
            (SampleFormat::Uint, 1),
            (SampleFormat::Void, 8),
        ] {
            assert!(matches!(
                SampleType::from_tiff(format, bits),
                Err(RasterError::UnsupportedSampleType { .. })
            ));
        }
    }
}
