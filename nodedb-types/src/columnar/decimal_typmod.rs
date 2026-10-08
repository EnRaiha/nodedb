// SPDX-License-Identifier: Apache-2.0

//! [`DecimalTypmod`]: the declared precision and scale of a `DECIMAL(p,s)`
//! column, and the rule that fits a value to it.

use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};

/// The largest precision PostgreSQL accepts in a `NUMERIC` typmod.
pub const PG_MAX_DECIMAL_PRECISION: i64 = 1000;

/// The largest precision the engine stores exactly.
///
/// A stored decimal is a `rust_decimal::Decimal`: a 96-bit mantissa and a
/// scale of at most 28. Every value of 28 digits fits that mantissa. Some
/// values of 29 digits do not.
pub const MAX_DECIMAL_PRECISION: u8 = 28;

/// The precision and scale a `DECIMAL(p,s)` column declares.
///
/// A value of this type always holds `1 <= precision <= 28` and
/// `scale <= precision`. [`DecimalTypmod::new`] is the only constructor, and
/// both decoders run it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "RawDecimalTypmod", into = "RawDecimalTypmod")]
pub struct DecimalTypmod {
    precision: u8,
    scale: u8,
}

/// The serde form of [`DecimalTypmod`], before validation.
#[derive(Serialize, Deserialize)]
struct RawDecimalTypmod {
    precision: i64,
    scale: i64,
}

/// A declared `DECIMAL(p,s)` that is not a valid typmod.
///
/// The messages follow PostgreSQL's `numeric` typmod errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecimalTypmodError {
    #[error(
        "NUMERIC precision {precision} must be between 1 and {max}",
        max = PG_MAX_DECIMAL_PRECISION
    )]
    PrecisionOutOfRange { precision: i64 },
    #[error("NUMERIC scale {scale} must be between 0 and precision {precision}")]
    ScaleOutOfRange { precision: i64, scale: i64 },
    #[error(
        "NUMERIC precision {precision} exceeds {max}, \
         the largest precision this server stores exactly",
        max = MAX_DECIMAL_PRECISION
    )]
    PrecisionUnsupported { precision: i64 },
}

/// A value whose rounded integer part has more digits than its
/// `DECIMAL(p,s)` column holds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "{value} does not fit DECIMAL({precision},{scale}): \
     the value must round to an absolute value less than 10^{max_integer_digits}"
)]
pub struct DecimalOutOfRange {
    pub value: Decimal,
    pub precision: u8,
    pub scale: u8,
    pub max_integer_digits: u32,
}

impl DecimalTypmod {
    /// Validate a declared precision and scale.
    ///
    /// PostgreSQL's range applies first: precision `1..=1000`, scale
    /// `0..=precision`. A precision past [`MAX_DECIMAL_PRECISION`] is then
    /// refused, because the engine cannot store every value it admits.
    pub fn new(precision: i64, scale: i64) -> Result<Self, DecimalTypmodError> {
        if !(1..=PG_MAX_DECIMAL_PRECISION).contains(&precision) {
            return Err(DecimalTypmodError::PrecisionOutOfRange { precision });
        }
        if !(0..=precision).contains(&scale) {
            return Err(DecimalTypmodError::ScaleOutOfRange { precision, scale });
        }
        let (Ok(precision_digits), Ok(scale_digits)) =
            (u8::try_from(precision), u8::try_from(scale))
        else {
            return Err(DecimalTypmodError::PrecisionUnsupported { precision });
        };
        if precision_digits > MAX_DECIMAL_PRECISION {
            return Err(DecimalTypmodError::PrecisionUnsupported { precision });
        }
        Ok(Self {
            precision: precision_digits,
            scale: scale_digits,
        })
    }

    /// Total significant digits.
    pub fn precision(self) -> u8 {
        self.precision
    }

    /// Digits after the decimal point.
    pub fn scale(self) -> u8 {
        self.scale
    }

    /// `value` fitted to this typmod, as PostgreSQL stores it.
    ///
    /// The value rounds to `scale` fractional digits, half away from zero,
    /// and carries exactly `scale` digits. A result with more than
    /// `precision - scale` integer digits does not fit and is refused.
    pub fn fit(self, value: Decimal) -> Result<Decimal, DecimalOutOfRange> {
        let scale = u32::from(self.scale);
        let mut fitted =
            value.round_dp_with_strategy(scale, RoundingStrategy::MidpointAwayFromZero);
        let max_integer_digits = u32::from(self.precision - self.scale);
        if integer_digits(fitted) > max_integer_digits {
            return Err(DecimalOutOfRange {
                value,
                precision: self.precision,
                scale: self.scale,
                max_integer_digits,
            });
        }
        // The fitted value has at most `precision <= 28` digits, so scaling
        // up only appends zeros and loses no digit.
        fitted.rescale(scale);
        Ok(fitted)
    }
}

impl TryFrom<RawDecimalTypmod> for DecimalTypmod {
    type Error = DecimalTypmodError;

    fn try_from(raw: RawDecimalTypmod) -> Result<Self, Self::Error> {
        Self::new(raw.precision, raw.scale)
    }
}

impl From<DecimalTypmod> for RawDecimalTypmod {
    fn from(typmod: DecimalTypmod) -> Self {
        Self {
            precision: i64::from(typmod.precision),
            scale: i64::from(typmod.scale),
        }
    }
}

impl zerompk::ToMessagePack for DecimalTypmod {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        writer.write_u8(self.precision)?;
        writer.write_u8(self.scale)
    }
}

impl<'de> zerompk::FromMessagePack<'de> for DecimalTypmod {
    fn read<R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(2)?;
        let precision = reader.read_u8()?;
        let scale = reader.read_u8()?;
        // An encoded typmod that fails validation is refused, the way an
        // unknown marker is.
        Self::new(i64::from(precision), i64::from(scale))
            .map_err(|_| zerompk::Error::InvalidMarker(precision))
    }
}

/// The count of digits left of the decimal point in `d`; zero for `|d| < 1`.
fn integer_digits(d: Decimal) -> u32 {
    // A `Decimal` is `mantissa / 10^scale` with `scale <= 28`, so both
    // operands fit `u128`.
    let integer_part = d.mantissa().unsigned_abs() / 10u128.pow(d.scale());
    integer_part.checked_ilog10().map_or(0, |log| log + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        text.parse().expect("test decimal parses")
    }

    fn typmod(precision: i64, scale: i64) -> DecimalTypmod {
        DecimalTypmod::new(precision, scale).expect("test typmod is valid")
    }

    #[test]
    fn new_accepts_the_engine_range() {
        for (precision, scale) in [(1, 0), (1, 1), (5, 2), (28, 0), (28, 28)] {
            let t = typmod(precision, scale);
            assert_eq!(i64::from(t.precision()), precision);
            assert_eq!(i64::from(t.scale()), scale);
        }
    }

    #[test]
    fn new_refuses_what_postgresql_refuses() {
        for precision in [0, -1, 1001, i64::MAX] {
            assert_eq!(
                DecimalTypmod::new(precision, 0),
                Err(DecimalTypmodError::PrecisionOutOfRange { precision })
            );
        }
        assert_eq!(
            DecimalTypmod::new(5, 6),
            Err(DecimalTypmodError::ScaleOutOfRange {
                precision: 5,
                scale: 6
            })
        );
        assert_eq!(
            DecimalTypmod::new(5, -1),
            Err(DecimalTypmodError::ScaleOutOfRange {
                precision: 5,
                scale: -1
            })
        );
    }

    #[test]
    fn new_refuses_a_precision_the_engine_cannot_store() {
        for precision in [29, 38, 300, 1000] {
            assert_eq!(
                DecimalTypmod::new(precision, 0),
                Err(DecimalTypmodError::PrecisionUnsupported { precision })
            );
        }
    }

    #[test]
    fn fit_rounds_half_away_from_zero_to_the_scale() {
        for (input, expected) in [
            ("1.005", "1.01"),
            ("-1.005", "-1.01"),
            ("1.004", "1.00"),
            ("12.5", "12.50"),
            ("7", "7.00"),
            ("999.994", "999.99"),
        ] {
            let fitted = typmod(5, 2).fit(dec(input)).expect(input);
            assert_eq!(fitted.to_string(), expected, "{input}");
        }
    }

    #[test]
    fn fit_refuses_too_many_integer_digits() {
        for input in ["123456.789", "1000", "999.995", "-1000.00"] {
            let err = typmod(5, 2).fit(dec(input)).expect_err(input);
            assert_eq!(err.max_integer_digits, 3, "{input}");
        }
        assert!(typmod(2, 2).fit(dec("0.995")).is_err());
        assert_eq!(typmod(2, 2).fit(dec("0.994")), Ok(dec("0.99")));
    }

    #[test]
    fn fit_holds_every_28_digit_value() {
        let widest = dec("9999999999999999999999999999");
        assert_eq!(typmod(28, 0).fit(widest), Ok(widest));
        let fraction = dec("0.9999999999999999999999999999");
        assert_eq!(typmod(28, 28).fit(fraction), Ok(fraction));
    }

    #[test]
    fn msgpack_roundtrip_and_validation() {
        let t = typmod(10, 2);
        let bytes = zerompk::to_msgpack_vec(&t).expect("encode");
        let back: DecimalTypmod = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, t);

        let invalid = zerompk::to_msgpack_vec(&(5u8, 6u8)).expect("encode raw pair");
        assert!(zerompk::from_msgpack::<DecimalTypmod>(&invalid).is_err());
    }

    #[test]
    fn serde_roundtrip_and_validation() {
        let t = typmod(10, 2);
        let json = serde_json::to_string(&t).expect("encode");
        assert_eq!(json, r#"{"precision":10,"scale":2}"#);
        let back: DecimalTypmod = serde_json::from_str(&json).expect("decode");
        assert_eq!(back, t);
        assert!(serde_json::from_str::<DecimalTypmod>(r#"{"precision":0,"scale":0}"#).is_err());
        assert!(serde_json::from_str::<DecimalTypmod>(r#"{"precision":39,"scale":0}"#).is_err());
    }
}
