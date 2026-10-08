// SPDX-License-Identifier: Apache-2.0

//! The amount a fungible `Transfer` moves.
//!
//! The planner types the amount against the field it moves, the way an
//! assignment types its value. A `DECIMAL` field takes an exact amount, so
//! the balance moves by exactly the literal. Every other field takes a
//! float amount, moved by float arithmetic.

use std::fmt;

use rust_decimal::Decimal;

/// The amount a fungible `Transfer` moves, typed by the field it moves.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TransferAmount {
    /// The amount of a field that is not `DECIMAL`.
    Float(f64),
    /// The exact amount of a `DECIMAL` field.
    Decimal(Decimal),
}

/// Wire tag of [`TransferAmount::Float`].
const TAG_FLOAT: u8 = 0;
/// Wire tag of [`TransferAmount::Decimal`].
const TAG_DECIMAL: u8 = 1;

impl TransferAmount {
    /// Whether the amount is above zero. A transfer moves a positive amount.
    pub fn is_positive(self) -> bool {
        match self {
            Self::Float(f) => f > 0.0,
            Self::Decimal(d) => d > Decimal::ZERO,
        }
    }

    /// The amount as an `f64`. A decimal converts through its text, so the
    /// result is the `f64` nearest the exact value.
    pub fn to_f64(self) -> f64 {
        match self {
            Self::Float(f) => f,
            Self::Decimal(d) => d.to_string().parse().unwrap_or(f64::NAN),
        }
    }

    /// The amount as an exact decimal. A float converts through its
    /// shortest round-trip text, so the literal `0.1` stays `0.1`. `None`
    /// when the float lies outside the `Decimal` range.
    pub fn to_decimal(self) -> Option<Decimal> {
        match self {
            Self::Float(f) => f.to_string().parse().ok(),
            Self::Decimal(d) => Some(d),
        }
    }
}

impl fmt::Display for TransferAmount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Float(v) => write!(f, "{v}"),
            Self::Decimal(d) => write!(f, "{d}"),
        }
    }
}

/// Encoded as `[tag, payload]`. A decimal payload is its 16-byte
/// `Decimal::serialize` form, the form `nodedb_types::Value` uses.
impl zerompk::ToMessagePack for TransferAmount {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        match self {
            Self::Float(f) => {
                writer.write_u8(TAG_FLOAT)?;
                writer.write_f64(*f)
            }
            Self::Decimal(d) => {
                writer.write_u8(TAG_DECIMAL)?;
                writer.write_binary(&d.serialize())
            }
        }
    }
}

impl<'de> zerompk::FromMessagePack<'de> for TransferAmount {
    fn read<R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(2)?;
        match reader.read_u8()? {
            TAG_FLOAT => Ok(Self::Float(reader.read_f64()?)),
            TAG_DECIMAL => {
                let bytes = reader.read_binary()?;
                let buf =
                    <[u8; 16]>::try_from(&bytes[..]).map_err(|_| zerompk::Error::BufferTooSmall)?;
                Ok(Self::Decimal(Decimal::deserialize(buf)))
            }
            tag => Err(zerompk::Error::InvalidMarker(tag)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        text.parse().expect("test decimal parses")
    }

    #[test]
    fn both_variants_roundtrip_through_msgpack() {
        for amount in [
            TransferAmount::Float(30.5),
            TransferAmount::Decimal(dec("12.345")),
            TransferAmount::Decimal(dec("-0.01")),
        ] {
            let bytes = zerompk::to_msgpack_vec(&amount).expect("encode");
            let back: TransferAmount = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(back, amount);
        }
    }

    #[test]
    fn an_unknown_tag_is_refused() {
        let bytes = zerompk::to_msgpack_vec(&(7u8, 1.0f64)).expect("encode");
        assert!(zerompk::from_msgpack::<TransferAmount>(&bytes).is_err());
    }

    #[test]
    fn a_float_literal_converts_to_its_exact_text() {
        assert_eq!(TransferAmount::Float(0.1).to_decimal(), Some(dec("0.1")));
        assert_eq!(TransferAmount::Float(1e300).to_decimal(), None);
        assert_eq!(TransferAmount::Decimal(dec("0.1")).to_f64(), 0.1);
    }

    #[test]
    fn only_a_positive_amount_is_positive() {
        assert!(TransferAmount::Decimal(dec("0.01")).is_positive());
        assert!(!TransferAmount::Decimal(Decimal::ZERO).is_positive());
        assert!(!TransferAmount::Float(-1.0).is_positive());
        assert!(!TransferAmount::Float(f64::NAN).is_positive());
    }
}
