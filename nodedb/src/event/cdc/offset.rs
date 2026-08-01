// SPDX-License-Identifier: BUSL-1.1

//! Lossless CDC positions.
//!
//! A WAL LSN alone is not a consumer cursor: one transaction can emit several
//! redo events at the same LSN, and durable topics can publish several messages
//! in one millisecond. `CdcOffset` therefore orders the event LSN and its
//! per-stream sequence lexicographically.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A lossless position in a CDC stream partition.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CdcOffset {
    /// WAL LSN, or the millisecond timestamp used by durable topics.
    pub lsn: u64,
    /// Event sequence that disambiguates positions sharing an LSN.
    pub sequence: u64,
}

impl From<u64> for CdcOffset {
    /// Bare programmatic LSN values retain the SQL legacy meaning: acknowledge
    /// every event at that LSN.
    fn from(lsn: u64) -> Self {
        Self::legacy_lsn(lsn)
    }
}

impl PartialEq<u64> for CdcOffset {
    fn eq(&self, lsn: &u64) -> bool {
        self.lsn == *lsn && (self.sequence == u64::MAX || (*lsn == 0 && *self == Self::ZERO))
    }
}

impl CdcOffset {
    /// The initial cursor before normal CDC positions.
    pub const ZERO: Self = Self {
        lsn: 0,
        sequence: 0,
    };

    pub const fn new(lsn: u64, sequence: u64) -> Self {
        Self { lsn, sequence }
    }

    /// Interpret a legacy bare LSN acknowledgement as acknowledgement of the
    /// complete LSN, including every sibling event.
    pub const fn legacy_lsn(lsn: u64) -> Self {
        Self {
            lsn,
            sequence: u64::MAX,
        }
    }

    /// Canonical text token accepted by `COMMIT OFFSET`.
    pub fn token(self) -> String {
        self.to_string()
    }
}

impl fmt::Display for CdcOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.lsn, self.sequence)
    }
}

/// Error returned when an offset token is neither canonical nor a bare legacy
/// LSN acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCdcOffsetError {
    token: String,
}

impl fmt::Display for ParseCdcOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid CDC offset '{}'; expected canonical <lsn>:<sequence> or bare legacy <lsn> (acknowledges the whole LSN)",
            self.token
        )
    }
}

impl std::error::Error for ParseCdcOffsetError {}

impl FromStr for CdcOffset {
    type Err = ParseCdcOffsetError;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        let invalid = || ParseCdcOffsetError {
            token: token.to_string(),
        };
        match token.split_once(':') {
            Some((lsn, sequence)) if !lsn.is_empty() && !sequence.is_empty() => Ok(Self::new(
                lsn.parse().map_err(|_| invalid())?,
                sequence.parse().map_err(|_| invalid())?,
            )),
            Some(_) => Err(invalid()),
            None => Ok(Self::legacy_lsn(token.parse().map_err(|_| invalid())?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_are_lexicographically_ordered() {
        assert!(CdcOffset::new(10, 2) > CdcOffset::new(10, 1));
        assert!(CdcOffset::new(11, 0) > CdcOffset::new(10, u64::MAX));
    }

    #[test]
    fn parses_canonical_and_legacy_tokens() {
        assert_eq!("12:3".parse(), Ok(CdcOffset::new(12, 3)));
        assert_eq!("12".parse(), Ok(CdcOffset::legacy_lsn(12)));
        let error = "12:bad".parse::<CdcOffset>().unwrap_err();
        assert!(error.to_string().contains("bare legacy <lsn>"));
    }
}
