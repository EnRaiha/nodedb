// SPDX-License-Identifier: BUSL-1.1

use std::fmt;
use std::str::FromStr;

const TOKEN_PREFIX: &str = "v1:";
const EPOCH_HEX_LEN: usize = 32;
const MAX_TOKEN_LEN: usize = TOKEN_PREFIX.len() + EPOCH_HEX_LEN + 1 + 20;

/// Opaque, versioned position in one ChangeStream publication epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChangeCursor {
    epoch: u128,
    sequence: u64,
}

impl ChangeCursor {
    pub(crate) const fn new(epoch: u128, sequence: u64) -> Self {
        Self { epoch, sequence }
    }

    pub(crate) const fn epoch(self) -> u128 {
        self.epoch
    }

    pub(crate) const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Whether two cursors belong to the same publication epoch.
    pub const fn same_epoch(self, other: Self) -> bool {
        self.epoch == other.epoch
    }

    /// Compare sequences only after confirming the cursors share an epoch.
    pub const fn is_after_in_same_epoch(self, other: Self) -> bool {
        self.same_epoch(other) && self.sequence > other.sequence
    }
}

impl fmt::Display for ChangeCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "v1:{:032x}:{}", self.epoch, self.sequence)
    }
}

/// Strict opaque-cursor parsing failure. Details are intentionally not exposed
/// to clients, which prevents token parsing from becoming a compatibility API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorParseError;

impl fmt::Display for CursorParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid change cursor")
    }
}

impl std::error::Error for CursorParseError {}

impl FromStr for ChangeCursor {
    type Err = CursorParseError;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        if token.len() > MAX_TOKEN_LEN {
            return Err(CursorParseError);
        }
        let Some(rest) = token.strip_prefix(TOKEN_PREFIX) else {
            return Err(CursorParseError);
        };
        let mut parts = rest.split(':');
        let (Some(epoch), Some(sequence), None) = (parts.next(), parts.next(), parts.next()) else {
            return Err(CursorParseError);
        };
        if epoch.len() != EPOCH_HEX_LEN
            || !epoch
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || sequence.is_empty()
            || sequence.len() > 20
            || (sequence.len() > 1 && sequence.starts_with('0'))
            || !sequence.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(CursorParseError);
        }
        let epoch = u128::from_str_radix(epoch, 16).map_err(|_| CursorParseError)?;
        let sequence = sequence.parse().map_err(|_| CursorParseError)?;
        Ok(Self { epoch, sequence })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_is_strict_and_bounded() {
        let cursor = ChangeCursor::new(0xab, 42);
        assert_eq!(cursor.to_string(), "v1:000000000000000000000000000000ab:42");
        assert!(cursor.to_string().parse::<ChangeCursor>().is_ok());
        for invalid in [
            "v1:AB:1",
            "v1:ab:01",
            "v2:000000000000000000000000000000ab:1",
            "v1:000000000000000000000000000000ab:-1",
        ] {
            assert!(invalid.parse::<ChangeCursor>().is_err());
        }
    }
}
