// SPDX-License-Identifier: BUSL-1.1

//! `statement_timeout`: parsing the session parameter, and reading the budget
//! it fixes for the next statement.
//!
//! The parameter is stored as the raw text the client set, the way every other
//! session parameter is, so `SHOW statement_timeout` echoes what was set. This
//! module is the one place that turns that text into a duration.
//!
//! Accepted forms match PostgreSQL: a bare integer is milliseconds, and a
//! suffixed integer names its own unit (`us`, `ms`, `s`, `min`, `h`, `d`).
//! Whitespace between the number and the unit is allowed. `0` means no session
//! limit, in any unit.

use std::time::Duration;

use super::connection::SessionId;
use super::store::SessionStore;

/// A `SET statement_timeout` value the parser refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid value for parameter \"statement_timeout\": \"{value}\"")]
pub struct InvalidStatementTimeout {
    /// The text the client sent, verbatim.
    pub value: String,
}

/// Nanoseconds per accepted unit suffix, longest suffix first so `ms` is not
/// matched as `s`.
const UNITS: &[(&str, u64)] = &[
    ("min", 60_000_000_000),
    ("ms", 1_000_000),
    ("us", 1_000),
    ("s", 1_000_000_000),
    ("h", 3_600_000_000_000),
    ("d", 86_400_000_000_000),
];

/// Parse a `statement_timeout` value.
///
/// `Ok(None)` means the session sets no limit — the value was `0`. The node's
/// configured default deadline still applies to such a statement; `0` removes
/// the session's own cap, not the server's.
pub fn parse_statement_timeout(raw: &str) -> Result<Option<Duration>, InvalidStatementTimeout> {
    let invalid = || InvalidStatementTimeout {
        value: raw.to_string(),
    };
    let text = raw.trim().trim_matches('\'').trim_matches('"').trim();
    if text.is_empty() {
        return Err(invalid());
    }

    let lower = text.to_ascii_lowercase();
    let (digits, nanos_per_unit) = match UNITS
        .iter()
        .find(|(suffix, _)| lower.len() > suffix.len() && lower.ends_with(suffix))
    {
        Some((suffix, nanos)) => (lower[..lower.len() - suffix.len()].trim_end(), *nanos),
        // No suffix: PostgreSQL reads a bare `statement_timeout` as milliseconds.
        None => (lower.as_str(), 1_000_000),
    };

    let count: u64 = digits.parse().map_err(|_| invalid())?;
    if count == 0 {
        return Ok(None);
    }
    let nanos = count.checked_mul(nanos_per_unit).ok_or_else(invalid)?;
    Ok(Some(Duration::from_nanos(nanos)))
}

impl SessionStore {
    /// The budget this session's `statement_timeout` fixes for its next
    /// statement, or `None` when the session sets no limit.
    ///
    /// A stored value the parser refuses reads as `None`. `SET` rejects such a
    /// value up front, so the only way to store one is a code path that
    /// bypassed the setter, and dropping a session cap is the safe reading of
    /// it — the node's default deadline still bounds the statement.
    pub fn statement_timeout(&self, addr: impl Into<SessionId>) -> Option<Duration> {
        let raw = self.get_parameter(addr, "statement_timeout")?;
        parse_statement_timeout(&raw).ok().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_integer_is_milliseconds() {
        assert_eq!(
            parse_statement_timeout("250"),
            Ok(Some(Duration::from_millis(250)))
        );
    }

    #[test]
    fn zero_means_no_session_limit() {
        assert_eq!(parse_statement_timeout("0"), Ok(None));
        assert_eq!(parse_statement_timeout("0s"), Ok(None));
        assert_eq!(parse_statement_timeout(" 0 ms "), Ok(None));
    }

    #[test]
    fn every_unit_suffix_parses() {
        assert_eq!(
            parse_statement_timeout("500us"),
            Ok(Some(Duration::from_micros(500)))
        );
        assert_eq!(
            parse_statement_timeout("30ms"),
            Ok(Some(Duration::from_millis(30)))
        );
        assert_eq!(
            parse_statement_timeout("2s"),
            Ok(Some(Duration::from_secs(2)))
        );
        assert_eq!(
            parse_statement_timeout("3min"),
            Ok(Some(Duration::from_secs(180)))
        );
        assert_eq!(
            parse_statement_timeout("2h"),
            Ok(Some(Duration::from_secs(7200)))
        );
        assert_eq!(
            parse_statement_timeout("1d"),
            Ok(Some(Duration::from_secs(86_400)))
        );
    }

    #[test]
    fn ms_is_not_read_as_s() {
        assert_ne!(
            parse_statement_timeout("5ms"),
            parse_statement_timeout("5s"),
            "the longest matching suffix wins"
        );
    }

    #[test]
    fn quoted_and_spaced_forms_parse() {
        assert_eq!(
            parse_statement_timeout("'1500 ms'"),
            Ok(Some(Duration::from_millis(1500)))
        );
        assert_eq!(
            parse_statement_timeout("2 S"),
            Ok(Some(Duration::from_secs(2)))
        );
    }

    #[test]
    fn junk_is_refused() {
        for value in ["", "abc", "-5", "5 fortnights", "1.5s", "s"] {
            assert!(
                parse_statement_timeout(value).is_err(),
                "{value} must be refused"
            );
        }
    }

    #[test]
    fn overflow_is_refused() {
        assert!(parse_statement_timeout(&format!("{}d", u64::MAX)).is_err());
    }
}
