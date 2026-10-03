// SPDX-License-Identifier: BUSL-1.1

//! PITR/restore utilities: timestamp parsing and the archived WAL coverage
//! check a point-in-time restore plans with.

/// Archived WAL that does not reach from a base's replay start to a target.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CoverageError {
    #[error(
        "archived WAL is missing LSNs {from}..={to}; replay needs every LSN from \
         {replay_start} through the target {target}"
    )]
    Gap {
        from: u64,
        to: u64,
        replay_start: u64,
        target: u64,
    },
    #[error("archived WAL ends at LSN {archived_through}, below the target LSN {target}")]
    EndsBeforeTarget { archived_through: u64, target: u64 },
}

impl From<CoverageError> for crate::Error {
    fn from(e: CoverageError) -> Self {
        crate::Error::Storage {
            engine: "snapshot".into(),
            detail: e.to_string(),
        }
    }
}

/// Whether the walk has every LSN it needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageStep {
    More,
    Covered,
}

/// Walks archived segments in LSN order and checks that together they hold
/// every LSN in `replay_start..=target`.
///
/// A segment's name gives its first LSN. Its highest record comes from a
/// scan. An empty segment says nothing about where the log ends, so the
/// boundary carries over from the last segment that held records.
#[derive(Debug, Clone)]
pub struct WalCoverage {
    replay_start: u64,
    target: u64,
    /// The lowest LSN no fed segment holds yet.
    next: u64,
}

impl WalCoverage {
    pub fn new(replay_start: u64, target: u64) -> Self {
        Self {
            replay_start,
            target,
            next: replay_start,
        }
    }

    pub fn is_covered(&self) -> bool {
        self.next > self.target
    }

    /// Feed the next archived segment, whose first LSN is at or below the
    /// target. `last_lsn` is its highest record, `None` when it holds none.
    pub fn feed(
        &mut self,
        first_lsn: u64,
        last_lsn: Option<u64>,
    ) -> Result<CoverageStep, CoverageError> {
        if first_lsn > self.next {
            return Err(self.gap_to(first_lsn));
        }
        if let Some(last) = last_lsn {
            self.next = self.next.max(last.saturating_add(1));
        }
        Ok(if self.is_covered() {
            CoverageStep::Covered
        } else {
            CoverageStep::More
        })
    }

    /// The error when the archive holds no further segment at or below the
    /// target before the walk is covered. `next_first_lsn` is the first LSN
    /// of the next archived segment, if any.
    pub fn missing(&self, next_first_lsn: Option<u64>) -> CoverageError {
        match next_first_lsn {
            Some(first) => self.gap_to(first),
            None => CoverageError::EndsBeforeTarget {
                archived_through: self.next.saturating_sub(1),
                target: self.target,
            },
        }
    }

    /// The gap from the next needed LSN up to the segment starting at `first`.
    fn gap_to(&self, first: u64) -> CoverageError {
        CoverageError::Gap {
            from: self.next,
            to: first.saturating_sub(1).min(self.target),
            replay_start: self.replay_start,
            target: self.target,
        }
    }
}

/// Smallest integer accepted as an epoch timestamp, in each unit.
///
/// The value is 1973-03-03T09:46:40Z expressed as seconds, milliseconds and
/// microseconds. Below it the three units overlap and no rule can tell them
/// apart, so an integer that small is refused and ISO 8601 names the instant.
const MIN_EPOCH_SECS: u64 = 100_000_000;
const MIN_EPOCH_MILLIS: u64 = MIN_EPOCH_SECS * 1_000;
const MIN_EPOCH_MICROS: u64 = MIN_EPOCH_SECS * 1_000_000;

/// Largest instant accepted, as microseconds since the epoch: 2100-01-01Z.
/// A larger value is a unit error, not a restore target anyone holds WAL for.
const MAX_EPOCH_MICROS: u64 = 4_102_444_800_000_000;

/// Parse a UTC timestamp into microseconds since the Unix epoch.
///
/// Accepted forms:
/// - RFC 3339 / ISO 8601 with an offset: `"2024-03-15T14:30:00Z"`,
///   `"2024-03-15T19:30:00+05:00"`
/// - ISO 8601 with no offset, read as UTC: `"2024-03-15T14:30:00"`
/// - Unix epoch seconds, milliseconds or microseconds: `"1710509400"`,
///   `"1710509400000"`, `"1710509400000000"`
///
/// An integer's unit comes from its magnitude, and the three ranges do not
/// overlap above [`MIN_EPOCH_SECS`]. Anything outside the accepted range is
/// refused rather than resolved to a wrong instant: this value selects the
/// point a restore rewinds to, so a silent misreading restores the wrong data.
pub fn parse_utc_timestamp(input: &str) -> crate::Result<u64> {
    let trimmed = input.trim();

    if let Ok(n) = trimmed.parse::<u64>() {
        return epoch_integer_to_micros(n, trimmed);
    }

    // `parse_from_rfc3339` reads the offset and rejects an impossible date,
    // so `2024-02-31` and `2024-13-01` are errors rather than silent rewrites.
    if let Ok(fixed) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return micros_since_epoch(fixed.timestamp_micros(), trimmed);
    }

    // An instant with no offset is UTC.
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S"))
    {
        return micros_since_epoch(naive.and_utc().timestamp_micros(), trimmed);
    }

    Err(crate::Error::BadRequest {
        detail: format!(
            "cannot parse UTC timestamp: '{trimmed}'. Expected RFC 3339 \
             (2024-03-15T14:30:00Z), or epoch seconds, milliseconds or microseconds"
        ),
    })
}

/// Resolve a bare integer to microseconds, taking its unit from its magnitude.
fn epoch_integer_to_micros(n: u64, original: &str) -> crate::Result<u64> {
    let micros = if n >= MIN_EPOCH_MICROS {
        n
    } else if n >= MIN_EPOCH_MILLIS {
        n * 1_000
    } else if n >= MIN_EPOCH_SECS {
        n * 1_000_000
    } else {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "epoch timestamp '{original}' is below {MIN_EPOCH_SECS}, where seconds, \
                 milliseconds and microseconds cannot be told apart. Use RFC 3339 instead"
            ),
        });
    };
    reject_beyond_max(micros, original)
}

/// Convert a chrono microsecond count, refusing an instant before the epoch.
fn micros_since_epoch(micros: i64, original: &str) -> crate::Result<u64> {
    let non_negative = u64::try_from(micros).map_err(|_| crate::Error::BadRequest {
        detail: format!(
            "UTC timestamp '{original}' precedes 1970-01-01Z, which no restore target reaches"
        ),
    })?;
    reject_beyond_max(non_negative, original)
}

/// Refuse an instant past [`MAX_EPOCH_MICROS`].
fn reject_beyond_max(micros: u64, original: &str) -> crate::Result<u64> {
    if micros > MAX_EPOCH_MICROS {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "UTC timestamp '{original}' resolves past the year 2100, so its unit is wrong"
            ),
        });
    }
    Ok(micros)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2024-03-15T14:30:00Z, the instant every case below resolves to.
    const REFERENCE_MICROS: u64 = 1_710_513_000_000_000;

    #[test]
    fn rfc3339_utc_resolves() {
        assert_eq!(
            parse_utc_timestamp("2024-03-15T14:30:00Z").unwrap(),
            REFERENCE_MICROS
        );
    }

    #[test]
    fn an_offset_shifts_the_instant_instead_of_being_ignored() {
        // 19:30+05:00 is the same instant as 14:30Z. Reading the offset as UTC
        // would land five hours late.
        assert_eq!(
            parse_utc_timestamp("2024-03-15T19:30:00+05:00").unwrap(),
            REFERENCE_MICROS
        );
    }

    #[test]
    fn an_instant_with_no_offset_is_utc() {
        assert_eq!(
            parse_utc_timestamp("2024-03-15T14:30:00").unwrap(),
            REFERENCE_MICROS
        );
    }

    #[test]
    fn seconds_millis_and_micros_all_resolve_to_one_instant() {
        for input in ["1710513000", "1710513000000", "1710513000000000"] {
            assert_eq!(
                parse_utc_timestamp(input).unwrap(),
                REFERENCE_MICROS,
                "{input} must resolve to the same instant"
            );
        }
    }

    #[test]
    fn an_impossible_date_is_refused() {
        // Both parse under hand-rolled month arithmetic: month 13 falls through
        // to January, and February never checks its own length.
        for input in ["2024-13-01T00:00:00Z", "2024-02-31T00:00:00Z"] {
            assert!(
                parse_utc_timestamp(input).is_err(),
                "{input} names no instant and must be refused"
            );
        }
    }

    #[test]
    fn a_pre_epoch_instant_is_refused() {
        // Subtracting 1970 from an earlier year underflows an unsigned year.
        assert!(parse_utc_timestamp("1969-01-01T00:00:00Z").is_err());
    }

    #[test]
    fn a_multibyte_input_is_refused_without_panicking() {
        // Byte-slicing the first ten bytes splits this input mid-character.
        assert!(parse_utc_timestamp("2024-03-1\u{e9}T14:30:00Z").is_err());
        assert!(parse_utc_timestamp("\u{4e00}\u{4e8c}\u{4e09}T14:30:00Z").is_err());
    }

    #[test]
    fn an_ambiguous_small_integer_is_refused() {
        // 1000 reads as seconds or milliseconds with equal warrant.
        assert!(parse_utc_timestamp("1000").is_err());
    }

    #[test]
    fn an_integer_past_the_year_2100_is_refused() {
        assert!(parse_utc_timestamp("9999999999999999999").is_err());
    }

    /// Feeds `(first_lsn, last_lsn)` segments until covered. An uncovered
    /// walk names what the first unfed segment leaves missing.
    fn walk(start: u64, target: u64, segments: &[(u64, Option<u64>)]) -> Result<(), CoverageError> {
        let mut coverage = WalCoverage::new(start, target);
        for &(first, last) in segments {
            if first > target {
                return Err(coverage.missing(Some(first)));
            }
            if coverage.feed(first, last)? == CoverageStep::Covered {
                return Ok(());
            }
        }
        Err(coverage.missing(None))
    }

    #[test]
    fn contiguous_segments_cover_the_target() {
        let segments = [(1, Some(10)), (11, Some(20)), (21, Some(30))];
        assert_eq!(walk(5, 25, &segments), Ok(()));
        assert_eq!(walk(11, 20, &segments), Ok(()));
    }

    #[test]
    fn a_missing_segment_names_the_missing_range() {
        let segments = [(1, Some(10)), (11, Some(20)), (31, Some(40))];
        assert_eq!(
            walk(5, 35, &segments),
            Err(CoverageError::Gap {
                from: 21,
                to: 30,
                replay_start: 5,
                target: 35
            })
        );
    }

    #[test]
    fn a_target_inside_the_gap_names_the_range_up_to_the_target() {
        let segments = [(1, Some(10)), (31, Some(40))];
        assert_eq!(
            walk(5, 25, &segments),
            Err(CoverageError::Gap {
                from: 11,
                to: 25,
                replay_start: 5,
                target: 25
            })
        );
    }

    #[test]
    fn an_archive_starting_above_the_replay_start_is_a_gap() {
        assert_eq!(
            walk(5, 25, &[(20, Some(30))]),
            Err(CoverageError::Gap {
                from: 5,
                to: 19,
                replay_start: 5,
                target: 25
            })
        );
    }

    #[test]
    fn an_archive_ending_below_the_target_is_refused() {
        assert_eq!(
            walk(5, 15, &[(1, Some(10))]),
            Err(CoverageError::EndsBeforeTarget {
                archived_through: 10,
                target: 15
            })
        );
    }

    #[test]
    fn an_empty_segment_holds_no_lsn() {
        assert_eq!(
            walk(5, 12, &[(1, Some(10)), (11, None)]),
            Err(CoverageError::EndsBeforeTarget {
                archived_through: 10,
                target: 12
            })
        );
    }

    #[test]
    fn junk_is_refused() {
        for input in ["", "not-a-time", "2024-03-15"] {
            assert!(
                parse_utc_timestamp(input).is_err(),
                "{input} must be refused"
            );
        }
    }
}
