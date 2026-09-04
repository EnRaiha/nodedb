// SPDX-License-Identifier: BUSL-1.1

//! PITR/restore utilities: timestamp parsing, dry-run validation, and restore planning.
//!
//! Extracted from `snapshot.rs` — contains standalone functions and types
//! used for Point-In-Time Recovery planning and validation.

use crate::types::Lsn;

use super::snapshot::SnapshotMeta;

/// Result of a PITR target resolution.
#[derive(Debug, Clone)]
pub struct PitrTarget {
    /// The closest base snapshot to restore from.
    pub base_snapshot: SnapshotMeta,
    /// Delta snapshots to apply in order (oldest first).
    pub deltas: Vec<SnapshotMeta>,
    /// Target LSN resolved from the requested UTC timestamp.
    pub replay_lsn: Lsn,
    /// Number of WAL records to replay after snapshot restore.
    pub wal_records_to_replay: u64,
}

/// Dry-run result for restore validation.
#[derive(Debug, Clone)]
pub struct RestoreDryRun {
    /// Whether the restore plan is valid.
    pub valid: bool,
    /// Human-readable description of what would happen.
    pub plan_description: String,
    /// Estimated time for restore (microseconds).
    pub estimated_duration_us: u64,
    /// Number of snapshot files to read.
    pub files_to_read: usize,
    /// Number of WAL records to replay.
    pub wal_records: u64,
    /// Issues found during validation.
    pub issues: Vec<String>,
}

/// Validate a restore plan without executing it.
pub fn dry_run_restore(target: &PitrTarget) -> RestoreDryRun {
    let mut issues = Vec::new();
    let files_to_read = 1 + target.deltas.len(); // base + deltas

    // Validate delta chain continuity.
    let mut expected_lsn = target.base_snapshot.end_lsn;
    for delta in &target.deltas {
        if delta.begin_lsn > expected_lsn {
            issues.push(format!(
                "gap in delta chain: expected begin_lsn <= {}, got {}",
                expected_lsn.as_u64(),
                delta.begin_lsn.as_u64()
            ));
        }
        expected_lsn = delta.end_lsn;
    }

    // Check that replay LSN is reachable.
    if target.replay_lsn < target.base_snapshot.begin_lsn {
        issues.push(format!(
            "replay LSN {} is before base snapshot begin {}",
            target.replay_lsn.as_u64(),
            target.base_snapshot.begin_lsn.as_u64()
        ));
    }

    let plan_description = format!(
        "Restore base snapshot #{} (LSN {}-{}), apply {} deltas, replay {} WAL records to LSN {}",
        target.base_snapshot.snapshot_id,
        target.base_snapshot.begin_lsn.as_u64(),
        target.base_snapshot.end_lsn.as_u64(),
        target.deltas.len(),
        target.wal_records_to_replay,
        target.replay_lsn.as_u64(),
    );

    // Rough estimate: 100MB/s for snapshot reads + 10K WAL records/sec.
    let total_snapshot_bytes: u64 =
        target.base_snapshot.data_bytes + target.deltas.iter().map(|d| d.data_bytes).sum::<u64>();
    let snapshot_us = (total_snapshot_bytes as f64 / 100_000_000.0 * 1_000_000.0) as u64;
    let wal_us = target.wal_records_to_replay * 100; // 100us per record

    RestoreDryRun {
        valid: issues.is_empty(),
        plan_description,
        estimated_duration_us: snapshot_us + wal_us,
        files_to_read,
        wal_records: target.wal_records_to_replay,
        issues,
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
