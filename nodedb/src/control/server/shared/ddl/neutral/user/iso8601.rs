// SPDX-License-Identifier: BUSL-1.1

//! ISO-8601 / RFC-3339 datetime parsing for `ALTER USER ... PASSWORD
//! EXPIRES`. Kept dependency-free — no external datetime crate.

/// Parse an ISO-8601 / RFC-3339 datetime string to a Unix timestamp (seconds).
///
/// Accepts:
/// - `2026-12-31` — date only, interpreted as midnight UTC
/// - `2026-12-31T23:59:59Z` — UTC datetime
/// - `2026-12-31T23:59:59` — datetime with no zone, interpreted as UTC
/// - `2026-12-31T23:59:59+05:30` — datetime with offset (converted to UTC)
/// - `2026-12-31 23:59:59Z` — space date/time separator
/// - a fractional-second suffix (`.sss`) is accepted and truncated
///
/// Every malformed component (out-of-range field, non-numeric text, bad
/// timezone) is rejected with a typed [`crate::Error::BadRequest`] — the
/// parser never falls back to a default value.
pub(super) fn parse_iso8601_to_unix(s: &str) -> crate::Result<u64> {
    let s = s.trim();

    // Date-only: YYYY-MM-DD.
    if s.len() == 10 {
        return parse_date_to_unix(s);
    }

    // Datetime: YYYY-MM-DD<sep>HH:MM[:SS][.frac][zone]. The shortest
    // valid form is YYYY-MM-DD then T then HH:MM (16 characters).
    if s.len() < 16 {
        return Err(bad(format!("unrecognised datetime format: '{s}'")));
    }
    let sep = s.as_bytes()[10];
    if sep != b'T' && sep != b't' && sep != b' ' {
        return Err(bad(format!(
            "expected 'T' or space between date and time in '{s}'"
        )));
    }

    let date_secs = parse_date_to_unix(&s[..10])?;
    let (time_str, tz_offset_secs) = split_timezone(&s[11..])?;
    let time_secs = parse_time_of_day(time_str)?;

    // A local wall-clock time at offset `+OFF` is UTC `local - OFF`.
    let local = date_secs as i64 + time_secs as i64;
    let utc = local - tz_offset_secs;
    if utc < 0 {
        return Err(bad(format!("datetime before Unix epoch: '{s}'")));
    }
    Ok(utc as u64)
}

/// Split a trailing timezone designator off a time string.
///
/// Returns the time-of-day substring and the offset east of UTC in
/// seconds. `Z`/`z` and a missing designator both yield offset `0`.
fn split_timezone(rest: &str) -> crate::Result<(&str, i64)> {
    if let Some(stripped) = rest.strip_suffix(['Z', 'z']) {
        return Ok((stripped, 0));
    }
    // The time-of-day itself contains no '+'/'-', so the first one marks
    // the start of a numeric offset.
    let Some(sign_idx) = rest.find(['+', '-']) else {
        return Ok((rest, 0));
    };
    let sign: i64 = if rest.as_bytes()[sign_idx] == b'-' {
        -1
    } else {
        1
    };
    let digits: String = rest[sign_idx + 1..]
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();
    let (oh, om) = match digits.len() {
        2 => (parse_u(&digits, "offset hour")?, 0),
        4 => (
            parse_u(&digits[..2], "offset hour")?,
            parse_u(&digits[2..], "offset minute")?,
        ),
        _ => return Err(bad(format!("malformed timezone offset: '{rest}'"))),
    };
    if oh > 14 || om > 59 {
        return Err(bad(format!("timezone offset out of range: '{rest}'")));
    }
    Ok((
        &rest[..sign_idx],
        sign * (oh as i64 * 3600 + om as i64 * 60),
    ))
}

/// Parse `HH:MM` or `HH:MM:SS` (with an optional fractional-second
/// suffix) to a count of seconds since midnight.
fn parse_time_of_day(t: &str) -> crate::Result<u64> {
    // Sub-second precision is not stored for password expiry — drop it.
    let t = t.split('.').next().unwrap_or(t);
    let parts: Vec<&str> = t.split(':').collect();
    if parts.len() != 2 && parts.len() != 3 {
        return Err(bad(format!("expected HH:MM[:SS], got '{t}'")));
    }
    let h = parse_u(parts[0], "hour")?;
    let m = parse_u(parts[1], "minute")?;
    let sec = match parts.get(2) {
        Some(s) => parse_u(s, "second")?,
        None => 0,
    };
    if h > 23 {
        return Err(bad(format!("hour out of range (0-23): {h}")));
    }
    if m > 59 {
        return Err(bad(format!("minute out of range (0-59): {m}")));
    }
    // 60 is permitted for a positive leap second.
    if sec > 60 {
        return Err(bad(format!("second out of range (0-60): {sec}")));
    }
    Ok(h * 3600 + m * 60 + sec)
}

/// Parse YYYY-MM-DD to midnight-UTC Unix timestamp.
fn parse_date_to_unix(s: &str) -> crate::Result<u64> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 || parts[0].len() != 4 || parts[1].len() != 2 || parts[2].len() != 2 {
        return Err(bad(format!("expected YYYY-MM-DD, got '{s}'")));
    }
    let y = parse_u(parts[0], "year")? as i64;
    let mo = parse_u(parts[1], "month")?;
    let d = parse_u(parts[2], "day")?;
    if !(1..=12).contains(&mo) {
        return Err(bad(format!("month out of range (1-12) in '{s}'")));
    }
    let dim = days_in_month(y, mo);
    if !(1..=dim).contains(&d) {
        return Err(bad(format!("day out of range (1-{dim}) in '{s}'")));
    }
    Ok(days_since_epoch(y, mo, d)? * 86400)
}

/// Number of days in a given Gregorian month, leap-year aware.
fn days_in_month(y: i64, mo: u64) -> u64 {
    match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(y) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Gregorian leap-year rule.
fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days from the Unix epoch (1970-01-01) to the given Gregorian date.
fn days_since_epoch(y: i64, mo: u64, d: u64) -> crate::Result<u64> {
    // Julian Day Number formula for the Gregorian calendar.
    let a = (14_i64 - mo as i64) / 12;
    let yr = y + 4800 - a;
    let m = mo as i64 + 12 * a - 3;
    let jdn = d as i64 + (153 * m + 2) / 5 + 365 * yr + yr / 4 - yr / 100 + yr / 400 - 32045;
    // Unix epoch 1970-01-01 = JDN 2440588.
    let unix_days = jdn - 2_440_588;
    if unix_days < 0 {
        return Err(bad(format!("date before Unix epoch: {y}-{mo:02}-{d:02}")));
    }
    Ok(unix_days as u64)
}

/// Parse a non-negative integer field, rejecting any non-numeric text.
fn parse_u(s: &str, what: &str) -> crate::Result<u64> {
    s.trim()
        .parse::<u64>()
        .map_err(|_| bad(format!("invalid {what}: '{s}'")))
}

/// Construct a `BadRequest` error.
fn bad(detail: String) -> crate::Error {
    crate::Error::BadRequest { detail }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_only_is_midnight_utc() {
        // 2026-12-31 = 20818 days after the 1970-01-01 epoch.
        assert_eq!(parse_iso8601_to_unix("2026-12-31").unwrap(), 20818 * 86400);
    }

    #[test]
    fn time_of_day_is_preserved() {
        let base = parse_iso8601_to_unix("2026-12-31").unwrap();
        let dt = parse_iso8601_to_unix("2026-12-31T12:30:45Z").unwrap();
        assert_eq!(dt, base + 12 * 3600 + 30 * 60 + 45);
    }

    #[test]
    fn no_zone_is_treated_as_utc() {
        assert_eq!(
            parse_iso8601_to_unix("2026-12-31T12:30:45").unwrap(),
            parse_iso8601_to_unix("2026-12-31T12:30:45Z").unwrap(),
        );
    }

    #[test]
    fn positive_offset_converts_to_utc() {
        // 12:30 at +05:30 is 07:00 UTC.
        let utc = parse_iso8601_to_unix("2026-12-31T12:30:00Z").unwrap();
        let off = parse_iso8601_to_unix("2026-12-31T12:30:00+05:30").unwrap();
        assert_eq!(off, utc - (5 * 3600 + 30 * 60));
    }

    #[test]
    fn negative_offset_converts_to_utc() {
        let utc = parse_iso8601_to_unix("2026-12-31T12:30:00Z").unwrap();
        let off = parse_iso8601_to_unix("2026-12-31T12:30:00-08:00").unwrap();
        assert_eq!(off, utc + 8 * 3600);
    }

    #[test]
    fn space_separator_and_compact_offset() {
        assert_eq!(
            parse_iso8601_to_unix("2026-12-31 12:30:00+0530").unwrap(),
            parse_iso8601_to_unix("2026-12-31T12:30:00+05:30").unwrap(),
        );
    }

    #[test]
    fn fractional_seconds_are_truncated() {
        assert_eq!(
            parse_iso8601_to_unix("2026-12-31T12:30:45.999Z").unwrap(),
            parse_iso8601_to_unix("2026-12-31T12:30:45Z").unwrap(),
        );
    }

    #[test]
    fn hh_mm_without_seconds() {
        let base = parse_iso8601_to_unix("2026-12-31").unwrap();
        assert_eq!(
            parse_iso8601_to_unix("2026-12-31T23:59Z").unwrap(),
            base + 23 * 3600 + 59 * 60,
        );
    }

    #[test]
    fn malformed_components_are_rejected() {
        assert!(parse_iso8601_to_unix("2026-12-31T99:99:99Z").is_err());
        assert!(parse_iso8601_to_unix("2026-12-31Tbad-time").is_err());
        assert!(parse_iso8601_to_unix("2026-13-01").is_err());
        assert!(parse_iso8601_to_unix("2026-02-29").is_err()); // 2026 is not a leap year
        assert!(parse_iso8601_to_unix("2026-12-31T12:30:00+99:00").is_err());
        assert!(parse_iso8601_to_unix("not-a-date").is_err());
    }

    #[test]
    fn leap_day_accepted_in_leap_year() {
        assert!(parse_iso8601_to_unix("2024-02-29").is_ok());
    }
}
