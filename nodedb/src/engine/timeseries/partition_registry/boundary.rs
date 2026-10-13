// SPDX-License-Identifier: BUSL-1.1

//! The interval a timestamp falls in, for a partition interval.
//!
//! A fixed duration aligns to multiples of itself from the epoch. A calendar
//! month starts at 00:00 UTC on the first day of the month, and a calendar
//! year at 00:00 UTC on 1 January. Both are exact: a month is 28 to 31 days
//! and a year 365 or 366.

use nodedb_types::timeseries::PartitionInterval;

const DAY_MS: i64 = 86_400_000;

/// The `[start, end)` interval, in milliseconds, that `ts_ms` falls in.
///
/// `None` for an interval that never splits rows: `Unbounded`, and `Auto`,
/// which the caller resolves to a concrete interval first.
pub fn interval_bounds(interval: &PartitionInterval, ts_ms: i64) -> Option<(i64, i64)> {
    match interval {
        PartitionInterval::Duration(ms) => {
            let ms = i64::try_from(*ms).ok().filter(|ms| *ms > 0)?;
            let start = ts_ms.div_euclid(ms).saturating_mul(ms);
            Some((start, start.saturating_add(ms)))
        }
        PartitionInterval::Month => {
            let (year, month, _) = civil_from_days(ts_ms.div_euclid(DAY_MS));
            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            Some((
                days_from_civil(year, month, 1).saturating_mul(DAY_MS),
                days_from_civil(next_year, next_month, 1).saturating_mul(DAY_MS),
            ))
        }
        PartitionInterval::Year => {
            let (year, _, _) = civil_from_days(ts_ms.div_euclid(DAY_MS));
            Some((
                days_from_civil(year, 1, 1).saturating_mul(DAY_MS),
                days_from_civil(year + 1, 1, 1).saturating_mul(DAY_MS),
            ))
        }
        // `PartitionInterval` is non-exhaustive: a variant this engine does not
        // know never splits rows.
        _ => None,
    }
}

/// `(year, month, day)` of the day `days` after 1970-01-01, proleptic
/// Gregorian.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// The day number, counted from 1970-01-01, of `year-month-day`, proleptic
/// Gregorian.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year.rem_euclid(400);
    let month = i64::from(month);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2024-01-01T00:00:00Z.
    const JAN_2024: i64 = 1_704_067_200_000;
    /// 2024-02-01T00:00:00Z.
    const FEB_2024: i64 = 1_706_745_600_000;
    /// 2024-03-01T00:00:00Z: 2024 is a leap year, so February has 29 days.
    const MAR_2024: i64 = 1_709_251_200_000;
    /// 2025-01-01T00:00:00Z.
    const JAN_2025: i64 = 1_735_689_600_000;

    #[test]
    fn a_duration_aligns_to_its_multiples() {
        let hour = PartitionInterval::Duration(3_600_000);
        assert_eq!(
            interval_bounds(&hour, 3_600_000 + 5),
            Some((3_600_000, 7_200_000))
        );
        assert_eq!(interval_bounds(&hour, -1), Some((-3_600_000, 0)));
    }

    #[test]
    fn a_month_starts_on_the_first_of_the_calendar_month() {
        assert_eq!(
            interval_bounds(&PartitionInterval::Month, JAN_2024 + 5 * DAY_MS),
            Some((JAN_2024, FEB_2024))
        );
        assert_eq!(
            interval_bounds(&PartitionInterval::Month, FEB_2024 + 28 * DAY_MS),
            Some((FEB_2024, MAR_2024)),
            "29 February 2024 is in February"
        );
        assert_eq!(
            interval_bounds(&PartitionInterval::Month, MAR_2024 - 1),
            Some((FEB_2024, MAR_2024))
        );
        assert_eq!(
            interval_bounds(&PartitionInterval::Month, MAR_2024),
            Some((MAR_2024, MAR_2024 + 31 * DAY_MS))
        );
    }

    #[test]
    fn december_rolls_into_the_next_year() {
        let dec_2024 = JAN_2025 - 31 * DAY_MS;
        assert_eq!(
            interval_bounds(&PartitionInterval::Month, JAN_2025 - 1),
            Some((dec_2024, JAN_2025))
        );
    }

    #[test]
    fn a_year_starts_on_the_first_of_january() {
        assert_eq!(
            interval_bounds(&PartitionInterval::Year, FEB_2024),
            Some((JAN_2024, JAN_2025))
        );
    }

    #[test]
    fn a_date_before_the_epoch_falls_in_its_own_month() {
        // 1969-12-31T23:59:59.999Z is in December 1969.
        let dec_1969 = -31 * DAY_MS;
        assert_eq!(
            interval_bounds(&PartitionInterval::Month, -1),
            Some((dec_1969, 0))
        );
    }

    #[test]
    fn unbounded_and_auto_never_split() {
        assert_eq!(interval_bounds(&PartitionInterval::Unbounded, 5), None);
        assert_eq!(interval_bounds(&PartitionInterval::Auto, 5), None);
    }
}
