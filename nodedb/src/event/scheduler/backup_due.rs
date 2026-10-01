// SPDX-License-Identifier: BUSL-1.1

//! Due selection for scheduled backups, from the replicated schedule mark.
//!
//! The mark says through which scheduled minute a schedule is settled. The
//! node running scheduled backups compares it with the clock. Every node
//! reads the same mark, so the decision does not depend on which node made
//! the earlier runs.

use super::cron::CronExpr;

/// What the node running scheduled backups does for one schedule now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupStep {
    /// Back up for this scheduled unix minute, then raise the mark to it.
    Run(u64),
    /// The schedule has no mark: raise it to this unix minute, so a minute
    /// due after it runs even when another node takes over first.
    Arm(u64),
    /// Nothing is due.
    Idle,
}

/// The step for a schedule settled through `mark` at unix minute `now_min`.
///
/// With a mark, the newest matching minute above it and at or below
/// `now_min` runs. Several missed minutes collapse into that one run: a
/// backup is a full image. With no mark, only `now_min` itself can run, so a
/// new schedule never back-fills minutes before it existed.
pub fn next_step(
    mark: Option<u64>,
    now_min: u64,
    cron: &CronExpr,
    utc_offset_seconds: i32,
) -> BackupStep {
    let matches =
        |minute: u64| cron.matches_epoch_with_offset(minute.saturating_mul(60), utc_offset_seconds);
    match mark {
        None if matches(now_min) => BackupStep::Run(now_min),
        None => BackupStep::Arm(now_min),
        Some(through) => (through.saturating_add(1)..=now_min)
            .rev()
            .find(|&minute| matches(minute))
            .map_or(BackupStep::Idle, BackupStep::Run),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_five() -> CronExpr {
        CronExpr::parse("*/5 * * * *").unwrap()
    }

    #[test]
    fn a_new_schedule_arms_or_runs_the_current_minute_only() {
        assert_eq!(next_step(None, 7, &every_five(), 0), BackupStep::Arm(7));
        assert_eq!(next_step(None, 10, &every_five(), 0), BackupStep::Run(10));
    }

    #[test]
    fn a_due_minute_above_the_mark_runs_whoever_reads_it() {
        // Armed at 7, minute 10 was due, and the clock reads 12.
        assert_eq!(
            next_step(Some(7), 12, &every_five(), 0),
            BackupStep::Run(10)
        );
        assert_eq!(next_step(Some(10), 12, &every_five(), 0), BackupStep::Idle);
        assert_eq!(next_step(Some(10), 10, &every_five(), 0), BackupStep::Idle);
    }

    #[test]
    fn missed_minutes_collapse_into_the_newest() {
        assert_eq!(
            next_step(Some(4), 31, &every_five(), 0),
            BackupStep::Run(30)
        );
        assert_eq!(next_step(Some(30), 34, &every_five(), 0), BackupStep::Idle);
    }

    #[test]
    fn the_timezone_offset_shifts_the_matching_minutes() {
        let hourly = CronExpr::parse("0 * * * *").unwrap();
        // UTC minute 90 is 01:30 UTC, and 02:00 at +00:30.
        assert_eq!(next_step(Some(60), 90, &hourly, 0), BackupStep::Idle);
        assert_eq!(next_step(Some(60), 90, &hourly, 1_800), BackupStep::Run(90));
    }
}
