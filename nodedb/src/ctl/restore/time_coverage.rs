// SPDX-License-Identifier: BUSL-1.1

//! Resolve a time target to an LSN from the archived time anchors.
//!
//! A time target resolves to the newest anchor committed at or before it.
//! An anchor in a missing segment is invisible, so the newest anchor the
//! archive shows can be older than the target's true LSN. The resolution
//! stands only when the archive holds every record from the resolved LSN up
//! to the first anchor past the target.

use nodedb_types::temporal::LsnTimeError;

use super::error::RestoreError;
use super::life::Base;
use super::plan::Scanner;
use super::timeline::Timeline;
use crate::storage::snapshot_restore::{CoverageStep, WalCoverage};

/// Resolve `target_ns` to an LSN from the archived time anchors.
///
/// Bases are tried newest first. Each reads anchors from the segment holding
/// its replay start until an anchor passes the target, so a recent target
/// reads only recent WAL. The first LSN that some base can reach wins.
pub(super) async fn resolve_time(
    scanner: &mut Scanner<'_, '_>,
    bases: &[Base],
    target_ns: u64,
    incarnation: &str,
) -> Result<u64, RestoreError> {
    let mut miss = RestoreError::NoTimeAnchors {
        incarnation: incarnation.to_string(),
    };
    for base in bases.iter().rev() {
        let replay_start = base.meta.begin_lsn.as_u64().saturating_add(1);
        let start = scanner.archive.containing(replay_start).unwrap_or(0);
        let (timeline, first_past) = anchors_from(scanner, start, target_ns).await?;
        let applied_high = base.meta.applied_high_lsn.as_u64();
        miss = match timeline.lsn_at_or_before(target_ns) {
            Ok(lsn) if lsn >= applied_high => {
                if let Some(first_past) = first_past {
                    require_contiguous(scanner, lsn.saturating_add(1), first_past).await?;
                }
                return Ok(lsn);
            }
            Ok(lsn) => RestoreError::NoBaseAtOrBelow {
                target_lsn: lsn,
                oldest_applied_high_lsn: applied_high,
            },
            Err(LsnTimeError::BeforeFirstAnchor {
                first_anchor_ns, ..
            }) => RestoreError::TargetBeforeFirstAnchor {
                target_ns,
                first_anchor_ns,
            },
            Err(_) => RestoreError::NoTimeAnchors {
                incarnation: incarnation.to_string(),
            },
        };
    }
    Err(miss)
}

/// Anchors of the segments from `start` through the first whose anchors
/// pass `target_ns`, or through the end of the archive, and the LSN of the
/// first anchor past `target_ns`, if any.
async fn anchors_from(
    scanner: &mut Scanner<'_, '_>,
    start: usize,
    target_ns: u64,
) -> Result<(Timeline, Option<u64>), RestoreError> {
    let mut anchors = Vec::new();
    for index in start..scanner.archive.segments().len() {
        let (_, scan) = scanner.scan(index).await?;
        let passes = scan
            .anchors
            .last()
            .is_some_and(|a| a.hlc_wall_ns > target_ns);
        anchors.extend(scan.anchors);
        if passes {
            break;
        }
    }
    let first_past = anchors
        .iter()
        .find(|a| a.hlc_wall_ns > target_ns)
        .map(|a| a.lsn);
    Ok((Timeline::from_anchors(anchors), first_past))
}

/// Check that the archive holds every LSN in `from..=through`. A gap fails
/// with the missing range.
async fn require_contiguous(
    scanner: &mut Scanner<'_, '_>,
    from: u64,
    through: u64,
) -> Result<(), RestoreError> {
    if from > through {
        return Ok(());
    }
    let archive = scanner.archive;
    let mut coverage = WalCoverage::new(from, through);
    let Some(mut index) = archive.containing(from) else {
        let next = archive.segments().first().map(|segment| segment.first_lsn);
        return Err(coverage.missing(next).into());
    };
    loop {
        let Some(segment) = archive.segments().get(index) else {
            return Err(coverage.missing(None).into());
        };
        if segment.first_lsn > through {
            return Err(coverage.missing(Some(segment.first_lsn)).into());
        }
        let (_, scan) = scanner.scan(index).await?;
        if coverage.feed(segment.first_lsn, scan.last_lsn)? == CoverageStep::Covered {
            return Ok(());
        }
        index += 1;
    }
}
