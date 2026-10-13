// SPDX-License-Identifier: Apache-2.0

//! AUTO_TIER routing of one timeseries scan across retention tiers.
//!
//! A tiered retention policy keeps raw rows for `raw_retain_ms`, then
//! downsampled buckets in one continuous aggregate per tier. A scan carries
//! the policy's tier layout; the core that owns the collection splits the
//! query range into segments and reads each segment from its tier.

/// The tier layout of an AUTO_TIER retention policy.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TieringPlan {
    /// How long raw rows are kept, in milliseconds. 0 = forever.
    pub raw_retain_ms: u64,
    /// Downsample tiers, finest first.
    pub tiers: Vec<TierRead>,
}

/// One downsample tier of a [`TieringPlan`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TierRead {
    /// The policy's tier number (1 = first downsample tier).
    pub tier_index: u32,
    /// The continuous aggregate that holds this tier's buckets.
    pub aggregate: String,
    /// Bucket width in milliseconds.
    pub resolution_ms: i64,
    /// How long this tier keeps buckets, in milliseconds. 0 = forever.
    pub retain_ms: u64,
}

/// Which source serves one segment of a tiered scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierSource {
    /// Raw rows of the collection.
    Raw,
    /// Buckets of `TieringPlan::tiers[index]`.
    Tier(usize),
}

/// One time segment of a tiered scan: `[start, end)` read from `source`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierSegment {
    pub source: TierSource,
    pub start: i64,
    pub end: i64,
}

/// Why a tier layout cannot split a range.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TieringError {
    #[error(
        "tier {tier_index} resolution {resolution_ms}ms is not a positive multiple of the \
         finer tier's {finer_ms}ms; a coarser bucket would straddle the finer tier's edge"
    )]
    UnalignedResolution {
        tier_index: u32,
        resolution_ms: i64,
        finer_ms: i64,
    },
}

impl TieringPlan {
    /// Split `query` (an inclusive `(min, max)` millisecond range) into
    /// segments, newest first.
    ///
    /// The edge between two sources is the retention boundary rounded up to
    /// the coarser tier's resolution, so every bucket a tier serves lies
    /// wholly before the edge and no row is read from two sources.
    pub fn segments(
        &self,
        query: (i64, i64),
        now_ms: i64,
    ) -> Result<Vec<TierSegment>, TieringError> {
        let (query_start, query_end) = query;
        let mut segments = Vec::new();
        // `upper` is the exclusive end of the source being placed.
        let mut upper = query_end.saturating_add(1);
        let mut source = TierSource::Raw;
        let mut boundary = now_ms;
        let mut retain = self.raw_retain_ms;
        let mut finer_ms = 1i64;
        for (index, tier) in self.tiers.iter().enumerate() {
            if retain == 0 {
                break;
            }
            if tier.resolution_ms <= 0 || tier.resolution_ms % finer_ms != 0 {
                return Err(TieringError::UnalignedResolution {
                    tier_index: tier.tier_index,
                    resolution_ms: tier.resolution_ms,
                    finer_ms,
                });
            }
            boundary = boundary.saturating_sub(retain as i64);
            let edge = align_up(boundary, tier.resolution_ms);
            push_segment(&mut segments, source, edge.max(query_start), upper);
            upper = upper.min(edge);
            source = TierSource::Tier(index);
            retain = tier.retain_ms;
            finer_ms = tier.resolution_ms;
        }
        push_segment(&mut segments, source, query_start, upper);
        Ok(segments)
    }
}

/// Append `[start, end)` when it is non-empty.
fn push_segment(out: &mut Vec<TierSegment>, source: TierSource, start: i64, end: i64) {
    if start < end {
        out.push(TierSegment { source, start, end });
    }
}

/// `ms` rounded up to a multiple of `step` (`step > 0`).
fn align_up(ms: i64, step: i64) -> i64 {
    let floor = ms.div_euclid(step).saturating_mul(step);
    if floor == ms {
        floor
    } else {
        floor.saturating_add(step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000;

    fn plan() -> TieringPlan {
        TieringPlan {
            raw_retain_ms: 7 * DAY as u64,
            tiers: vec![
                TierRead {
                    tier_index: 1,
                    aggregate: "_policy_p_tier1".into(),
                    resolution_ms: 60_000,
                    retain_ms: 90 * DAY as u64,
                },
                TierRead {
                    tier_index: 2,
                    aggregate: "_policy_p_tier2".into(),
                    resolution_ms: 3_600_000,
                    retain_ms: 0,
                },
            ],
        }
    }

    #[test]
    fn recent_range_reads_raw_only() {
        let now = 100_000 * DAY;
        let segments = plan().segments((now - 3_600_000, now), now).expect("split");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].source, TierSource::Raw);
        assert_eq!(segments[0].end, now + 1);
    }

    #[test]
    fn range_spanning_tiers_splits_on_aligned_edges() {
        let now = 100_000 * DAY + 12_345;
        let segments = plan().segments((now - 200 * DAY, now), now).expect("split");
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].source, TierSource::Raw);
        assert_eq!(segments[1].source, TierSource::Tier(0));
        assert_eq!(segments[2].source, TierSource::Tier(1));
        // Edges are aligned to the coarser source's resolution and contiguous.
        assert_eq!(segments[0].start, segments[1].end);
        assert_eq!(segments[1].start, segments[2].end);
        assert_eq!(segments[0].start % 60_000, 0);
        assert_eq!(segments[1].start % 3_600_000, 0);
    }

    #[test]
    fn forever_raw_retention_never_reads_a_tier() {
        let mut plan = plan();
        plan.raw_retain_ms = 0;
        let now = 100_000 * DAY;
        let segments = plan.segments((0, now), now).expect("split");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].source, TierSource::Raw);
    }

    #[test]
    fn unaligned_resolution_is_refused() {
        let mut plan = plan();
        plan.tiers[1].resolution_ms = 90_000;
        let now = 100_000 * DAY;
        assert!(matches!(
            plan.segments((0, now), now),
            Err(TieringError::UnalignedResolution { tier_index: 2, .. })
        ));
    }
}
