// SPDX-License-Identifier: BUSL-1.1

//! Per-side read capture for the distributed shuffle-JOIN resolver.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::types::Lsn;

/// One join-side's read observation from a distributed shuffle join.
///
/// The shuffle-JOIN resolver records TWO of these — one for the probe (left)
/// collection and one for the build (right) collection — each carrying that
/// side's own bare full-collection scan plan and the REAL per-collection
/// read-version LSN its producers observed. The record seam re-derives the
/// collection / engine / read key from `scan_plan` (a single-collection scan,
/// so it is NOT collapsed to just the left side the way a `HashJoin` plan is)
/// and stamps the entry at `read_version_lsn`, so the commit-time OCC validator
/// re-homes and revalidates each side's vshard independently. This closes the
/// serializability hole where a build-side collection never appeared in the
/// read-set and a concurrent write to it went undetected.
pub struct ShuffleReadCapture {
    pub scan_plan: PhysicalPlan,
    pub read_version_lsn: Lsn,
}
