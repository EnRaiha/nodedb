// SPDX-License-Identifier: BUSL-1.1

//! Operator-facing text for one node's part of a cluster restore.

use std::fmt;
use std::path::PathBuf;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;

use super::super::error::format_nanos;
use super::super::report::RestoreReport;
use super::execute::ClusterOutcome;
use super::plan::ClusterRestorePlan;

/// A cluster restore plan, and what executing it wrote. `outcome` is `None`
/// for a dry run.
pub struct ClusterReport {
    pub data_dir: PathBuf,
    pub plan: ClusterRestorePlan,
    pub outcome: Option<ClusterOutcome>,
}

impl fmt::Display for ClusterReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let plan = &self.plan;
        let node = RestoreReport {
            data_dir: self.data_dir.clone(),
            plan: plan.wal.clone(),
            outcome: self.outcome.as_ref().map(|outcome| outcome.node.clone()),
        };
        write!(f, "{node}")?;
        writeln!(
            f,
            "  point:        restore point {}, watermark {}",
            plan.restore_point,
            format_nanos(plan.watermark)
        )?;
        writeln!(
            f,
            "  dropped:      {} records at or below the target are at or above the watermark",
            plan.dropped_below_target
        )?;
        writeln!(
            f,
            "  metadata:     base catalogs at index {}, {} log entries through index {}",
            plan.metadata.base_index,
            plan.metadata.entries.len(),
            plan.restore_point
        )?;
        for place in &plan.groups {
            if place.group_id == SEQUENCER_GROUP_ID {
                writeln!(
                    f,
                    "  sequencer:    log index {}, next epoch {}",
                    place.index, place.next_epoch
                )?;
            } else {
                writeln!(f, "  group {:<7} log index {}", place.group_id, place.index)?;
            }
        }
        if let Some(outcome) = &self.outcome {
            writeln!(
                f,
                "  generation:   {}; every group starts at term {} and the cluster epoch is {}",
                outcome.generation, outcome.fence, outcome.fence
            )?;
            if !outcome.unrecorded_groups.is_empty() {
                writeln!(
                    f,
                    "  unrecorded:   groups {:?} recorded no place here; each catches up from \
                     its leader",
                    outcome.unrecorded_groups
                )?;
            }
            writeln!(
                f,
                "  run the restore on every other node of the cluster, then start every node"
            )?;
        }
        Ok(())
    }
}
