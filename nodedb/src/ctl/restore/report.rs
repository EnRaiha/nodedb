// SPDX-License-Identifier: BUSL-1.1

//! Operator-facing text for a restore plan and its outcome.

use std::fmt;
use std::path::PathBuf;

use nodedb_wal::segment::segment_filename;

use super::error::{format_micros, format_nanos};
use super::execute::RestoreOutcome;
use super::plan::RestorePlan;

/// A plan, and what executing it wrote. `outcome` is `None` for a dry run.
pub struct RestoreReport {
    pub data_dir: PathBuf,
    pub plan: RestorePlan,
    pub outcome: Option<RestoreOutcome>,
}

impl fmt::Display for RestoreReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let plan = &self.plan;
        let base = &plan.base.meta;
        match &self.outcome {
            None => writeln!(f, "restore plan (dry run, nothing written)")?,
            Some(_) => writeln!(f, "restore complete")?,
        }
        writeln!(f, "  data dir:     {}", self.data_dir.display())?;
        writeln!(
            f,
            "  node:         {} incarnation {}",
            plan.node_id, plan.incarnation
        )?;
        writeln!(
            f,
            "  base:         snapshot {} ({}), created {}, {} bytes",
            base.snapshot_id,
            plan.base.prefix,
            format_micros(base.created_at_us),
            base.data_bytes
        )?;
        writeln!(
            f,
            "                replays from LSN {}, holds writes through LSN {}",
            plan.replay_start(),
            base.applied_high_lsn.as_u64()
        )?;
        write!(f, "  target:       LSN {}", plan.target_lsn)?;
        if let Some(ns) = plan.target_commit_ns {
            write!(f, ", committed {}", format_nanos(ns))?;
        }
        if let Some(us) = plan.requested_us {
            write!(f, " (requested {})", format_micros(us))?;
        }
        writeln!(f)?;
        match plan.segments.first().zip(plan.segments.last()) {
            None => writeln!(f, "  WAL:          none; the base reaches the target")?,
            Some((first, last)) => {
                writeln!(
                    f,
                    "  WAL:          {} segments, LSN {} through {}, {} bytes to fetch",
                    plan.segments.len(),
                    first.first_lsn,
                    plan.target_lsn,
                    plan.wal_bytes()
                )?;
                for seg in &plan.segments {
                    writeln!(
                        f,
                        "                {} ({} bytes)",
                        segment_filename(seg.first_lsn),
                        seg.size
                    )?;
                }
                if let Some(cut) = &plan.cut {
                    writeln!(
                        f,
                        "  cut:          {} keeps {} bytes, drops {} records above the target",
                        segment_filename(last.first_lsn),
                        cut.kept_bytes,
                        cut.dropped_records
                    )?;
                }
            }
        }
        if !plan.refused.is_empty() {
            writeln!(
                f,
                "  refused:      {} records at or below the target were refused later; \
                 replay drops them",
                plan.refused.len()
            )?;
        }
        if let Some(through) = plan.aborts_scanned_through {
            writeln!(f, "  abort scan:   archived WAL read through LSN {through}")?;
        }
        if let Some(meta) = &plan.metadata {
            writeln!(
                f,
                "  metadata:     log entries {}..={} replayed after the base, each stamped \
                 below {}ns kept; the archive covers through {}ns",
                meta.tail.base_index.saturating_add(1),
                meta.through,
                meta.watermark,
                meta.archived_through_ns
            )?;
        }
        if let Some(outcome) = &self.outcome {
            writeln!(
                f,
                "  written:      {} base files ({} bytes), {} WAL segments ({} bytes)",
                outcome.base.files_restored,
                outcome.base.bytes_restored,
                outcome.wal_segments,
                outcome.wal_bytes
            )?;
            if let Some(lsn) = outcome.wal_last_lsn {
                writeln!(
                    f,
                    "  WAL ends:     LSN {lsn}; the next write takes LSN {}",
                    lsn.saturating_add(1)
                )?;
            }
            writeln!(
                f,
                "  next boot replays the base plus WAL through the target; start the server"
            )?;
        }
        Ok(())
    }
}
