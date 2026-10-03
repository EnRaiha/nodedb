// SPDX-License-Identifier: BUSL-1.1

//! Execute a restore plan: write the base, then the archived WAL cut at the
//! target, into the empty data directory. A node restore of a cluster member
//! then brings the catalogs to the target and starts the metadata log.
//!
//! The next boot over the directory needs no flag. It opens the WAL as it
//! always does: replay reads every segment, each core skips records at or
//! below its restored floor, and the writer resumes after the last record,
//! which the cut placed at the target.

use super::archive::{Archive, Fetched};
use super::env::RestoreEnv;
use super::error::RestoreError;
use super::life::Life;
use super::node_metadata::{NodeMetadata, branch_node_timeline, install_node_metadata};
use super::plan::{PlannedSegment, RestorePlan};
use super::segment::cut_segment;
use crate::storage::snapshot_executor::{
    RestoreResult, RestoreSource, discard_wal_seed, execute_restore, seed_first_lsn,
    wal_segment_rel,
};
use crate::storage::snapshot_files::{RestoreWriter, clear_dir_contents};

/// What a restore wrote.
#[derive(Debug, Clone)]
pub struct RestoreOutcome {
    pub base: RestoreResult,
    pub wal_segments: u64,
    pub wal_bytes: u64,
    /// Highest LSN the restored WAL holds, write-abort markers included.
    pub wal_last_lsn: Option<u64>,
}

/// Write `plan` into `env.data_dir`. A failure leaves the directory empty.
pub async fn execute_plan(
    env: &RestoreEnv,
    life: &Life,
    archive: &Archive<'_>,
    plan: &RestorePlan,
) -> Result<RestoreOutcome, RestoreError> {
    let cold_store = env.cold.object_store();
    let source = RestoreSource {
        prefix: &plan.base.prefix,
        snapshot_store: &life.store,
        cold_store: Some(&cold_store),
        encryption_key: &env.key,
    };
    let base = execute_restore(&env.data_dir, &source).await?;
    let written = match write_wal(env, archive, plan, base.applied_high_lsn.as_u64()).await {
        Ok(wal) => match &plan.metadata {
            Some(meta) => install_metadata(env, meta, plan.surrogate_hwm)
                .await
                .map(|()| wal),
            None => Ok(wal),
        },
        Err(error) => Err(error),
    };
    match written {
        Ok((wal_segments, wal_bytes, wal_last_lsn)) => Ok(RestoreOutcome {
            base,
            wal_segments,
            wal_bytes,
            wal_last_lsn,
        }),
        Err(error) => Err(match clear_dir_contents(&env.data_dir) {
            Ok(()) => error,
            Err(cleanup) => RestoreError::CleanupFailed {
                error: Box::new(error),
                cleanup: Box::new(RestoreError::from(cleanup)),
                data_dir: env.data_dir.clone(),
            },
        }),
    }
}

/// Start the restored node's metadata timeline, then bring its catalogs and
/// metadata log to the target on it.
async fn install_metadata(
    env: &RestoreEnv,
    meta: &NodeMetadata,
    wal_surrogate_hwm: Option<u32>,
) -> Result<(), RestoreError> {
    let timeline = branch_node_timeline(env, meta).await?;
    install_node_metadata(&env.data_dir, meta, timeline, wal_surrogate_hwm)
}

/// Write every planned segment, the last one cut at the target, then drop
/// the seed segment they supersede.
async fn write_wal(
    env: &RestoreEnv,
    archive: &Archive<'_>,
    plan: &RestorePlan,
    applied_high: u64,
) -> Result<(u64, u64, Option<u64>), RestoreError> {
    let Some((last, earlier)) = plan.segments.split_last() else {
        return Ok((0, 0, None));
    };
    let mut writer = RestoreWriter::new(&env.data_dir);
    for planned in earlier {
        let fetched = fetch_planned(archive, planned).await?;
        writer.write(&wal_segment_rel(planned.first_lsn), &fetched.bytes)?;
    }
    let fetched = fetch_planned(archive, last).await?;
    let cut = cut_segment(
        &fetched.key,
        &fetched.bytes,
        plan.target_lsn,
        &plan.refused,
        plan.surrogate_hwm,
        env.alignment,
    )?;
    writer.write(&wal_segment_rel(last.first_lsn), &cut.bytes)?;

    // A planned segment named like the seed has already replaced it.
    let seed = seed_first_lsn(applied_high)?;
    if !plan.segments.iter().any(|seg| seg.first_lsn == seed) {
        discard_wal_seed(&env.data_dir, applied_high)?;
    }
    let (files, bytes) = writer.finish()?;
    Ok((files, bytes, cut.last_lsn))
}

/// Fetch a planned segment and refuse it unless it is the image planning
/// checked.
async fn fetch_planned(
    archive: &Archive<'_>,
    planned: &PlannedSegment,
) -> Result<Fetched, RestoreError> {
    let fetched = archive.fetch(planned.index).await?;
    if fetched.crc32c != planned.crc32c {
        return Err(RestoreError::SegmentChanged {
            key: fetched.key,
            planned: planned.crc32c,
            actual: fetched.crc32c,
        });
    }
    Ok(fetched)
}
