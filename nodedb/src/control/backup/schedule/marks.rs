// SPDX-License-Identifier: BUSL-1.1

//! Durable, replicated progress of each scheduled backup.
//!
//! A schedule's mark is the scheduled minute through which every run is
//! settled. The node running scheduled backups raises it through the
//! metadata group after each completed run, so a node that takes that role
//! later reads the same mark.
//!
//! The scheduler reads a mark only after this node applied the metadata
//! group through a read index its leader confirmed. A node whose catalog
//! lags therefore never mistakes a finished minute for a due one.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::blocking::off_runtime;
use crate::config::server::BackupScheduleSettings;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::cluster::linearizable_read::confirm_linearizable_read;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::backup_schedule_marks::StoredBackupScheduleMark;
use crate::control::state::SharedState;
use crate::event::scheduler::coordinator::ensure_system_coordinator;

/// How long a mark read waits for its read index and for this node to apply
/// through it. A read that does not finish in time skips its tick.
const MARK_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The minute through which `schedule` is settled in this node's catalog, or
/// `None` when it was never armed at its current config incarnation. The
/// value can lag the metadata group: the scheduler uses
/// [`settled_through_linearizable`].
pub fn settled_through(
    state: &SharedState,
    schedule: &BackupScheduleSettings,
) -> crate::Result<Option<u64>> {
    Ok(state
        .credentials
        .catalog()
        .backup_schedule_mark(&schedule.job_name(), schedule.incarnation())?
        .map(|mark| mark.through_minute))
}

/// The mark of `schedule` as of now: every mark committed before this call is
/// applied here first. Fails when the metadata group confirms no read index,
/// or this node does not apply through it within [`MARK_READ_TIMEOUT`].
pub async fn settled_through_linearizable(
    state: &Arc<SharedState>,
    schedule: &BackupScheduleSettings,
) -> crate::Result<Option<u64>> {
    confirm_linearizable_read(
        state,
        &[nodedb_cluster::METADATA_GROUP_ID],
        Instant::now() + MARK_READ_TIMEOUT,
    )
    .await?;
    let (state, schedule) = (Arc::clone(state), schedule.clone());
    off_runtime("backup schedule mark read", move || {
        settled_through(&state, &schedule)
    })
    .await
}

/// Raise the mark of `schedule` to `through_minute` on every node.
///
/// Fails without proposing when this node is no longer the `_system`
/// coordinator. Returns once this node applied the mark.
pub async fn raise(
    state: &Arc<SharedState>,
    schedule: &BackupScheduleSettings,
    through_minute: u64,
) -> crate::Result<()> {
    ensure_system_coordinator(state)?;
    let entry = CatalogEntry::PutBackupScheduleMark(Box::new(StoredBackupScheduleMark {
        job: schedule.job_name(),
        incarnation: schedule.incarnation(),
        through_minute,
    }));
    // The proposer awaits every wait of the write on this task.
    propose_catalog_entry_async(state, &entry).await?;
    Ok(())
}
