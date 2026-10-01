// SPDX-License-Identifier: BUSL-1.1

//! PITR work run on demand, outside its schedule: one base snapshot, and one
//! WAL archive pass.

use std::sync::Arc;
use std::time::Duration;

use crate::config::server::PitrSettings;
use crate::control::state::SharedState;
use crate::storage::snapshot::SnapshotMeta;
use crate::wal::archiver::WalArchiver;

/// The time an on-demand base can take.
const ON_DEMAND_BASE_DEADLINE: Duration = Duration::from_secs(600);

/// Take one base snapshot now, with the default retention.
pub async fn take_base_now(state: &Arc<SharedState>) -> crate::Result<SnapshotMeta> {
    let life = state
        .pitr
        .life()
        .cloned()
        .ok_or_else(|| crate::Error::Config {
            detail: "a base snapshot needs PITR wired at boot; set pitr.enabled = true".into(),
        })?;
    let retention = PitrSettings::default().retention()?;
    super::task::run_once(state, &life, retention, ON_DEMAND_BASE_DEADLINE).await
}

/// Take a base once the gateway is open, because a Raft snapshot install
/// replaced rows no WAL record carries. A restore to a target at or after the
/// install starts from a base taken after it. Does nothing with PITR off.
pub fn force_base_after_install(state: &Arc<SharedState>) {
    if state.pitr.life().is_none() {
        return;
    }
    let state = Arc::clone(state);
    tokio::spawn(async move {
        let phase = crate::control::startup::StartupPhase::GatewayEnable;
        if state.startup.await_phase(phase).await.is_err() {
            return;
        }
        if let Err(error) = take_base_now(&state).await {
            tracing::warn!(
                %error,
                "the base after a Raft snapshot install failed; a restore to a later target \
                 waits for the next scheduled base"
            );
        }
    });
}

/// Seal the active WAL segment and upload every sealed segment the archive
/// lacks. Returns whether the pass uploaded everything it found sealed.
pub async fn archive_wal_now(state: &SharedState) -> crate::Result<bool> {
    let cold = state
        .cold_storage
        .clone()
        .ok_or_else(|| crate::Error::Config {
            detail: "the WAL archive needs [cold_storage]".into(),
        })?;
    state.wal.seal_active_segment()?;
    let mut archiver = WalArchiver::new(
        state.node_id,
        state.data_dir.clone(),
        cold,
        state.system_metrics.clone(),
    );
    let Some(listed) = archiver.tick(&state.wal).await else {
        return Ok(false);
    };
    let cursor = archiver.cursor();
    Ok(cursor.is_some_and(|cursor| {
        listed
            .segments
            .iter()
            .filter(|seg| seg.first_lsn < listed.active_first_lsn)
            .all(|seg| cursor.is_archived(seg.first_lsn))
    }))
}
