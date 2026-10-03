// SPDX-License-Identifier: BUSL-1.1

//! The cut barriers this node applied, kept in the system catalog.
//!
//! Every entry a group's log places after a cut barrier records a commit HLC
//! above the barrier's watermark, on every replica. The apply loop tracks
//! the barriers it applies. After a restart it applies again the entries
//! above its durable applied index, and some of those follow a barrier it
//! applied before the restart. The catalog rows give those barriers back. A
//! WAL checkpoint never removes them. A barrier finishes its apply only once
//! its row is durable.

use std::time::Duration;

use tracing::{error, info, warn};

use crate::control::cluster::metadata_applier::{MetadataApplyWedge, WedgeReport, classify};

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::cut_floors::StoredBarrier;
use crate::control::state::SharedState;

/// One group's cut barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedCut {
    pub group_id: u64,
    /// Log index of the barrier.
    pub barrier_index: u64,
    /// The barrier's watermark HLC.
    pub hlc: u64,
}

/// Every cut barrier `catalog` holds.
pub fn load_recorded_cuts(catalog: &SystemCatalog) -> crate::Result<Vec<RecordedCut>> {
    Ok(catalog
        .load_cut_floors()?
        .into_iter()
        .map(|(group_id, barrier)| RecordedCut {
            group_id,
            barrier_index: barrier.index,
            hlc: barrier.watermark,
        })
        .collect())
}

/// Durably record the cut barrier group `group_id` applies at `barrier_index`
/// with watermark `hlc`.
pub fn persist_cut_floor(
    state: &SharedState,
    group_id: u64,
    barrier_index: u64,
    hlc: u64,
) -> crate::Result<()> {
    let barrier = StoredBarrier {
        index: barrier_index,
        watermark: hlc,
    };
    let durable = state.pitr.durable_applied(group_id);
    state
        .credentials
        .catalog()
        .put_cut_floor(group_id, barrier, durable)
}

/// First wait between two attempts of a floor write.
const FIRST_RETRY: Duration = Duration::from_millis(10);

/// Longest wait between two attempts of a floor write.
const LAST_RETRY: Duration = Duration::from_secs(1);

/// Entry kind a floor-write wedge report names.
const BARRIER_ENTRY_KIND: &str = "CutBarrier";

/// Run `attempt` until it succeeds, waiting longer after each failure. The
/// barrier apply awaits this: no later entry of the group starts, and the
/// barrier does not settle, until the floor is durable.
///
/// A failure `classify` names permanent wedges the node at once. A transient
/// failure wedges it once the wait between attempts reaches `LAST_RETRY`.
/// The wedge goes on `wedge`, the marker the readiness probe reads. The log
/// names the first failure, the wedge, and the recovery, never each retry.
/// A later success clears the report this barrier recorded.
pub async fn persist_until_durable(
    wedge: &MetadataApplyWedge,
    group_id: u64,
    barrier_index: u64,
    mut attempt: impl FnMut() -> crate::Result<()>,
) {
    let mut wait = FIRST_RETRY;
    let mut failures = 0u64;
    let mut wedged = false;
    // The report this barrier recorded. A report another writer holds is
    // never cleared here.
    let mut recorded: Option<WedgeReport> = None;
    loop {
        match attempt() {
            Ok(()) => {
                if let Some(report) = recorded.as_ref() {
                    wedge.clear(report);
                }
                if failures > 0 {
                    info!(
                        group_id,
                        barrier_index, failures, "cut barrier floor persisted; the group resumes"
                    );
                }
                return;
            }
            Err(e) => {
                failures += 1;
                if failures == 1 {
                    warn!(
                        group_id,
                        barrier_index,
                        error = %e,
                        "cut barrier floor not persisted; the group holds at the barrier and retries"
                    );
                }
                let stalled = classify(&e).is_permanent() || wait >= LAST_RETRY;
                if stalled && !wedged {
                    wedged = true;
                    let report = WedgeReport {
                        raft_index: barrier_index,
                        last_applied_watermark: barrier_index.saturating_sub(1),
                        entry_kind: BARRIER_ENTRY_KIND.to_owned(),
                        error: format!("group {group_id}: cut barrier floor not persisted: {e}"),
                    };
                    error!(
                        group_id,
                        barrier_index,
                        failures,
                        error = %e,
                        "cut barrier floor write keeps failing; the group is halted and this node \
                         is no longer ready"
                    );
                    if wedge.record(report.clone()) {
                        recorded = Some(report);
                    }
                }
            }
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(LAST_RETRY);
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn the_barrier_holds_until_its_floor_persists() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let wedge = MetadataApplyWedge::default();
        let barrier = StoredBarrier {
            index: 10,
            watermark: 100,
        };
        let fail = nodedb_types::fail_point::FailGuard::fail(
            "cut_floor::before_persist",
            "injected floor write error",
        );
        let persist = persist_until_durable(&wedge, 1, 10, || catalog.put_cut_floor(1, barrier, 0));
        tokio::pin!(persist);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), persist.as_mut())
                .await
                .is_err(),
            "the barrier does not finish while its floor write fails"
        );
        assert!(catalog.load_cut_floors().unwrap().is_empty());
        assert!(
            !wedge.is_wedged(),
            "a floor write that failed briefly does not wedge the node"
        );

        // The waits between attempts reach LAST_RETRY after about 1.3 s.
        assert!(
            tokio::time::timeout(Duration::from_secs(3), persist.as_mut())
                .await
                .is_err(),
            "the barrier does not finish while its floor write fails"
        );
        let report = wedge
            .report()
            .expect("a floor write that keeps failing wedges the node");
        assert_eq!(report.raft_index, 10);
        assert_eq!(report.last_applied_watermark, 9);
        assert_eq!(report.entry_kind, BARRIER_ENTRY_KIND);

        drop(fail);
        tokio::time::timeout(Duration::from_secs(5), persist)
            .await
            .expect("the barrier finishes once the floor write succeeds");
        assert_eq!(catalog.load_cut_floors().unwrap(), [(1, barrier)]);
        assert!(
            !wedge.is_wedged(),
            "the recovery clears the barrier's report"
        );
    }

    #[tokio::test]
    async fn a_recovered_floor_keeps_another_writers_report() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let wedge = MetadataApplyWedge::default();
        let applier = WedgeReport {
            raft_index: 3,
            last_applied_watermark: 2,
            entry_kind: "DdlPrepared".into(),
            error: "applier".into(),
        };
        wedge.record(applier.clone());
        let barrier = StoredBarrier {
            index: 10,
            watermark: 100,
        };
        let fail = nodedb_types::fail_point::FailGuard::fail(
            "cut_floor::before_persist",
            "injected floor write error",
        );
        let persist = persist_until_durable(&wedge, 1, 10, || catalog.put_cut_floor(1, barrier, 0));
        tokio::pin!(persist);
        assert!(
            tokio::time::timeout(Duration::from_secs(3), persist.as_mut())
                .await
                .is_err()
        );
        drop(fail);
        tokio::time::timeout(Duration::from_secs(5), persist)
            .await
            .expect("the barrier finishes once the floor write succeeds");
        assert_eq!(wedge.report(), Some(applier));
    }
}
