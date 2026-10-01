// SPDX-License-Identifier: BUSL-1.1

//! The applied index a data-group snapshot is cut at.
//!
//! The builder holds the group's apply gate exclusive, so no entry of the
//! group starts during the capture. Every entry that started before the fence
//! finishes on its core and settles in log order. The cut is the highest
//! started entry, or the applied index the group restored at boot when no
//! entry started since. Once the group settled through the cut, the Data
//! Plane holds exactly the entries at or below it, and so do the write marks
//! and the committed proposal keys.
//!
//! The cut is at or above the Raft snapshot index the snapshot is sent at.
//! The follower that installs it concludes the entries between the two
//! without applying them (see
//! [`crate::control::distributed_applier::propose_tracker::ProposeTracker::cover_through`]).

use std::time::Duration;

use nodedb_cluster::WaitOutcome;

use crate::Error;
use crate::control::state::SharedState;

/// How long the fenced build waits for the group to settle through the cut.
/// A write its core parks holds the group below the cut. The build then fails
/// and the next heartbeat retries it, so the fence never holds the group's
/// apply for longer than this.
const CUT_SETTLE_TIMEOUT: Duration = Duration::from_secs(5);

/// The index to cut at: the highest started entry, or the applied index when
/// it is higher.
fn cut_of(started: u64, applied: u64) -> u64 {
    started.max(applied)
}

/// Settle `group_id` through its cut and return the cut. The caller holds the
/// group's apply gate exclusive.
///
/// Fails when the group does not settle within [`CUT_SETTLE_TIMEOUT`], or when
/// the cut is below `last_included_index`: the Raft log dropped entries this
/// node never applied, and no capture here holds them.
pub(crate) async fn settled_cut(
    shared: &SharedState,
    group_id: u64,
    last_included_index: u64,
) -> Result<u64, Error> {
    let Some(tracker) = shared.propose_tracker.get() else {
        // No apply loop runs, so nothing applies above the Raft index.
        return Ok(last_included_index);
    };
    let watcher = shared.applied_index_watcher(group_id);
    let cut = cut_of(tracker.started_through(group_id), watcher.current());
    if watcher.current() < cut {
        let waiting = std::sync::Arc::clone(&watcher);
        let outcome =
            tokio::task::spawn_blocking(move || waiting.wait_for(cut, CUT_SETTLE_TIMEOUT))
                .await
                .map_err(|e| Error::Internal {
                    detail: format!(
                        "snapshot build: group {group_id}: the settle wait for cut {cut} \
                         did not finish: {e}"
                    ),
                })?;
        match outcome {
            WaitOutcome::Reached => {}
            WaitOutcome::TimedOut | WaitOutcome::GroupGone => {
                return Err(Error::Internal {
                    detail: format!(
                        "snapshot build: group {group_id} settled through {} of the entries \
                         it started through {cut}; the build retries on the next heartbeat",
                        watcher.current()
                    ),
                });
            }
        }
    }
    if cut < last_included_index {
        return Err(Error::Internal {
            detail: format!(
                "snapshot build: group {group_id} applied through {cut}, below the Raft \
                 snapshot index {last_included_index}; this node holds no state to send"
            ),
        });
    }
    Ok(cut)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cut_is_the_higher_of_started_and_applied() {
        assert_eq!(cut_of(12, 9), 12, "entries in flight raise the cut");
        assert_eq!(
            cut_of(0, 40),
            40,
            "a restored group cuts at its applied index"
        );
        assert_eq!(cut_of(40, 40), 40);
    }
}
