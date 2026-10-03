// SPDX-License-Identifier: BUSL-1.1

//! The metadata leader's side of the authorization lease.
//!
//! Only the current metadata leader answers renewals and barriers. Each
//! leadership term starts a fresh [`LeaseTable`], whose floors are loaded
//! before anything is granted or released:
//!
//! - **Metadata group:** the leader's own confirmed read index.
//! - **Sequencer group and every group homing a tree source:** a read index
//!   from each group's leader.
//!
//! A read index is at or above every entry committed before it was taken, so
//! the floors cover every change acknowledged in an earlier term.
//!
//! A grant and a release are answered only after the leader confirms its
//! leadership against a quorum, taken after the decision. A leader deposed
//! meanwhile answers `NotLeader`, so no lease it grants and no barrier it
//! releases outlives its term unseen.
//!
//! Each reply has a deadline: the leader budget of [`LeaseTiming`] after the
//! request arrived, or after a barrier's own wait. It ends a reply margin
//! before the holder's read timeout, so the reply arrives before the holder
//! gives up.
//!
//! A leader that is the only voter of the metadata group pins its own lease
//! (see [`super::table`]). No other node can lead the group then, so every
//! barrier releases here, and each one waits for this node's coverage.
//! Planning on this node then needs no lease that expires on the clock.

use std::collections::HashSet;
use std::sync::{Mutex, Weak};
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::{
    AuthBarrierOutcome, AuthBarrierRequest, AuthBarrierResponse, AuthLeaseRenewOutcome,
    AuthLeaseRenewRequest, AuthLeaseRenewResponse, GroupCoverage, METADATA_GROUP_ID,
};
use tokio::sync::Notify;

use crate::control::security::auth_fence::cluster::{
    confirmed_read_index, group_of_vshard, wait_applied,
};
use crate::control::security::auth_fence::view::apply_committed_tree_defs;
use crate::control::state::SharedState;

use super::leadership::{leader_hint, leading_term, sole_voter_term};
use super::table::{BarrierState, LeaseTable, RenewDecision};
use super::timing::LeaseTiming;
use super::withheld_warn::WithheldWarnings;

/// Answers lease renewals and barriers while this node leads the metadata
/// group.
pub struct LeaderLeaseService {
    /// Held weakly: the service lives on `SharedState`.
    state: Weak<SharedState>,
    timing: LeaseTiming,
    table: Mutex<Option<LeaseTable>>,
    /// Woken on every renewal and floor load, for waiting barriers.
    changed: Notify,
    /// One floor load at a time.
    floors_loading: tokio::sync::Mutex<()>,
    /// Rate limit of the withheld-renewal warning.
    withheld_warnings: WithheldWarnings,
}

impl std::fmt::Debug for LeaderLeaseService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderLeaseService")
            .field("timing", &self.timing)
            .finish_non_exhaustive()
    }
}

impl LeaderLeaseService {
    pub fn new(state: Weak<SharedState>, timing: LeaseTiming) -> Self {
        Self {
            state,
            timing,
            table: Mutex::new(None),
            changed: Notify::new(),
            floors_loading: tokio::sync::Mutex::new(()),
            withheld_warnings: WithheldWarnings::default(),
        }
    }

    fn table(&self) -> std::sync::MutexGuard<'_, Option<LeaseTable>> {
        self.table.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Whether the table of `term` pins the lease of `node_id`.
    ///
    /// The caller passes the term in which it read that this node is the
    /// only voter of the metadata group.
    pub fn holds_pinned_lease(&self, term: u64, node_id: u64) -> bool {
        self.table()
            .as_ref()
            .is_some_and(|table| table.term() == term && table.is_pinned(node_id))
    }

    /// Run `edit` on the current table, creating one for `term` first.
    #[cfg(test)]
    pub(crate) fn edit_table(&self, term: u64, now: Instant, edit: impl FnOnce(&mut LeaseTable)) {
        let mut table = self.table();
        let table = table.get_or_insert_with(|| LeaseTable::new(term, now));
        edit(table);
    }

    /// Make the table of `term` current and load its floors by `reply_by`.
    async fn table_ready(
        &self,
        state: &SharedState,
        term: u64,
        reply_by: Instant,
    ) -> crate::Result<()> {
        {
            let mut table = self.table();
            if table.as_ref().is_none_or(|t| t.term() != term) {
                *table = Some(LeaseTable::new(term, Instant::now()));
            }
            if table.as_ref().is_some_and(LeaseTable::floors_ready) {
                return Ok(());
            }
        }
        let _loading = self.floors_loading.lock().await;
        if self
            .table()
            .as_ref()
            .is_some_and(|t| t.term() == term && t.floors_ready())
        {
            return Ok(());
        }
        let floors = self.load_floors(state, reply_by).await?;
        if let Some(table) = self.table().as_mut().filter(|t| t.term() == term) {
            table.load_floors(&floors);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    /// The floors a new term starts from, loaded by `reply_by`.
    async fn load_floors(
        &self,
        state: &SharedState,
        reply_by: Instant,
    ) -> crate::Result<Vec<GroupCoverage>> {
        let metadata = confirmed_read_index(state, METADATA_GROUP_ID, time_left(reply_by)).await?;
        // The source set comes from tree definitions, which live in the
        // metadata group. Apply it through the read index first.
        wait_applied(state, METADATA_GROUP_ID, metadata, time_left(reply_by)).await?;
        apply_committed_tree_defs(state).await;

        let mut groups: HashSet<u64> = HashSet::new();
        for vshard_id in state.authorization_fence.sources().source_vshards() {
            groups.insert(group_of_vshard(state, vshard_id)?);
        }
        groups.insert(SEQUENCER_GROUP_ID);
        let timeout = time_left(reply_by);
        let group_floors = try_join_all(groups.into_iter().map(|group_id| async move {
            confirmed_read_index(state, group_id, timeout)
                .await
                .map(|through| GroupCoverage { group_id, through })
        }))
        .await?;

        let mut floors = vec![GroupCoverage {
            group_id: METADATA_GROUP_ID,
            through: metadata,
        }];
        floors.extend(group_floors);
        Ok(floors)
    }

    /// Whether this node still leads the metadata group in `term`, confirmed
    /// against a quorum by `reply_by`.
    async fn confirm_leadership(&self, state: &SharedState, term: u64, reply_by: Instant) -> bool {
        confirmed_read_index(state, METADATA_GROUP_ID, time_left(reply_by))
            .await
            .is_ok()
            && leading_term(state) == Some(term)
    }

    fn not_leader_renewal(state: Option<&SharedState>) -> AuthLeaseRenewResponse {
        let (leader_hint, term) = state.map_or((None, 0), leader_hint);
        AuthLeaseRenewResponse {
            outcome: AuthLeaseRenewOutcome::NotLeader { leader_hint, term },
        }
    }

    fn not_leader_barrier(state: Option<&SharedState>) -> AuthBarrierResponse {
        let (leader_hint, term) = state.map_or((None, 0), leader_hint);
        AuthBarrierResponse {
            outcome: AuthBarrierOutcome::NotLeader { leader_hint, term },
        }
    }

    /// Grant or withhold the lease of the node that sent `req`.
    pub async fn renew_lease(&self, req: AuthLeaseRenewRequest) -> AuthLeaseRenewResponse {
        let Some(state) = self.state.upgrade() else {
            return Self::not_leader_renewal(None);
        };
        let Some(term) = leading_term(&state) else {
            return Self::not_leader_renewal(Some(&state));
        };
        let reply_by = Instant::now() + self.timing.leader_budget();
        if let Err(error) = self.table_ready(&state, term, reply_by).await {
            if self
                .withheld_warnings
                .should_warn(req.node_id, Instant::now())
            {
                tracing::warn!(
                    node_id = req.node_id,
                    %error,
                    "authorization lease: renewal withheld: the floors of this term are not loaded"
                );
            }
            return AuthLeaseRenewResponse {
                outcome: AuthLeaseRenewOutcome::Withheld,
            };
        }
        let sole_voter = sole_voter_term(&state) == Some(term);
        let (decision, shortfall) = {
            let mut table = self.table();
            match table.as_mut().filter(|t| t.term() == term) {
                Some(table) => {
                    table.observe_sole_voter(sole_voter);
                    let decision = table.renew(
                        req.node_id,
                        &req.coverage,
                        Instant::now(),
                        self.timing.lease,
                    );
                    if decision == RenewDecision::Granted
                        && sole_voter
                        && req.node_id == state.node_id
                    {
                        table.pin(req.node_id);
                    }
                    let shortfall = match decision {
                        RenewDecision::Withheld => table.shortfall(&req.coverage),
                        RenewDecision::Granted => Vec::new(),
                    };
                    (decision, shortfall)
                }
                None => return Self::not_leader_renewal(Some(&state)),
            }
        };
        if decision == RenewDecision::Withheld
            && self
                .withheld_warnings
                .should_warn(req.node_id, Instant::now())
        {
            // Each entry: (group_id, floor, reported coverage or None).
            tracing::warn!(
                node_id = req.node_id,
                term,
                short_groups = ?shortfall,
                "authorization lease: renewal withheld: the node's coverage is below the floor \
                 of each group listed as (group_id, floor, reported)"
            );
        }
        self.changed.notify_waiters();
        let outcome = match decision {
            RenewDecision::Withheld => AuthLeaseRenewOutcome::Withheld,
            RenewDecision::Granted if self.confirm_leadership(&state, term, reply_by).await => {
                AuthLeaseRenewOutcome::Granted {
                    lease_ms: u64::try_from(self.timing.lease.as_millis()).unwrap_or(u64::MAX),
                }
            }
            RenewDecision::Granted => {
                return Self::not_leader_renewal(Some(&state));
            }
        };
        AuthLeaseRenewResponse { outcome }
    }

    /// Answer once no lease holder can plan against state older than the
    /// request's targets.
    pub async fn hold_barrier(&self, req: AuthBarrierRequest) -> AuthBarrierResponse {
        let started = Instant::now();
        let deadline = started + Duration::from_millis(req.timeout_ms);
        let reply_by = deadline + self.timing.leader_budget();
        let timed_out = || AuthBarrierResponse {
            outcome: AuthBarrierOutcome::Timeout {
                waited_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            },
        };
        loop {
            let Some(state) = self.state.upgrade() else {
                return Self::not_leader_barrier(None);
            };
            let Some(term) = leading_term(&state) else {
                return Self::not_leader_barrier(Some(&state));
            };
            if let Err(error) = self.table_ready(&state, term, reply_by).await {
                tracing::debug!(%error, "authorization barrier: floors not loaded yet");
                if Instant::now() >= deadline {
                    return timed_out();
                }
                tokio::time::sleep(self.timing.renew_every).await;
                continue;
            }
            let sole_voter = sole_voter_term(&state) == Some(term);
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let status = {
                let mut table = self.table();
                match table.as_mut().filter(|t| t.term() == term) {
                    Some(table) => {
                        table.observe_sole_voter(sole_voter);
                        table.raise_floors(&req.targets);
                        table.barrier(&req.targets, Instant::now(), self.timing.lease)
                    }
                    None => continue,
                }
            };
            match status {
                BarrierState::Released => {
                    return if self.confirm_leadership(&state, term, reply_by).await {
                        AuthBarrierResponse {
                            outcome: AuthBarrierOutcome::Released,
                        }
                    } else {
                        Self::not_leader_barrier(Some(&state))
                    };
                }
                BarrierState::NotReady => tokio::time::sleep(self.timing.renew_every).await,
                BarrierState::Waiting { until } => {
                    // A pinned holder releases the barrier by renewing, which
                    // wakes `notified`. A check every renewal interval also
                    // sees the pin end when a second voter joins.
                    let until = until.unwrap_or_else(|| Instant::now() + self.timing.renew_every);
                    let wake = tokio::time::Instant::from_std(until.min(deadline));
                    drop(state);
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(wake) => {}
                    }
                }
            }
            if Instant::now() >= deadline {
                return timed_out();
            }
        }
    }
}

/// Time left until `reply_by`, zero once it passed.
fn time_left(reply_by: Instant) -> Duration {
    reply_by.saturating_duration_since(Instant::now())
}

#[async_trait::async_trait]
impl nodedb_cluster::AuthLeaseService for LeaderLeaseService {
    async fn renew(&self, req: AuthLeaseRenewRequest) -> AuthLeaseRenewResponse {
        self.renew_lease(req).await
    }

    async fn barrier(&self, req: AuthBarrierRequest) -> AuthBarrierResponse {
        self.hold_barrier(req).await
    }
}
