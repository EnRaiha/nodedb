// SPDX-License-Identifier: BUSL-1.1

//! Holding firing events until their partition's owner fires their actions.
//!
//! Every replica holds every firing event, whatever its catalog holds. What
//! a replica's catalog holds depends on when it applied a trigger's DDL,
//! which differs per replica. The log alone decides what is held, so a new
//! owner never lacks an event whose trigger an earlier owner's catalog had.
//! The owner decides at firing whether the event has actions.
//!
//! The cost is bounded per write: one ledger row per firing event, written in
//! one ledger commit per consumer batch, and released once the cursor passes
//! it. The owner commits the cursor over events with no action at a bounded
//! rate (`fire`), so a partition with no triggers costs one cursor commit per
//! interval, not one per write.

use std::sync::Arc;
use std::time::Duration;

use tracing::error;

use crate::control::state::SharedState;
use crate::event::cdc::{CdcOffset, CdcRouter};
use crate::event::types::{EventSource, WriteEvent};

use super::held::HeldAction;
use super::store::ActionLane;

/// Longest pause between attempts to hold events the ledger refused.
const HOLD_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// One event to hold: its partition, position and held form.
pub type HeldRow = (u32, CdcOffset, HeldAction);

/// The lane of this node. `None` until the Event Plane opens its sink
/// ledgers.
pub(super) fn lane(state: &SharedState) -> Option<&ActionLane> {
    state.sink_ledgers.get().map(|ledgers| &ledgers.actions)
}

/// Whether `event`'s source fires AFTER triggers and DEFINE EVENT actions.
/// A trigger's own writes, restored rows and CRDT merges fire nothing.
fn fires_actions(event: &WriteEvent) -> bool {
    match event.source {
        EventSource::User | EventSource::ImplicitClient | EventSource::Deferred => true,
        EventSource::Trigger
        | EventSource::RaftFollower
        | EventSource::CrdtSync
        | EventSource::Restore => false,
    }
}

/// The row that holds `event`, a row write, with its position. `None` for an
/// event whose source fires nothing, or before the lane opens.
///
/// Every replica runs this for every row write it applies, in apply order,
/// so every replica numbers a partition alike.
pub fn action_row(
    event: &WriteEvent,
    state: &Arc<SharedState>,
    router: &CdcRouter,
) -> Option<HeldRow> {
    if !fires_actions(event) {
        return None;
    }
    let Some(lane) = lane(state) else {
        error!(
            collection = %event.collection,
            "the trigger action ledger is not open; an event's actions cannot be held"
        );
        return None;
    };
    let (partition, position) = lane.positions.next(event, router, |partition| {
        lane.ledger.tail(partition).ok().flatten()
    });
    let held = HeldAction::of(event)?;
    Some((partition, position, held))
}

/// Hold `rows` in one ledger commit, and wake the firing task. Returns once
/// they are durable, so the Event Plane's watermark never passes an event
/// this node does not hold. A commit the disk refuses is retried until it
/// lands.
pub async fn hold_rows(state: &Arc<SharedState>, rows: &[HeldRow]) {
    if rows.is_empty() {
        return;
    }
    let Some(lane) = lane(state) else {
        return;
    };
    let mut backoff = Duration::from_millis(10);
    while let Err(error) = lane.ledger.hold_many(rows) {
        error!(
            events = rows.len(),
            error = %error,
            "trigger actions could not be held; retrying"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(HOLD_BACKOFF_MAX);
    }
    lane.wake.notify_one();
}
