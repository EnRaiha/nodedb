// SPDX-License-Identifier: BUSL-1.1

//! Positions derived from the replicated routing table: a data-group
//! entry's position with its partition epoch, and the partitions a snapshot
//! install leaves without events.

use crate::control::state::SharedState;
use crate::event::cdc::offset::CdcOffset;

use super::marker::ReplicatedPosition;

/// The position of the entry at `log_index` of `group_id`, applied for
/// `vshard`. The epoch is the one at which the vShard moved to `group_id`,
/// which the replicated routing table holds identically on every node.
pub fn entry_position(
    state: &SharedState,
    vshard: u32,
    group_id: u64,
    log_index: u64,
) -> ReplicatedPosition {
    let epoch = state.cluster_routing.as_ref().map_or(0, |routing| {
        routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .vshard_epoch(vshard, group_id)
    });
    ReplicatedPosition {
        epoch,
        group_id,
        log_index,
    }
}

/// Record that this node holds no change events for the entries through
/// `last_included_index` of `group_id`, which a snapshot install applied
/// without replaying them. Each partition of the group then serves only from
/// the next entry. The floors are persisted with the CDC ledger when it is
/// open, and boot persists any raised before it opened.
pub fn record_install_floor(state: &SharedState, group_id: u64, last_included_index: u64) {
    if last_included_index == 0 {
        return;
    }
    let Some(routing) = state.cluster_routing.as_ref() else {
        return;
    };
    let floors: Vec<(u32, CdcOffset)> = {
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        routing
            .vshards_for_group(group_id)
            .into_iter()
            .map(|vshard| {
                let epoch = routing.vshard_epoch(vshard, group_id);
                (vshard, CdcOffset::at(epoch, last_included_index + 1, 0))
            })
            .collect()
    };
    for (partition, floor) in &floors {
        state.cdc_router.availability().raise(*partition, *floor);
    }
    if let Some(ledgers) = state.sink_ledgers.get()
        && let Err(error) = ledgers.cdc.persist_floors(&floors)
    {
        tracing::error!(
            group_id,
            last_included_index,
            %error,
            "change-event availability floors after a snapshot install did not persist; \
             after a restart this node could serve a cursor below them"
        );
    }
}
