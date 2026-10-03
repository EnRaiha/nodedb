// SPDX-License-Identifier: BUSL-1.1

//! The trigger action lane's state on one node.

use std::path::Path;

use tokio::sync::Notify;

use super::delivered::DeliveredEvents;
use super::ledger::ActionLedger;
use super::position::ActionPositions;

/// The lane's durable ledger, its position allocator, how far each consumer
/// delivered, and the signal that wakes its firing task when an event is
/// held.
pub struct ActionLane {
    pub ledger: ActionLedger,
    pub positions: ActionPositions,
    pub delivered: DeliveredEvents,
    pub wake: Notify,
}

impl ActionLane {
    /// Open the lane's ledger under `dir`, for `num_cores` consumers.
    pub fn open(dir: &Path, num_cores: usize) -> crate::Result<Self> {
        Ok(Self {
            ledger: ActionLedger::open(dir)?,
            positions: ActionPositions::new(),
            delivered: DeliveredEvents::new(num_cores),
            wake: Notify::new(),
        })
    }
}
