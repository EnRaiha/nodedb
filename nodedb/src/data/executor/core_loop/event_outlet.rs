// SPDX-License-Identifier: BUSL-1.1

//! Where a core's write events leave for the Event Plane.

use std::sync::Arc;

use crate::event::bus::EventProducer;
use crate::event::interest::EventInterest;

/// The event bus side of a core.
pub(in crate::data::executor) struct EventOutlet {
    /// Emits WriteEvents to the Event Plane. One per core, `!Send` once
    /// pinned. `None` when the Event Plane is disabled.
    pub(in crate::data::executor) producer: Option<EventProducer>,
    /// The sequence of the last event this core emitted.
    pub(in crate::data::executor) sequence: u64,
    /// The collections whose write events some Event Plane consumer reads.
    /// An engine that emits events on demand (a timeseries ingest) emits
    /// none for a collection outside it. Read lock-free. Holds no collection
    /// until boot wires the shared set via `set_event_interest`.
    pub(in crate::data::executor) interest: Arc<EventInterest>,
}

impl EventOutlet {
    /// An outlet with no producer and an empty interest set.
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            producer: None,
            sequence: 0,
            interest: EventInterest::new(),
        }
    }
}
