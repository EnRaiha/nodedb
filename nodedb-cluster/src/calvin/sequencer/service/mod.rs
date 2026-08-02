// SPDX-License-Identifier: BUSL-1.1

pub mod core;
pub mod epoch_seed;
pub mod reservations;

// `self::` is required: a bare `core` in a `use` path resolves to the `core`
// crate, not this module's sibling.
pub use self::core::{RESERVATION_POSITION_BAND, SequencerReceivers, SequencerService};
// Re-exported so existing call sites (`service::SequencerMetrics`) don't break.
pub use crate::calvin::sequencer::metrics::{ConflictKey, SequencerMetrics};
