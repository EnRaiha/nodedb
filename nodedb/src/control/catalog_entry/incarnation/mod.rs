// SPDX-License-Identifier: BUSL-1.1

//! Incarnation fencing for delete-type catalog entries.
//!
//! The metadata log replays from its start on every boot. A delete proposed
//! against one incarnation of a name must not remove a later incarnation of
//! that name. Each fenced delete carries the `modification_hlc` of the row it
//! targeted, frozen at propose time. Apply acknowledges the delete when the
//! row has since moved past that clock.

pub mod fence;
pub mod stamp;
pub mod target;

pub use target::{Incarnation, RowKey};
