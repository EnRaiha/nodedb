// SPDX-License-Identifier: BUSL-1.1

//! Trigger dispatcher: bridges Event Plane events to Control Plane trigger fire.

pub mod batch;
mod enqueue;
pub mod identity;
pub mod retry_action;
pub mod single;

pub use batch::dispatch_trigger_batch;
pub use retry_action::retry_action;
pub use single::dispatch_triggers;
