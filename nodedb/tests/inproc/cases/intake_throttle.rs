// SPDX-License-Identifier: BUSL-1.1

//! Data Plane request-intake throttle: the level a core settles on, and the
//! read depth and suspend decision that follow from it.

#[path = "intake_throttle/helpers.rs"]
mod helpers;
#[path = "intake_throttle/memory_pressure.rs"]
mod memory_pressure;
#[path = "intake_throttle/response_ring.rs"]
mod response_ring;
