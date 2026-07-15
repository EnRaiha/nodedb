// SPDX-License-Identifier: BUSL-1.1

//! Leader-side reservation RPC hooks (reserve-read admission, release on
//! commit/abort): bridge the cluster `ReserveRead` / `ReleaseReservation`
//! triggers to the node-local reservation primitives in
//! `crate::control::planner::calvin::reservation`.

pub mod hooks;

pub use hooks::{RegistryReleaseReservation, RegistryReserveRead};
