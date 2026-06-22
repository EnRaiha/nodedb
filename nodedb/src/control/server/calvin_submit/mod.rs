// SPDX-License-Identifier: BUSL-1.1

//! Routed Calvin-submit (Cv1): the sequencer-leader-side cluster hook that
//! submits a forwarded `TxClass` to the local Calvin sequencer inbox and awaits
//! its completion. The coordinator-side routing helper lives in
//! `crate::control::planner::calvin::submit::submit_calvin_routed`.

pub mod hook;
pub mod inbox_hook;

pub use hook::RegistryCalvinSubmit;
pub use inbox_hook::RegistryCalvinSubmitInbox;
