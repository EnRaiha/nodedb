// SPDX-License-Identifier: BUSL-1.1

//! Central shutdown coordination: one watch, one registry, one
//! spawn helper. Replaces the ad-hoc `_shutdown_senders: Vec<_>`
//! graveyard that carried no join handles, no laggard detection,
//! and no per-loop names.
//!
//! Every long-running background loop in the Control Plane and
//! Event Plane MUST spawn via [`spawn_loop`] / [`spawn_blocking_loop`]
//! and subscribe to the canonical [`ShutdownWatch`] held on
//! `SharedState`. On `main.rs`'s ctrl-c path, the watch is
//! signaled and [`LoopRegistry::shutdown_all_strict`] awaits every
//! registered handle. The bounded [`LoopRegistry::shutdown_all`] API
//! remains available for noncritical callers, aborting async laggards
//! and logging blocking laggards.

pub mod bus;
pub mod phase;
pub mod receiver;
pub mod registry;
pub mod report;
pub mod spawn;
pub mod watch;

pub use bus::{DrainGuard, PHASE_BUDGET, ShutdownBus, ShutdownHandle, TaskId, spawn_drainable};
pub use phase::ShutdownPhase;
pub use receiver::ShutdownReceiver;
pub use registry::{LoopHandle, LoopRegistry, RegistryClosed};
pub use report::{LaggardReport, ShutdownReport};
pub use spawn::{spawn_blocking_loop, spawn_loop, spawn_loop_no_abort};
pub use watch::ShutdownWatch;
