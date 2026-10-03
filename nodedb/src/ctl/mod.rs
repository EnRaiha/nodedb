// SPDX-License-Identifier: BUSL-1.1

//! Operator subcommands for the `nodedb` binary (L.4).
//!
//! Dispatched from `main.rs` before the server startup path: if the
//! first CLI arg is a recognised subcommand name the binary runs the
//! subcommand and exits; otherwise the arg is treated as a config-file
//! path and the normal server bootstrap runs.
//!
//! Subcommands are kept small and side-effect-free where possible so
//! they can be tested without a running cluster. Rotation commands
//! that *do* need a running cluster (`rotate-ca`) talk to the local
//! node's admin HTTP surface — never the Raft transport directly.

pub mod args;
pub mod dispatch;
pub mod healthcheck;
pub mod join_token;
pub mod regen_certs;
pub mod restore;
pub mod rotate_ca;

pub use dispatch::{Subcommand, parse_subcommand, run_subcommand};
