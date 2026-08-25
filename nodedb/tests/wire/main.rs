// SPDX-License-Identifier: BUSL-1.1

//! Grouped test target for pgwire-over-subprocess integration tests.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file. The harness (`harness/`) spawns the real `nodedb` binary as a
//! subprocess instead of linking the server library in-process, so this
//! target's dependency closure stays small no matter how many case files it
//! grows to hold.

mod cases;
mod harness;
