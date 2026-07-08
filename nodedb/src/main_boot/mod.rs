// SPDX-License-Identifier: BUSL-1.1

//! Binary-only boot-phase modules for `main()`. Each submodule is a pure
//! relocation of one cohesive chunk of the original monolithic `main()`
//! body — no behavior change, just named phases so `main.rs` reads as a
//! thin sequence of steps instead of one 500+ line function.

pub(crate) mod background;
pub(crate) mod data_plane;
pub(crate) mod gates;
pub(crate) mod listeners;
pub(crate) mod post_open;
pub(crate) mod shared_state;
pub(crate) mod shutdown_wiring;
pub(crate) mod startup_log;
