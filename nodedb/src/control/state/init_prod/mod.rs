// SPDX-License-Identifier: BUSL-1.1

//! SharedState::open — production constructor loading from disk.

mod bootstrap;
mod handles;
mod open;
mod post_init;

pub use handles::DataPlaneHandles;
