// SPDX-License-Identifier: BUSL-1.1

//! Native protocol session implementation.

mod auth;
mod request;
mod run;
mod session_chunk;
mod session_stream;
mod state;

#[cfg(test)]
mod tests;

pub(crate) use self::state::NativeConnectionResources;
pub use self::state::NativeSession;
pub(super) use super::{codec, dispatch};
