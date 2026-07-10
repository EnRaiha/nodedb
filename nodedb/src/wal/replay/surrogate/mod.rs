// SPDX-License-Identifier: BUSL-1.1

pub mod alloc;
pub mod bind;
pub mod dispatch;

pub use alloc::apply_surrogate_alloc;
pub use bind::apply_surrogate_bind;
pub use dispatch::{ReplayStats, replay_surrogate_records};
