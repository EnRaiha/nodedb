// SPDX-License-Identifier: Apache-2.0

//! Distance metrics for vector similarity search.

mod compute;
pub mod dispatch;
pub mod scalar;
pub mod simd;
pub(crate) mod typed_scalar;

pub use compute::{batch_distances, distance};
pub use scalar::*;
