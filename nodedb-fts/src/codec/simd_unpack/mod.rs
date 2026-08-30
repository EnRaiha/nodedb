// SPDX-License-Identifier: Apache-2.0

//! SIMD-accelerated bitpack unpacking with runtime dispatch.
//!
//! Dispatch order:
//! - x86_64: SSE2 (runtime detected) → scalar
//! - aarch64: NEON (compile-time baseline) → scalar
//! - wasm32/other: scalar

mod scalar;

#[cfg(target_arch = "x86_64")]
mod sse2;

#[cfg(target_arch = "aarch64")]
mod neon;

mod unpack;

pub use unpack::{unpack, unpack_simd};
