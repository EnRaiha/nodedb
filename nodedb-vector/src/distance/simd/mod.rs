// SPDX-License-Identifier: Apache-2.0

//! Runtime SIMD dispatch for vector distance and bitmap operations.

pub mod bbq;
pub mod hamming;
pub mod runtime;
pub mod scalar;

// The x86 tier modules are crate-private on purpose. Their entry points are
// safe `pub fn`s that call `#[target_feature]` implementations, so exposing the
// module would let safe code outside the crate execute AVX2 or AVX-512
// instructions on a host that does not have them. `SimdRuntime::detect()` is the
// only supported way to reach a tier, and it selects one under a runtime feature
// probe. NEON is baseline on aarch64 and wasm simd128 is a compile-time gate, so
// those two stay public.
#[cfg(target_arch = "x86_64")]
pub(crate) mod avx2;
#[cfg(target_arch = "x86_64")]
pub(crate) mod avx512;
#[cfg(target_arch = "aarch64")]
pub mod neon;
#[cfg(target_arch = "wasm32")]
pub mod wasm_simd128;

pub use runtime::{SimdRuntime, runtime};
