// SPDX-License-Identifier: Apache-2.0

//! SIMD-accelerated filter kernels returning u64 bitmasks.
//!
//! Each kernel compares a column slice against a scalar and returns a
//! packed `Vec<u64>` where bit *i* is set iff the predicate holds for
//! element *i*. One u64 word covers 64 rows.
//!
//! Runtime CPU detection selects the fastest path:
//! - AVX-512 (512-bit, 16 u32 / 8 f64|i64 per op)
//! - AVX2   (256-bit,  8 u32 / 4 f64|i64 per op)
//! - Scalar fallback (auto-vectorized by LLVM)
//!
//! Companion helpers: `popcount`, `bitmask_and`, `bitmask_or`, `bitmask_to_indices`.

pub(crate) mod avx2;
pub(crate) mod avx512;
pub(crate) mod bitmask;
pub(crate) mod neon;
pub(crate) mod runtime;
pub(crate) mod scalar;
pub(crate) mod wasm;

pub use bitmask::{
    bitmask_all, bitmask_and, bitmask_not, bitmask_or, bitmask_to_indices, popcount, words_for,
};
pub use runtime::{FilterSimdRuntime, filter_runtime};
