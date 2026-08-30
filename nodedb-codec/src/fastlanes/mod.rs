// SPDX-License-Identifier: Apache-2.0

//! FastLanes-inspired FOR + bit-packing codec for integer columns.
//!
//! Frame-of-Reference (FOR): subtract the minimum value from all values,
//! reducing them to small unsigned residuals. Then bit-pack the residuals
//! using the minimum number of bits.
//!
//! The bit-packing loop is written as simple scalar operations on contiguous
//! arrays, which LLVM auto-vectorizes to AVX2/AVX-512/NEON/WASM-SIMD without
//! explicit intrinsics. This is the FastLanes insight: structured scalar code
//! that the compiler vectorizes, portable across all targets.
//!
//! Wire format:
//! ```text
//! [4 bytes] total value count (LE u32)
//! [2 bytes] block count (LE u16)
//! For each block:
//!   [2 bytes] values in this block (LE u16, max 1024)
//!   [1 byte]  bit width (0-64)
//!   [8 bytes] min value / reference (LE i64)
//!   [N bytes] bit-packed residuals
//! ```
//!
//! Block size: 1024 values. Last block may be smaller.

mod bits;
mod block;
mod codec;
mod header;
mod iterator;
mod range;

pub use block::bit_width_for_range;
pub use codec::{decode, encode};
pub use iterator::BlockIterator;
pub use range::{block_byte_offsets, block_count, decode_block_range, decode_single_block};
