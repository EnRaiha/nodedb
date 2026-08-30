// SPDX-License-Identifier: Apache-2.0

mod backing;
pub mod plain;

pub use backing::VectorSegmentBacking;
pub use plain::PlainMmapBacking;
