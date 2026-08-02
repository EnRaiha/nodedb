// SPDX-License-Identifier: Apache-2.0

//! Decode flushed segment blobs back into per-row `Value`s.
//!
//! Segments are write-once: once encoded they are only ever read, scanned, or
//! (for RESTORE) decoded back into rows and re-issued through the normal
//! durable write path. Nothing rewrites a segment in place, so there is no
//! segment-rewrite protocol here.

pub mod extract;
pub mod rows;

pub use rows::materialize_segment_live_rows;
