// SPDX-License-Identifier: Apache-2.0

//! Decode flushed segment blobs back into per-row `Value`s.
//!
//! On Origin, segments are write-once: once encoded they are only ever read,
//! scanned, or (for RESTORE) decoded back into rows and re-issued through the
//! normal durable write path. The per-row decode lives here; the in-place
//! rewrite embedded deployments use is in [`crate::compaction`].

pub mod extract;
pub mod rows;

pub use rows::materialize_segment_live_rows;
