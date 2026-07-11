// SPDX-License-Identifier: BUSL-1.1

//! Concrete `ArrayLocalExecutor` implementation for the shard-side array handler.
//!
//! `DataPlaneArrayExecutor` implements the `ArrayLocalExecutor` trait defined in
//! `nodedb-cluster`. It bridges incoming distributed array RPC requests into the
//! local Data Plane via the SPSC bridge, awaits the response, and converts the
//! Data Plane response format into the zerompk-encoded shapes the cluster handler
//! expects.
//!
//! # Slice rows
//! The Data Plane encodes slice results as a flat msgpack array (one element per
//! row). This executor parses the array header and uses `skip_value` to extract
//! per-row byte slices, returning them as `Vec<Vec<u8>>`.
//!
//! # Surrogate bitmap scan
//! The Data Plane encodes surrogate scan results as a msgpack array of
//! `{"id": "<hex_surrogate>", "data": <empty_map>}` document rows. This executor
//! collects the hex surrogate strings, builds a `SurrogateBitmap`, and
//! zerompk-serializes it as the response.
//!
//! # Module layout
//! The single `ArrayLocalExecutor` trait impl (Rust requires it in one block)
//! lives in [`trait_impl`] and delegates each method to a concern-split inherent
//! method:
//! - [`executor`]: the `DataPlaneArrayExecutor` type + shared SPSC
//!   dispatch-and-await scaffolding.
//! - [`read`]: read/scan handlers (slice, aggregate, surrogate-bitmap scan) plus
//!   the response-row parsers.
//! - [`write`]: write handlers (put, delete).

mod executor;
mod read;
mod trait_impl;
mod write;

pub use executor::DataPlaneArrayExecutor;
