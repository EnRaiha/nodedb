// SPDX-License-Identifier: BUSL-1.1

//! Every Data-Plane `ErrorCode`, and each Control-Plane error a client acts
//! on, answers one class on native, pgwire and HTTP.
//!
//! pgwire renders a Data-Plane verdict as its SQLSTATE. A native client reads
//! the numeric `nodedb_types` code on the frame. The two agree when the
//! numeric code renders, through the numeric-code SQLSTATE table, in the same
//! SQLSTATE class (the first two characters) as the pgwire SQLSTATE. That same
//! table renders a code that crossed a node as a bare number, so agreement
//! also keeps a verdict's class across nodes.

mod code_parity;
mod code_samples;
mod control_plane;
mod error_index;
mod error_parity;
mod error_samples;
mod hop_parity;
mod support;

pub(crate) use error_index::error_variant_index;
pub(crate) use error_samples::error_samples;
