// SPDX-License-Identifier: Apache-2.0

//! Post-scan filter evaluation.
//!
//! `ScanFilter` represents a single filter predicate.
//!
//! Shared between Origin (Control Plane + Data Plane) and Lite.

pub mod like;
pub mod op;
pub mod parse;
pub mod timestamp;
pub mod types;

pub use like::sql_like_match;
pub use op::FilterOp;
pub use parse::parse_simple_predicates;
pub use timestamp::value_as_timestamp_ms;
pub use types::ScanFilter;
