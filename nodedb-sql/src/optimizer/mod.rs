// SPDX-License-Identifier: Apache-2.0

pub mod constant_fold;
mod pipeline;
pub mod point_get;
pub mod predicate_pushdown;

pub use pipeline::optimize;
