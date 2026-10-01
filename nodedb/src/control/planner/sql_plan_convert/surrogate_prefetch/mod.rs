// SPDX-License-Identifier: BUSL-1.1

//! Resolve a batch of plans' surrogate keys, async, around the synchronous
//! conversion that reads the answers.

pub mod bind;
pub mod cache;
pub mod resolve;

pub use bind::convert_bound;
pub use cache::PrefetchedSurrogates;
