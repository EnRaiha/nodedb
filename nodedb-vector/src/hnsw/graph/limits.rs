// SPDX-License-Identifier: Apache-2.0

//! Sizing limits for the HNSW graph structure.

/// Initial arena capacity used when constructing a new [`super::index::HnswIndex`].
///
/// Sized to cover `ef_construction = 200` (the default) without needing a
/// reallocation on the first insert or search.
pub(crate) const ARENA_INITIAL_CAPACITY: usize = 256;

/// Hard cap on the layer assigned to any node during insertion.
/// Standard HNSW practice — prevents pathological RNG draws from inflating
/// `max_layer` and slowing every subsequent search.
pub const MAX_LAYER_CAP: usize = 16;
