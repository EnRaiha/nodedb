// SPDX-License-Identifier: Apache-2.0

/// Storage abstraction for HNSW vector data.
///
/// Two implementations coexist:
/// - [`crate::segment_backing::PlainMmapBacking`]: zero-copy mmap of a plaintext NDVS file (Origin).
/// - `PagedbBacking`: encrypted segment read via pagedb (Lite, task 2a.2).
///
/// Implementations are responsible for vector retrieval; HNSW graph traversal
/// makes no storage-level decisions.
///
/// # `Send + Sync` bound
///
/// The bound allows consumers to park the backing in an
/// `Arc<dyn VectorSegmentBacking>` and dispatch across tasks.  Lite will
/// require this in task 2a.2.
///
/// # Return type for `get_vector`
///
/// `-> Option<&[f32]>` borrows from `&self`.  This is correct for
/// `PlainMmapBacking` (slice into mmap region lives as long as the backing)
/// and for the planned `PagedbBacking` (which will hold a long-lived
/// decrypted vector slab in a `MmapView` field on `self`).
///
/// If a future backing genuinely cannot return a `&self`-lifetime slice
/// without copying, the signature can be changed to `Cow<'_, [f32]>` in a
/// follow-up refactor.  For now both known impls support the zero-copy path.
pub trait VectorSegmentBacking: Send + Sync {
    /// Number of vectors stored in this segment.
    fn len(&self) -> usize;

    /// Returns `true` when the segment contains no vectors.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Dimensionality of each stored vector.
    fn dim(&self) -> usize;

    /// Fetch one vector by local position id (0..len).
    ///
    /// Backings should make this cheap — zero-copy where possible, decrypted
    /// page lookup on cold paths.  Returns `None` if `id` is out of bounds.
    fn get_vector(&self, id: u32) -> Option<&[f32]>;

    /// Surrogate id for a local position.
    ///
    /// Returns `None` if `id` is out of bounds.
    fn get_surrogate(&self, id: u32) -> Option<u64>;

    /// Optional prefetch hint.
    ///
    /// Implementations may call `madvise(MADV_WILLNEED)` (mmap) or warm a
    /// pagedb page cache entry.  Default is a no-op.
    fn prefetch(&self, _id: u32) {}
}
