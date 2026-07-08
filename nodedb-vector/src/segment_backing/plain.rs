// SPDX-License-Identifier: Apache-2.0

//! [`PlainMmapBacking`]: zero-copy [`VectorSegmentBacking`] over a plaintext
//! NDVS mmap segment.

use std::sync::Arc;

use crate::mmap_segment::MmapVectorSegment;

use super::VectorSegmentBacking;

/// Zero-copy [`VectorSegmentBacking`] backed by a plaintext NDVS mmap segment.
///
/// Vectors and surrogate IDs are served as slices directly into the mmap
/// region — no allocation on the read path.
///
/// # `Send + Sync` rationale
///
/// [`MmapVectorSegment`] is declared `!Send + !Sync` because it holds a
/// `*const u8` field (`base`) pointing into the mmap region.  Raw pointers are
/// conservative: the compiler cannot know whether the pointee is safe to share.
///
/// The mmap region behind `base` is:
/// - mapped with `PROT_READ | MAP_PRIVATE` — never mutated through this
///   pointer after construction,
/// - valid for exactly the lifetime of the [`MmapVectorSegment`] (the
///   descriptor `_fd` keeps the file open; `munmap` runs in `Drop`),
/// - not thread-affine — the OS virtual-memory subsystem treats it as a
///   process-global read-only region.
///
/// All access to `base` goes through `get_vector` / `get_surrogate` /
/// `prefetch`, which derive shared borrows (`&[f32]`, `&[u8]`) that live no
/// longer than `&self`.  No `&mut` path exists.  Multiple threads reading
/// distinct vectors concurrently is safe for the same reason `&[T]` is `Sync`.
///
/// The `Arc<MmapVectorSegment>` wrapper ensures the segment (and therefore
/// the mmap region) outlives any `&[f32]` slice handed out through this type.
///
/// `PlainMmapBacking` is `Send + Sync` automatically: [`MmapVectorSegment`]
/// carries the `unsafe impl Send + Sync` (justified by the invariants above),
/// so `Arc<MmapVectorSegment>` — and therefore this wrapper — is too.
pub struct PlainMmapBacking {
    inner: Arc<MmapVectorSegment>,
}

impl PlainMmapBacking {
    /// Wrap a [`MmapVectorSegment`] that is not yet reference-counted.
    pub fn new(seg: MmapVectorSegment) -> Self {
        Self {
            inner: Arc::new(seg),
        }
    }

    /// Wrap an already reference-counted segment.
    ///
    /// Useful when the same segment is shared with other consumers (e.g. a
    /// [`crate::collection::VectorCollection`] that also owns the segment for
    /// direct SIMD scan).
    pub fn from_arc(seg: Arc<MmapVectorSegment>) -> Self {
        Self { inner: seg }
    }

    /// Access the underlying segment.
    pub fn segment(&self) -> &Arc<MmapVectorSegment> {
        &self.inner
    }
}

impl VectorSegmentBacking for PlainMmapBacking {
    #[inline]
    fn len(&self) -> usize {
        self.inner.count()
    }

    #[inline]
    fn dim(&self) -> usize {
        self.inner.dim()
    }

    #[inline]
    fn get_vector(&self, id: u32) -> Option<&[f32]> {
        self.inner.get_vector(id)
    }

    #[inline]
    fn get_surrogate(&self, id: u32) -> Option<u64> {
        self.inner.get_surrogate_id(id)
    }

    #[inline]
    fn prefetch(&self, id: u32) {
        self.inner.prefetch(id);
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::mmap_segment::MmapVectorSegment;

    fn make_backing(dim: usize, vecs: &[Vec<f32>]) -> PlainMmapBacking {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.ndvs");

        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let surrogates: Vec<u64> = (0..vecs.len() as u64).collect();

        let seg =
            MmapVectorSegment::create_with_surrogates(&path, dim, &refs, &surrogates).unwrap();

        // Keep the tempdir alive by leaking it for the test duration.
        // The backing borrows from the mmap, which is already self-contained
        // (fd kept open by the segment); dir can be dropped.
        drop(dir);

        PlainMmapBacking::new(seg)
    }

    #[test]
    fn plain_backing_basic_roundtrip() {
        let dim = 4;
        let vecs = vec![
            vec![1.0_f32, 2.0, 3.0, 4.0],
            vec![5.0_f32, 6.0, 7.0, 8.0],
            vec![9.0_f32, 10.0, 11.0, 12.0],
        ];

        let backing = make_backing(dim, &vecs);

        assert_eq!(backing.len(), 3);
        assert_eq!(backing.dim(), 4);
        assert!(!backing.is_empty());

        for (i, expected) in vecs.iter().enumerate() {
            let got = backing
                .get_vector(i as u32)
                .expect("vector must be present");
            assert_eq!(got, expected.as_slice(), "vector {i} mismatch");

            let sid = backing
                .get_surrogate(i as u32)
                .expect("surrogate must be present");
            assert_eq!(sid, i as u64, "surrogate {i} mismatch");
        }

        // prefetch must not panic
        backing.prefetch(0);
        backing.prefetch(1);
        backing.prefetch(2);
    }

    /// Compile-time proof that `PlainMmapBacking` satisfies `Send + Sync`.
    #[test]
    fn plain_backing_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}

        let dir = tempdir().unwrap();
        let path = dir.path().join("check.ndvs");
        let seg = MmapVectorSegment::create(&path, 2, &[&[1.0_f32, 2.0]]).unwrap();
        let backing = PlainMmapBacking::new(seg);

        assert_send_sync(&backing);
    }

    #[test]
    fn plain_backing_out_of_bounds_returns_none() {
        let backing = make_backing(3, &[vec![1.0_f32, 2.0, 3.0]]);

        assert!(
            backing.get_vector(1).is_none(),
            "id=1 must be out of bounds"
        );
        assert!(
            backing.get_surrogate(1).is_none(),
            "id=1 surrogate must be out of bounds"
        );
        // prefetch on out-of-bounds must be a no-op (no panic)
        backing.prefetch(1);
    }

    #[test]
    fn plain_backing_empty_segment() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.ndvs");
        let seg = MmapVectorSegment::create(&path, 4, &[]).unwrap();
        let backing = PlainMmapBacking::new(seg);

        assert_eq!(backing.len(), 0);
        assert!(backing.is_empty());
        assert!(backing.get_vector(0).is_none());
        assert!(backing.get_surrogate(0).is_none());
        backing.prefetch(0);
    }
}
