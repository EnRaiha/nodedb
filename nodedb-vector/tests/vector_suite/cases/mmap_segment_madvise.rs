// SPDX-License-Identifier: BUSL-1.1

//! Spec: mmap vector segments must advise the kernel of their access pattern.
//!
//! HNSW graph traversal jumps between non-adjacent vector IDs. Default
//! `MADV_NORMAL` triggers ~128 KiB readahead per 4 KiB fault, wasting NVMe
//! bandwidth on neighbouring vectors that are evicted before the graph
//! walk reaches them. After `mmap`, segments must set `MADV_RANDOM`.
//!
//! When a segment is dropped, its pages should be proactively released via
//! `MADV_DONTNEED` so cold segments don't dominate the kernel page cache.

use nodedb_vector::mmap_segment::MmapVectorSegment;
use tempfile::tempdir;

fn make_segment(path: &std::path::Path, dim: usize, n: usize) -> MmapVectorSegment {
    let vecs: Vec<Vec<f32>> = (0..n).map(|i| vec![i as f32; dim]).collect();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
    MmapVectorSegment::create(path, dim, &refs).unwrap()
}

#[test]
fn open_advises_random() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("rand.vseg");
    let seg = make_segment(&path, 64, 32);

    // Spec: open must call madvise(MADV_RANDOM) on the mapped region.
    // Observable via an accessor that records the last advice hint.
    let advice = seg.madvise_state();
    assert_eq!(
        advice,
        Some(libc::MADV_RANDOM),
        "MmapVectorSegment::open must advise MADV_RANDOM; got {advice:?}"
    );
}

#[test]
fn reopen_also_advises_random() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("reopen.vseg");
    make_segment(&path, 16, 8);

    let seg = MmapVectorSegment::open(&path).unwrap();
    assert_eq!(seg.madvise_state(), Some(libc::MADV_RANDOM));
}

#[test]
fn drop_releases_pages_when_configured() {
    use nodedb_vector::mmap_segment::VectorSegmentDropPolicy;

    let dir = tempdir().unwrap();
    let path = dir.path().join("drop.vseg");

    // Default policy releases pages on drop.
    assert!(VectorSegmentDropPolicy::default().dontneed_on_drop());

    // The drop-hook must observably emit MADV_DONTNEED. Expose via a
    // test-only counter on the segment module.
    let before = nodedb_vector::mmap_segment::observability::dontneed_count();
    {
        let _seg = make_segment(&path, 8, 4);
    }
    let after = nodedb_vector::mmap_segment::observability::dontneed_count();
    assert_eq!(
        after - before,
        1,
        "drop must call madvise(MADV_DONTNEED) when policy.dontneed_on_drop()=true"
    );
}

#[test]
fn drop_skips_release_when_disabled() {
    use nodedb_vector::mmap_segment::VectorSegmentDropPolicy;

    let dir = tempdir().unwrap();
    let path = dir.path().join("drop_off.vseg");
    let vecs: Vec<Vec<f32>> = (0..4).map(|i| vec![i as f32; 8]).collect();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();

    let before = nodedb_vector::mmap_segment::observability::dontneed_count();
    {
        let _seg = MmapVectorSegment::create_with_policy(
            &path,
            8,
            &refs,
            VectorSegmentDropPolicy::keep_resident(),
        )
        .unwrap();
    }
    let after = nodedb_vector::mmap_segment::observability::dontneed_count();
    assert_eq!(
        after, before,
        "keep_resident() policy must suppress MADV_DONTNEED on drop"
    );
}

#[test]
fn empty_segment_does_not_advise() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.vseg");

    // Zero data bytes — advising a header-only region is a noop at best,
    // EINVAL at worst on some kernels. The open path must handle this.
    let seg = MmapVectorSegment::create(&path, 3, &[]).unwrap();
    assert_eq!(seg.count(), 0);
    // Either None (skipped advise on zero-data) or MADV_RANDOM (advised header
    // page only) are acceptable; a panic or error is not.
    let advice = seg.madvise_state();
    assert!(advice.is_none() || advice == Some(libc::MADV_RANDOM));
}
