// SPDX-License-Identifier: Apache-2.0

//! Memory-mapped reader for the NDVS v2 vector segment format.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use nodedb_mem::{BudgetGuard, EngineId, MemoryGovernor};

use super::format::{
    FOOTER_SIZE, FORMAT_VERSION, HEADER_SIZE, MAGIC, VectorSegmentCodec, VectorSegmentDropPolicy,
    observability, vec_pad,
};
use super::writer::write_segment;
use crate::error::VectorError;

/// Memory-mapped vector segment file (v2 NDVS format).
///
/// Exposes a `&[f32]` view of the vector data block and a `&[u64]` view of
/// the surrogate ID block — both zero-copy slices into the mmap region.
///
/// Typically owned by a single Data Plane core, but safe to share across
/// threads behind an [`Arc`]: see the `Send`/`Sync` safety note below.
#[derive(Debug)]
pub struct MmapVectorSegment {
    path: PathBuf,
    _fd: std::fs::File,
    base: *const u8,
    mmap_size: usize,
    dim: usize,
    count: usize,
    /// Byte offset of the vector data block within the mmap.
    vec_offset: usize,
    /// Byte offset of the surrogate ID block within the mmap.
    sid_offset: usize,
    drop_policy: VectorSegmentDropPolicy,
    madvise_state: Option<libc::c_int>,
    /// RAII budget guard for the mmap region.  Held for the lifetime of the
    /// mapping; released automatically on `Drop` alongside `munmap`.
    _budget_guard: Option<BudgetGuard>,
}

// SAFETY: `MmapVectorSegment` holds a `*const u8` (`base`) into a read-only
// `MAP_PRIVATE` mmap region. The region is immutable after construction,
// process-global, and valid for the lifetime of the value. There is no
// interior mutability, so concurrent reads from multiple threads are sound.
unsafe impl Send for MmapVectorSegment {}
unsafe impl Sync for MmapVectorSegment {}

impl MmapVectorSegment {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Create a new segment file (surrogates default to 0) and open it.
    pub fn create(path: &Path, dim: usize, vectors: &[&[f32]]) -> std::io::Result<Self> {
        write_segment(path, dim, vectors, &[])?;
        Self::open_with_policy(path, VectorSegmentDropPolicy::default())
    }

    /// Create a new segment file with explicit surrogate IDs and open it.
    pub fn create_with_surrogates(
        path: &Path,
        dim: usize,
        vectors: &[&[f32]],
        surrogate_ids: &[u64],
    ) -> std::io::Result<Self> {
        write_segment(path, dim, vectors, surrogate_ids)?;
        Self::open_with_policy(path, VectorSegmentDropPolicy::default())
    }

    /// Create a new segment with an explicit drop policy.
    pub fn create_with_policy(
        path: &Path,
        dim: usize,
        vectors: &[&[f32]],
        policy: VectorSegmentDropPolicy,
    ) -> std::io::Result<Self> {
        write_segment(path, dim, vectors, &[])?;
        Self::open_with_policy(path, policy)
    }

    /// Open an existing segment file and memory-map it.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        Self::open_with_policy(path, VectorSegmentDropPolicy::default())
    }

    /// Open an existing segment with an explicit drop policy.
    pub fn open_with_policy(path: &Path, policy: VectorSegmentDropPolicy) -> std::io::Result<Self> {
        let fd = std::fs::OpenOptions::new().read(true).open(path)?;
        let file_size = fd.metadata()?.len() as usize;

        let min_size = HEADER_SIZE + FOOTER_SIZE;
        if file_size < min_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("segment file too small: {file_size} < {min_size} bytes"),
            ));
        }

        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_size,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd.as_raw_fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        let base = base as *const u8;

        Self::validate_and_build(fd, base, file_size, path, policy, None).inspect_err(|_e| {
            unsafe { libc::munmap(base as *mut libc::c_void, file_size) };
        })
    }

    /// Open an existing segment with a memory governor.
    ///
    /// Reserves `file_size` bytes in the `EngineId::Vector` budget before
    /// mapping the file.  Returns `VectorError::BudgetExhausted` if the
    /// governor rejects the reservation.  The reservation is released
    /// automatically when the segment is dropped (RAII via `BudgetGuard`).
    pub fn open_with_governor(
        path: &Path,
        governor: &Arc<MemoryGovernor>,
    ) -> Result<Self, VectorError> {
        Self::open_with_governor_and_policy(path, governor, VectorSegmentDropPolicy::default())
    }

    /// Open an existing segment with a memory governor and explicit drop policy.
    pub fn open_with_governor_and_policy(
        path: &Path,
        governor: &Arc<MemoryGovernor>,
        policy: VectorSegmentDropPolicy,
    ) -> Result<Self, VectorError> {
        let fd = std::fs::OpenOptions::new().read(true).open(path)?;
        let file_size = fd.metadata()?.len() as usize;

        let budget_guard = governor.reserve(EngineId::Vector, file_size)?;

        let min_size = HEADER_SIZE + FOOTER_SIZE;
        if file_size < min_size {
            // budget_guard dropped here → bytes returned to budget
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("segment file too small: {file_size} < {min_size} bytes"),
            )
            .into());
        }

        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_size,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd.as_raw_fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            // budget_guard dropped here → bytes returned to budget
            return Err(std::io::Error::last_os_error().into());
        }
        let base = base as *const u8;

        Self::validate_and_build(fd, base, file_size, path, policy, Some(budget_guard))
            .map_err(VectorError::from)
            .inspect_err(|_| {
                unsafe { libc::munmap(base as *mut libc::c_void, file_size) };
            })
    }

    // ── Validation ────────────────────────────────────────────────────────────

    fn validate_and_build(
        fd: std::fs::File,
        base: *const u8,
        file_size: usize,
        path: &Path,
        policy: VectorSegmentDropPolicy,
        budget_guard: Option<BudgetGuard>,
    ) -> std::io::Result<Self> {
        // Validate magic + format version.
        let header = unsafe { std::slice::from_raw_parts(base, HEADER_SIZE) };
        if &header[0..4] != MAGIC.as_slice() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid NDVS magic bytes",
            ));
        }
        let fv = u16::from_le_bytes([header[4], header[5]]);
        if fv != FORMAT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported segment format version {fv}; expected {FORMAT_VERSION}"),
            ));
        }

        let dim = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let count = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]) as usize;
        let compression_byte = header[21];

        // Codec dispatch — exhaustive match; non-None variants will be
        // obvious when compression is wired in the future.
        let codec = VectorSegmentCodec::from_u8(compression_byte)?;
        match codec {
            VectorSegmentCodec::None => { /* raw packed f32 — proceed */ }
        }

        if dim == 0 && count > 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "segment has dim=0 with nonzero count",
            ));
        }

        // Validate total file size with overflow-safe arithmetic.
        let vec_bytes = dim
            .checked_mul(count)
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("segment header overflow: dim={dim}, count={count}"),
                )
            })?;
        let sid_bytes = count.checked_mul(8).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("surrogate block overflow: count={count}"),
            )
        })?;
        let pad_bytes = vec_pad(vec_bytes);
        let expected = HEADER_SIZE
            .checked_add(vec_bytes)
            .and_then(|n| n.checked_add(pad_bytes))
            .and_then(|n| n.checked_add(sid_bytes))
            .and_then(|n| n.checked_add(FOOTER_SIZE))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "total segment size overflow",
                )
            })?;
        if file_size != expected {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("segment size mismatch: expected {expected} bytes, got {file_size}"),
            ));
        }

        // Validate footer.
        let footer_start = file_size - FOOTER_SIZE;
        let footer = unsafe { std::slice::from_raw_parts(base.add(footer_start), FOOTER_SIZE) };

        // Trailing magic — last 4 bytes of the file must be b"NDVS".
        if &footer[42..46] != MAGIC.as_slice() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid NDVS trailing magic bytes",
            ));
        }

        let footer_fv = u16::from_le_bytes([footer[0], footer[1]]);
        if footer_fv != FORMAT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "unsupported segment footer version {footer_fv}; expected {FORMAT_VERSION}"
                ),
            ));
        }
        let stored_footer_size =
            u32::from_le_bytes([footer[38], footer[39], footer[40], footer[41]]) as usize;
        if stored_footer_size != FOOTER_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("footer_size field {stored_footer_size} != {FOOTER_SIZE}"),
            ));
        }

        // CRC32C integrity check.
        let body = unsafe { std::slice::from_raw_parts(base, footer_start) };
        let computed = crc32c::crc32c(body);
        let stored = u32::from_le_bytes([footer[34], footer[35], footer[36], footer[37]]);
        if computed != stored {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}"),
            ));
        }

        let vec_offset = HEADER_SIZE;
        let sid_offset = HEADER_SIZE + vec_bytes + pad_bytes;

        // Advise MADV_RANDOM: HNSW traversal is non-sequential.
        let mut madvise_state = None;
        if vec_bytes + sid_bytes > 0 {
            let rc =
                unsafe { libc::madvise(base as *mut libc::c_void, file_size, libc::MADV_RANDOM) };
            if rc == 0 {
                madvise_state = Some(libc::MADV_RANDOM);
                observability::RANDOM_COUNT.fetch_add(1, Ordering::Relaxed);
            } else {
                tracing::warn!(
                    path = %path.display(),
                    errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                    "madvise(MADV_RANDOM) failed on vector segment; continuing with kernel default",
                );
            }
        }

        Ok(Self {
            path: path.to_path_buf(),
            _fd: fd,
            base,
            mmap_size: file_size,
            dim,
            count,
            vec_offset,
            sid_offset,
            drop_policy: policy,
            madvise_state,
            _budget_guard: budget_guard,
        })
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /// The madvise hint set on this segment (if any).
    pub fn madvise_state(&self) -> Option<libc::c_int> {
        self.madvise_state
    }

    /// Get a vector by local index. Returns a slice into the mmap'd region.
    #[inline]
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        let idx = id as usize;
        if idx >= self.count {
            return None;
        }
        let byte_len = self.dim.checked_mul(4)?;
        let offset = self.vec_offset.checked_add(idx.checked_mul(byte_len)?)?;
        let end = offset.checked_add(byte_len)?;
        if end > self.sid_offset {
            return None;
        }
        unsafe {
            let ptr = self.base.add(offset) as *const f32;
            Some(std::slice::from_raw_parts(ptr, self.dim))
        }
    }

    /// Get the surrogate ID for a local index (0-based row in this segment).
    #[inline]
    pub fn get_surrogate_id(&self, id: u32) -> Option<u64> {
        let idx = id as usize;
        if idx >= self.count {
            return None;
        }
        let offset = self.sid_offset.checked_add(idx.checked_mul(8)?)?;
        let end = offset.checked_add(8)?;
        let sid_end = self.mmap_size - FOOTER_SIZE;
        if end > sid_end {
            return None;
        }
        let bytes = unsafe { std::slice::from_raw_parts(self.base.add(offset), 8) };
        Some(u64::from_le_bytes(bytes.try_into().expect("invariant: from_raw_parts constructed with len=8, so try_into::<[u8;8]> always succeeds")))
    }

    /// The full vector data block as a contiguous `&[f32]` of length `D × N`.
    ///
    /// Suitable for SIMD distance computation over all vectors.
    #[inline]
    pub fn all_vectors_flat(&self) -> &[f32] {
        let float_count = self.dim * self.count;
        unsafe {
            let ptr = self.base.add(self.vec_offset) as *const f32;
            std::slice::from_raw_parts(ptr, float_count)
        }
    }

    /// The full surrogate ID block as a contiguous `&[u64]` of length `N`.
    ///
    /// Parallel to `all_vectors_flat`: row `i` in vectors ↔ `surrogate_ids[i]`.
    #[inline]
    pub fn all_surrogate_ids(&self) -> &[u64] {
        unsafe {
            let ptr = self.base.add(self.sid_offset) as *const u64;
            std::slice::from_raw_parts(ptr, self.count)
        }
    }

    /// Prefetch a vector's page into memory via `madvise(MADV_WILLNEED)`.
    pub fn prefetch(&self, id: u32) {
        let idx = id as usize;
        if idx >= self.count {
            return;
        }
        let byte_len = match self.dim.checked_mul(4) {
            Some(v) => v,
            None => return,
        };
        let Some(idx_bytes) = idx.checked_mul(byte_len) else {
            return;
        };
        let Some(offset) = self.vec_offset.checked_add(idx_bytes) else {
            return;
        };
        if offset
            .checked_add(byte_len)
            .is_none_or(|e| e > self.sid_offset)
        {
            return;
        }
        let page_start = offset & !(4095);
        let len = (byte_len + 4095) & !(4095);
        unsafe {
            libc::madvise(
                self.base.add(page_start) as *mut libc::c_void,
                len,
                libc::MADV_WILLNEED,
            );
        }
    }

    /// Prefetch a batch of vector IDs.
    pub fn prefetch_batch(&self, ids: &[u32]) {
        for &id in ids {
            self.prefetch(id);
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn mmap_bytes(&self) -> usize {
        self.mmap_size
    }

    pub fn file_size(&self) -> usize {
        self.mmap_size
    }
}

impl Drop for MmapVectorSegment {
    fn drop(&mut self) {
        if !self.base.is_null() && self.mmap_size > 0 {
            if self.drop_policy.dontneed_on_drop() {
                let data_bytes = self.mmap_size.saturating_sub(HEADER_SIZE + FOOTER_SIZE);
                if data_bytes > 0 {
                    unsafe {
                        libc::madvise(
                            self.base as *mut libc::c_void,
                            self.mmap_size,
                            libc::MADV_DONTNEED,
                        );
                    }
                    observability::DONTNEED_COUNT.fetch_add(1, Ordering::Relaxed);
                }
            }
            unsafe {
                libc::munmap(self.base as *mut libc::c_void, self.mmap_size);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vseg");

        let v0 = vec![1.0f32, 2.0, 3.0];
        let v1 = vec![4.0f32, 5.0, 6.0];
        let v2 = vec![7.0f32, 8.0, 9.0];
        let surrogates = vec![10u64, 20, 30];

        let seg =
            MmapVectorSegment::create_with_surrogates(&path, 3, &[&v0, &v1, &v2], &surrogates)
                .unwrap();

        assert_eq!(seg.dim(), 3);
        assert_eq!(seg.count(), 3);
        assert_eq!(seg.get_vector(0).unwrap(), &[1.0, 2.0, 3.0]);
        assert_eq!(seg.get_vector(1).unwrap(), &[4.0, 5.0, 6.0]);
        assert_eq!(seg.get_vector(2).unwrap(), &[7.0, 8.0, 9.0]);
        assert!(seg.get_vector(3).is_none());
        assert_eq!(seg.get_surrogate_id(0).unwrap(), 10);
        assert_eq!(seg.get_surrogate_id(1).unwrap(), 20);
        assert_eq!(seg.get_surrogate_id(2).unwrap(), 30);
        assert!(seg.get_surrogate_id(3).is_none());
    }

    #[test]
    fn flat_slices() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flat.vseg");

        let v0 = vec![1.0f32, 2.0, 3.0];
        let v1 = vec![4.0f32, 5.0, 6.0];
        let sids = vec![100u64, 200];

        let seg = MmapVectorSegment::create_with_surrogates(&path, 3, &[&v0, &v1], &sids).unwrap();

        assert_eq!(seg.all_vectors_flat(), &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        assert_eq!(seg.all_surrogate_ids(), &[100u64, 200]);
    }

    #[test]
    fn reopen_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.vseg");

        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|i| vec![i as f32, (i as f32).sin(), (i as f32).cos()])
            .collect();
        let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();
        let sids: Vec<u64> = (0u64..100).collect();

        MmapVectorSegment::create_with_surrogates(&path, 3, &refs, &sids).unwrap();

        let seg = MmapVectorSegment::open(&path).unwrap();
        assert_eq!(seg.count(), 100);
        for (i, v) in vectors.iter().enumerate() {
            assert_eq!(seg.get_vector(i as u32).unwrap(), v.as_slice());
            assert_eq!(seg.get_surrogate_id(i as u32).unwrap(), i as u64);
        }
    }

    #[test]
    fn no_surrogates_defaults_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nosid.vseg");

        let v = vec![1.0f32, 2.0];
        let seg = MmapVectorSegment::create(&path, 2, &[&v]).unwrap();
        assert_eq!(seg.get_surrogate_id(0).unwrap(), 0);
    }

    #[test]
    fn prefetch_does_not_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prefetch.vseg");

        let v = vec![1.0f32; 768];
        let seg = MmapVectorSegment::create(&path, 768, &[&v]).unwrap();
        seg.prefetch(0);
        seg.prefetch(999);
    }

    #[test]
    fn empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.vseg");

        let seg = MmapVectorSegment::create(&path, 3, &[]).unwrap();
        assert_eq!(seg.count(), 0);
        assert!(seg.get_vector(0).is_none());
        assert_eq!(seg.all_vectors_flat().len(), 0);
        assert_eq!(seg.all_surrogate_ids().len(), 0);
    }

    #[test]
    fn footer_golden_layout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("golden.vseg");
        let v = vec![1.0f32, 2.0, 3.0];
        write_segment(&path, 3, &[&v], &[42]).unwrap();
        let data = std::fs::read(&path).unwrap();

        // Footer starts at file_size - FOOTER_SIZE.
        let footer_start = data.len() - FOOTER_SIZE;
        let footer = &data[footer_start..];

        // [0..2] format_version = FORMAT_VERSION.
        let fv = u16::from_le_bytes([footer[0], footer[1]]);
        assert_eq!(fv, FORMAT_VERSION);

        // [34..38] checksum matches body CRC32C.
        let body = &data[..footer_start];
        let expected_crc = crc32c::crc32c(body);
        let stored_crc = u32::from_le_bytes([footer[34], footer[35], footer[36], footer[37]]);
        assert_eq!(stored_crc, expected_crc);

        // [38..42] footer_size = FOOTER_SIZE (46).
        let fs = u32::from_le_bytes([footer[38], footer[39], footer[40], footer[41]]) as usize;
        assert_eq!(fs, FOOTER_SIZE);

        // [42..46] trailing magic = b"NDVS".
        assert_eq!(&footer[42..46], b"NDVS");
    }

    #[test]
    fn trailing_magic_corruption_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trailmagic.vseg");
        let v = vec![1.0f32, 2.0, 3.0];
        write_segment(&path, 3, &[&v], &[42]).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        // Corrupt the last 4 bytes (trailing magic).
        let last = data.len();
        data[last - 4] = 0xde;
        data[last - 3] = 0xad;
        data[last - 2] = 0xbe;
        data[last - 1] = 0xef;
        std::fs::write(&path, &data).unwrap();

        let result = MmapVectorSegment::open(&path);
        assert!(result.is_err(), "expected trailing magic error");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("trailing magic"),
            "expected trailing magic message, got: {err}"
        );
    }

    #[test]
    fn footer_version_mismatch_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fvmismatch.vseg");
        let v = vec![1.0f32, 2.0, 3.0];
        write_segment(&path, 3, &[&v], &[42]).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        // Corrupt the footer format version bytes to 99.
        let footer_start = data.len() - FOOTER_SIZE;
        let fv_bytes = 99u16.to_le_bytes();
        data[footer_start] = fv_bytes[0];
        data[footer_start + 1] = fv_bytes[1];
        std::fs::write(&path, &data).unwrap();

        let result = MmapVectorSegment::open(&path);
        assert!(result.is_err(), "expected footer version mismatch error");
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn crc_corruption_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.vseg");

        let v = vec![1.0f32, 2.0, 3.0];
        write_segment(&path, 3, &[&v], &[42]).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        data[HEADER_SIZE] ^= 0xff;
        std::fs::write(&path, &data).unwrap();

        let result = MmapVectorSegment::open(&path);
        assert!(result.is_err(), "expected CRC error");
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn bad_magic_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("badmagic.vseg");

        let v = vec![1.0f32, 2.0];
        write_segment(&path, 2, &[&v], &[]).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        data[0] = b'X';
        std::fs::write(&path, &data).unwrap();

        let result = MmapVectorSegment::open(&path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn overflow_dim_count_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overflow.vseg");

        let dim: u32 = 0x40000001;
        let count: u64 = 0x40000001;

        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&dim.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.push(0u8); // dtype
        buf.push(0u8); // codec None
        buf.extend_from_slice(&[0u8; 6]); // reserved
        std::fs::write(&path, &buf).unwrap();

        let result = MmapVectorSegment::open(&path);
        assert!(
            result.is_err(),
            "expected Err for overflow-inducing dim/count"
        );
    }

    #[test]
    fn zero_dim_with_nonzero_count_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zerodim.vseg");

        let dim: u32 = 0;
        let count: u64 = 1000;

        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&dim.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.push(0u8);
        buf.push(0u8);
        buf.extend_from_slice(&[0u8; 6]);
        buf.extend_from_slice(&[0u8; 64]);
        std::fs::write(&path, &buf).unwrap();

        let result = MmapVectorSegment::open(&path);
        assert!(result.is_err(), "expected Err for dim=0 with nonzero count");
    }
}
