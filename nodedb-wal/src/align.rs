// SPDX-License-Identifier: Apache-2.0

//! O_DIRECT alignment utilities.
//!
//! O_DIRECT requires that:
//! - The file offset is aligned to the logical block size (typically 512 or 4096).
//! - The memory buffer address is aligned to the logical block size.
//! - The I/O size is a multiple of the logical block size.
//!
//! This module provides an aligned buffer type that satisfies these constraints.

use crate::error::{Result, WalError};

/// Default alignment for O_DIRECT I/O (4 KiB).
///
/// This matches the typical NVMe logical block size and Linux page size.
/// Can be overridden at runtime via `WalWriterConfig::alignment`, which reads
/// from `WalTuning::alignment`.
pub const DEFAULT_ALIGNMENT: usize = 4096;

/// An aligned byte buffer suitable for O_DIRECT I/O.
///
/// The buffer's starting address is guaranteed to be aligned to `alignment`,
/// and its capacity is rounded up to a multiple of `alignment`.
pub struct AlignedBuf {
    /// Raw allocation. Layout guarantees alignment.
    ptr: *mut u8,

    /// Usable capacity (always a multiple of `alignment`).
    capacity: usize,

    /// Current write position within the buffer.
    len: usize,

    /// Alignment in bytes (power of two).
    alignment: usize,

    /// Layout used for deallocation.
    layout: std::alloc::Layout,
}

// SAFETY: The buffer is a plain byte array with no interior mutability concerns.
// It is owned by a single writer at a time.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    /// Allocate a new aligned buffer.
    ///
    /// `min_capacity` is rounded up to the next multiple of `alignment`.
    /// `alignment` must be a power of two.
    pub fn new(min_capacity: usize, alignment: usize) -> Result<Self> {
        assert!(
            alignment.is_power_of_two(),
            "alignment must be power of two"
        );
        assert!(alignment > 0, "alignment must be > 0");

        // Round capacity up to alignment boundary.
        let capacity = round_up(min_capacity.max(alignment), alignment);

        let layout = std::alloc::Layout::from_size_align(capacity, alignment).map_err(|_| {
            WalError::AlignmentViolation {
                context: "buffer allocation",
                required: alignment,
                actual: min_capacity,
            }
        })?;

        // SAFETY: Layout has non-zero size (capacity >= alignment > 0).
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        Ok(Self {
            ptr,
            capacity,
            len: 0,
            alignment,
            layout,
        })
    }

    /// Allocate with the default 4 KiB alignment.
    pub fn with_default_alignment(min_capacity: usize) -> Result<Self> {
        Self::new(min_capacity, DEFAULT_ALIGNMENT)
    }

    /// Write bytes into the buffer. Returns the number of bytes written.
    ///
    /// If the buffer doesn't have enough remaining capacity, writes as much
    /// as possible and returns the count.
    pub fn write(&mut self, data: &[u8]) -> usize {
        let available = self.capacity - self.len;
        let to_write = data.len().min(available);
        if to_write > 0 {
            // SAFETY: ptr + len is within the allocation, and to_write <= available.
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), to_write);
            }
            self.len += to_write;
        }
        to_write
    }

    /// Get the written portion of the buffer as a byte slice.
    ///
    /// The returned slice starts at an aligned address.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for `len` bytes, and we have shared access.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Get the written portion padded up to the next alignment boundary.
    ///
    /// O_DIRECT requires the I/O size to be a multiple of the block size.
    /// The padding bytes are only zeroed on the buffer's first use (from
    /// `alloc_zeroed`); after a `clear()` they are stale content left over
    /// from whatever was previously written there. Callers that write under
    /// O_DIRECT must frame the padding region as a real record (see
    /// `crate::record::padding`) rather than relying on it reading as zero.
    pub fn as_aligned_slice(&self) -> &[u8] {
        let aligned_len = round_up(self.len, self.alignment);
        let actual_len = aligned_len.min(self.capacity);
        // SAFETY: ptr is valid for `capacity` bytes, and actual_len <= capacity.
        unsafe { std::slice::from_raw_parts(self.ptr, actual_len) }
    }

    /// Reset the buffer for reuse without deallocating.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Current number of bytes written.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer has no written data.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Total capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Remaining capacity.
    pub fn remaining(&self) -> usize {
        self.capacity - self.len
    }

    /// The alignment of this buffer.
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    /// Raw pointer to the buffer start (for io_uring submission).
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Mutable raw pointer to the buffer start.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with this layout in `new()`.
        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

/// Round `value` up to the next multiple of `align` (which must be a power of two).
#[inline]
pub const fn round_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Check if a value is aligned to the given alignment.
#[inline]
pub const fn is_aligned(value: usize, align: usize) -> bool {
    value & (align - 1) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_up_works() {
        assert_eq!(round_up(0, 4096), 0);
        assert_eq!(round_up(1, 4096), 4096);
        assert_eq!(round_up(4096, 4096), 4096);
        assert_eq!(round_up(4097, 4096), 8192);
        assert_eq!(round_up(8192, 4096), 8192);
    }

    #[test]
    fn is_aligned_works() {
        assert!(is_aligned(0, 4096));
        assert!(is_aligned(4096, 4096));
        assert!(is_aligned(8192, 4096));
        assert!(!is_aligned(1, 4096));
        assert!(!is_aligned(4097, 4096));
    }

    #[test]
    fn aligned_buf_address_is_aligned() {
        let buf = AlignedBuf::with_default_alignment(1).unwrap();
        assert!(is_aligned(buf.as_ptr() as usize, DEFAULT_ALIGNMENT));
    }

    #[test]
    fn aligned_buf_capacity_is_aligned() {
        let buf = AlignedBuf::with_default_alignment(1).unwrap();
        assert!(is_aligned(buf.capacity(), DEFAULT_ALIGNMENT));
        assert!(buf.capacity() >= DEFAULT_ALIGNMENT);
    }

    #[test]
    fn aligned_buf_write_and_read() {
        let mut buf = AlignedBuf::with_default_alignment(8192).unwrap();
        let data = b"hello nodedb WAL";
        let written = buf.write(data);
        assert_eq!(written, data.len());
        assert_eq!(&buf.as_slice()[..data.len()], data);
    }

    #[test]
    fn aligned_slice_pads_to_boundary() {
        let mut buf = AlignedBuf::with_default_alignment(8192).unwrap();
        buf.write(b"short");
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.as_aligned_slice().len(), DEFAULT_ALIGNMENT);
    }

    #[test]
    fn clear_resets_without_dealloc() {
        let mut buf = AlignedBuf::with_default_alignment(8192).unwrap();
        let ptr_before = buf.as_ptr();
        buf.write(b"some data");
        buf.clear();
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert_eq!(buf.as_ptr(), ptr_before); // Same allocation.
    }
}
