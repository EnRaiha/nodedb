// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// Bitmask helpers
// ---------------------------------------------------------------------------

/// Count total set bits across a bitmask.
#[inline]
pub fn popcount(mask: &[u64]) -> u64 {
    mask.iter().map(|w| w.count_ones() as u64).sum()
}

/// Bitwise AND of two equal-length bitmasks, SIMD-accelerated.
pub fn bitmask_and(a: &[u64], b: &[u64]) -> Vec<u64> {
    let len = a.len().min(b.len());
    if len == 0 {
        return Vec::new();
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            return unsafe { bitmask_and_avx512(&a[..len], &b[..len]) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { bitmask_and_avx2(&a[..len], &b[..len]) };
        }
    }
    bitmask_and_scalar(&a[..len], &b[..len])
}

#[inline]
fn bitmask_and_scalar(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = vec![0u64; a.len()];
    for i in 0..a.len() {
        out[i] = a[i] & b[i];
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn bitmask_and_avx512(a: &[u64], b: &[u64]) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let len = a.len();
        let mut out = vec![0u64; len];
        let chunks = len / 8;
        let a_ptr = a.as_ptr() as *const i64;
        let b_ptr = b.as_ptr() as *const i64;
        let o_ptr = out.as_mut_ptr() as *mut i64;

        for i in 0..chunks {
            let va = _mm512_loadu_si512(a_ptr.add(i * 8).cast());
            let vb = _mm512_loadu_si512(b_ptr.add(i * 8).cast());
            let vc = _mm512_and_si512(va, vb);
            _mm512_storeu_si512(o_ptr.add(i * 8).cast(), vc);
        }

        // Scalar tail.
        for i in (chunks * 8)..len {
            out[i] = a[i] & b[i];
        }

        out
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn bitmask_and_avx2(a: &[u64], b: &[u64]) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let len = a.len();
        let mut out = vec![0u64; len];
        let chunks = len / 4;
        let a_ptr = a.as_ptr() as *const i64;
        let b_ptr = b.as_ptr() as *const i64;

        for i in 0..chunks {
            let va = _mm256_loadu_si256(a_ptr.add(i * 4).cast());
            let vb = _mm256_loadu_si256(b_ptr.add(i * 4).cast());
            let vc = _mm256_and_si256(va, vb);
            _mm256_storeu_si256(out.as_mut_ptr().add(i * 4).cast(), vc);
        }

        // Scalar tail.
        for i in (chunks * 4)..len {
            out[i] = a[i] & b[i];
        }

        out
    }
}

/// Bitwise OR of two bitmasks, SIMD-accelerated.
/// If the slices differ in length, the longer tail is copied as-is.
pub fn bitmask_or(a: &[u64], b: &[u64]) -> Vec<u64> {
    let max_len = a.len().max(b.len());
    let min_len = a.len().min(b.len());
    if max_len == 0 {
        return Vec::new();
    }

    // OR the overlapping prefix.
    let prefix = if min_len == 0 {
        Vec::new()
    } else {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                unsafe { bitmask_or_avx512(&a[..min_len], &b[..min_len]) }
            } else if std::is_x86_feature_detected!("avx2") {
                unsafe { bitmask_or_avx2(&a[..min_len], &b[..min_len]) }
            } else {
                bitmask_or_scalar(&a[..min_len], &b[..min_len])
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        bitmask_or_scalar(&a[..min_len], &b[..min_len])
    };

    // Extend with the tail of the longer slice.
    let mut out = prefix;
    out.resize(max_len, 0u64);
    if a.len() > min_len {
        out[min_len..].copy_from_slice(&a[min_len..]);
    } else if b.len() > min_len {
        out[min_len..].copy_from_slice(&b[min_len..]);
    }
    out
}

#[inline]
fn bitmask_or_scalar(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = vec![0u64; a.len()];
    for i in 0..a.len() {
        out[i] = a[i] | b[i];
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn bitmask_or_avx512(a: &[u64], b: &[u64]) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let len = a.len();
        let mut out = vec![0u64; len];
        let chunks = len / 8;
        let a_ptr = a.as_ptr() as *const i64;
        let b_ptr = b.as_ptr() as *const i64;
        let o_ptr = out.as_mut_ptr() as *mut i64;

        for i in 0..chunks {
            let va = _mm512_loadu_si512(a_ptr.add(i * 8).cast());
            let vb = _mm512_loadu_si512(b_ptr.add(i * 8).cast());
            let vc = _mm512_or_si512(va, vb);
            _mm512_storeu_si512(o_ptr.add(i * 8).cast(), vc);
        }

        // Scalar tail.
        for i in (chunks * 8)..len {
            out[i] = a[i] | b[i];
        }

        out
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn bitmask_or_avx2(a: &[u64], b: &[u64]) -> Vec<u64> {
    use std::arch::x86_64::*;
    unsafe {
        let len = a.len();
        let mut out = vec![0u64; len];
        let chunks = len / 4;
        let a_ptr = a.as_ptr() as *const i64;
        let b_ptr = b.as_ptr() as *const i64;

        for i in 0..chunks {
            let va = _mm256_loadu_si256(a_ptr.add(i * 4).cast());
            let vb = _mm256_loadu_si256(b_ptr.add(i * 4).cast());
            let vc = _mm256_or_si256(va, vb);
            _mm256_storeu_si256(out.as_mut_ptr().add(i * 4).cast(), vc);
        }

        // Scalar tail.
        for i in (chunks * 4)..len {
            out[i] = a[i] | b[i];
        }

        out
    }
}

/// Bitwise NOT of a bitmask (flips all bits up to `row_count`).
pub fn bitmask_not(mask: &[u64], row_count: usize) -> Vec<u64> {
    let words = row_count.div_ceil(64);
    let mut out = vec![0u64; words];
    for i in 0..mask.len().min(words) {
        out[i] = !mask[i];
    }
    // Clear bits beyond row_count in the last word.
    let tail = row_count % 64;
    if tail > 0 && !out.is_empty() {
        let last = out.len() - 1;
        out[last] &= (1u64 << tail) - 1;
    }
    out
}

/// Create an all-ones bitmask for `row_count` rows.
pub fn bitmask_all(row_count: usize) -> Vec<u64> {
    let words = row_count.div_ceil(64);
    let mut out = vec![u64::MAX; words];
    let tail = row_count % 64;
    if tail > 0 && !out.is_empty() {
        let last = out.len() - 1;
        out[last] = (1u64 << tail) - 1;
    }
    out
}

/// Expand bitmask to a selection vector of row indices.
pub fn bitmask_to_indices(mask: &[u64]) -> Vec<u32> {
    let ones: u64 = popcount(mask);
    let mut out = Vec::with_capacity(ones as usize);
    for (word_idx, &word) in mask.iter().enumerate() {
        if word == 0 {
            continue;
        }
        let base = (word_idx as u32) * 64;
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros();
            out.push(base + bit);
            w &= w - 1; // clear lowest set bit
        }
    }
    out
}

/// Number of u64 words needed for `row_count` bits.
#[inline]
pub fn words_for(row_count: usize) -> usize {
    row_count.div_ceil(64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmask_and_works() {
        let a = vec![0b1111_0000u64];
        let b = vec![0b1010_1010u64];
        let c = bitmask_and(&a, &b);
        assert_eq!(c, vec![0b1010_0000u64]);
    }

    #[test]
    fn bitmask_or_works() {
        let a = vec![0b1111_0000u64];
        let b = vec![0b0000_1111u64];
        let c = bitmask_or(&a, &b);
        assert_eq!(c, vec![0b1111_1111u64]);
    }

    #[test]
    fn bitmask_not_works() {
        let mask = vec![0u64];
        let inv = bitmask_not(&mask, 10);
        assert_eq!(popcount(&inv), 10);
    }

    #[test]
    fn bitmask_all_works() {
        let mask = bitmask_all(100);
        assert_eq!(popcount(&mask), 100);
        // Should have exactly 2 words (64 + 36 bits).
        assert_eq!(mask.len(), 2);
    }

    #[test]
    fn bitmask_to_indices_works() {
        let mask = vec![0b1010_0101u64];
        let indices = bitmask_to_indices(&mask);
        assert_eq!(indices, vec![0, 2, 5, 7]);
    }

    #[test]
    fn popcount_works() {
        assert_eq!(popcount(&[u64::MAX, u64::MAX]), 128);
        assert_eq!(popcount(&[0, 0]), 0);
        assert_eq!(popcount(&[1]), 1);
    }
}
