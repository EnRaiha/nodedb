// SPDX-License-Identifier: Apache-2.0

//! Shared SIMD Hamming-distance kernel with runtime CPU-feature dispatch.
//!
//! Used by 1-bit quantizers ([`bbq`](super::bbq), [`rabitq`](super::rabitq))
//! and any future binary code that needs to count differing bits between
//! two equal-length packed-bit slices.
//!
//! Dispatch order (resolved once via [`OnceLock`]):
//!
//! 1. `x86_64` + `avx512f` + `avx512vpopcntdq` → AVX-512 VPOPCNTDQ kernel
//! 2. `x86_64` + `avx2`                       → 64-bit popcnt unrolled 4×
//! 3. `aarch64`                                → NEON `vcntq_u8`
//! 4. fallback                                  → scalar 8-byte popcnt

#![allow(unsafe_op_in_unsafe_fn)]

use std::sync::OnceLock;

type HammingFn = fn(a: &[u8], b: &[u8]) -> u32;

static DISPATCH: OnceLock<HammingFn> = OnceLock::new();

fn scalar(a: &[u8], b: &[u8]) -> u32 {
    let chunks = a.len() / 8;
    let rem = a.len() % 8;
    let mut count = 0u32;
    for i in 0..chunks {
        let av = u64::from_ne_bytes(a[i * 8..i * 8 + 8].try_into().unwrap_or([0u8; 8]));
        let bv = u64::from_ne_bytes(b[i * 8..i * 8 + 8].try_into().unwrap_or([0u8; 8]));
        count += (av ^ bv).count_ones();
    }
    let base = chunks * 8;
    for i in 0..rem {
        count += (a[base + i] ^ b[base + i]).count_ones();
    }
    count
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512vpopcntdq")]
unsafe fn avx512(a: &[u8], b: &[u8]) -> u32 {
    use std::arch::x86_64::*;
    let mut count = 0u32;
    let chunks = a.len() / 64;
    let rem = a.len() % 64;
    for i in 0..chunks {
        let va = _mm512_loadu_si512(a.as_ptr().add(i * 64) as *const __m512i);
        let vb = _mm512_loadu_si512(b.as_ptr().add(i * 64) as *const __m512i);
        let xored = _mm512_xor_si512(va, vb);
        let popcnt = _mm512_popcnt_epi64(xored);
        let lo = _mm512_extracti64x4_epi64(popcnt, 0);
        let hi = _mm512_extracti64x4_epi64(popcnt, 1);
        let sum4 = _mm256_add_epi64(lo, hi);
        let sum2 = _mm256_add_epi64(sum4, _mm256_permute4x64_epi64(sum4, 0b0100_1110));
        let sum1 = _mm256_add_epi64(sum2, _mm256_permute4x64_epi64(sum2, 0b0001_0001));
        count += _mm256_extract_epi64(sum1, 0) as u32;
    }
    let base = chunks * 64;
    for i in 0..rem {
        count += (a[base + i] ^ b[base + i]).count_ones();
    }
    count
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2(a: &[u8], b: &[u8]) -> u32 {
    let mut count = 0u32;
    let n8 = a.len() / 8;
    let mut i = 0usize;
    while i + 4 <= n8 {
        for word in i..i + 4 {
            let start = word * 8;
            let end = start + 8;
            let av = u64::from_ne_bytes(a[start..end].try_into().unwrap_or([0u8; 8]));
            let bv = u64::from_ne_bytes(b[start..end].try_into().unwrap_or([0u8; 8]));
            count += (av ^ bv).count_ones();
        }
        i += 4;
    }
    while i < n8 {
        let start = i * 8;
        let end = start + 8;
        let av = u64::from_ne_bytes(a[start..end].try_into().unwrap_or([0u8; 8]));
        let bv = u64::from_ne_bytes(b[start..end].try_into().unwrap_or([0u8; 8]));
        count += (av ^ bv).count_ones();
        i += 1;
    }
    let tail_start = n8 * 8;
    for j in tail_start..a.len() {
        count += (a[j] ^ b[j]).count_ones();
    }
    count
}

#[cfg(target_arch = "x86_64")]
fn avx512_trampoline(a: &[u8], b: &[u8]) -> u32 {
    unsafe { avx512(a, b) }
}

#[cfg(target_arch = "x86_64")]
fn avx2_trampoline(a: &[u8], b: &[u8]) -> u32 {
    unsafe { avx2(a, b) }
}

#[cfg(target_arch = "aarch64")]
fn neon(a: &[u8], b: &[u8]) -> u32 {
    use std::arch::aarch64::*;
    let mut count = 0u32;
    let chunks = a.len() / 16;
    let rem = a.len() % 16;
    unsafe {
        for i in 0..chunks {
            let va = vld1q_u8(a.as_ptr().add(i * 16));
            let vb = vld1q_u8(b.as_ptr().add(i * 16));
            let xored = veorq_u8(va, vb);
            let popcnt = vcntq_u8(xored);
            count += vaddvq_u8(popcnt) as u32;
        }
    }
    let base = chunks * 16;
    for i in 0..rem {
        count += (a[base + i] ^ b[base + i]).count_ones();
    }
    count
}

fn resolve() -> HammingFn {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512vpopcntdq")
        {
            return avx512_trampoline;
        }
        if std::is_x86_feature_detected!("avx2") {
            return avx2_trampoline;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        return neon;
    }
    #[allow(unreachable_code)]
    {
        scalar
    }
}

/// Count the number of differing bits between two equal-length packed-bit
/// slices, dispatching to the best available SIMD kernel at runtime.
///
/// In debug builds, panics if `a.len() != b.len()`. In release builds, the
/// shorter length is used and the tail of the longer slice is ignored.
#[inline]
pub fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len(), "hamming_distance: slice length mismatch");
    let len = a.len().min(b.len());
    let f = DISPATCH.get_or_init(resolve);
    f(&a[..len], &b[..len])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_distance_to_self() {
        let bits = vec![0b1010_1010u8, 0b1100_1100, 0b1111_0000];
        assert_eq!(hamming_distance(&bits, &bits), 0);
    }

    #[test]
    fn full_inversion_byte() {
        let a = vec![0xFFu8];
        let b = vec![0x00u8];
        assert_eq!(hamming_distance(&a, &b), 8);
    }

    #[test]
    fn full_inversion_multibyte() {
        let dim = 64;
        let a: Vec<u8> = (0..dim as u8).collect();
        let b: Vec<u8> = a.iter().map(|&x| !x).collect();
        assert_eq!(hamming_distance(&a, &b), 512);
    }

    #[test]
    fn unaligned_subslices_match_scalar() {
        let a_storage: Vec<u8> = (0..258).map(|i| (i * 31 + 7) as u8).collect();
        let b_storage: Vec<u8> = (0..259).map(|i| (i * 17 + 3) as u8).collect();
        let a = &a_storage[1..257];
        let b = &b_storage[2..258];
        assert_eq!(hamming_distance(a, b), scalar(a, b));
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_directly_handles_unaligned_subslices() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let a_storage: Vec<u8> = (0..258).map(|i| (i * 31 + 7) as u8).collect();
        let b_storage: Vec<u8> = (0..259).map(|i| (i * 17 + 3) as u8).collect();
        let a = &a_storage[1..257];
        let b = &b_storage[2..258];
        assert_eq!(unsafe { avx2(a, b) }, scalar(a, b));
    }

    #[test]
    fn agrees_across_lengths() {
        // Cross-check dispatched kernel against scalar reference for various
        // lengths so any SIMD path divergence is caught.
        for len in [1usize, 7, 8, 9, 15, 16, 31, 32, 63, 64, 65, 127, 128, 255] {
            let a: Vec<u8> = (0..len).map(|i| (i * 31 + 7) as u8).collect();
            let b: Vec<u8> = (0..len).map(|i| (i * 17 + 3) as u8).collect();
            assert_eq!(hamming_distance(&a, &b), scalar(&a, &b), "len={len}");
        }
    }
}
