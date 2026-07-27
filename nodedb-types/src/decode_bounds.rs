// SPDX-License-Identifier: Apache-2.0

//! Dependency-neutral allocation bounds for untrusted binary decoders.

/// Validate a decoded element count before reserving its output container.
///
/// The encoded frame must contain at least `min_encoded_bytes_per_item` bytes
/// for every declared item. Both that proof and the output allocation use
/// checked multiplication so 32-bit targets cannot wrap either calculation.
/// Returns the approved element capacity on success.
pub fn checked_decode_capacity(
    count: usize,
    element_size: usize,
    remaining_encoded_bytes: usize,
    min_encoded_bytes_per_item: usize,
    max_count: usize,
    max_allocation_bytes: usize,
) -> Option<usize> {
    if count > max_count {
        return None;
    }
    let allocation_bytes = count.checked_mul(element_size)?;
    if allocation_bytes > max_allocation_bytes {
        return None;
    }
    let minimum_encoded_bytes = count.checked_mul(min_encoded_bytes_per_item)?;
    if minimum_encoded_bytes > remaining_encoded_bytes {
        return None;
    }
    Some(count)
}

#[cfg(test)]
mod tests {
    use super::checked_decode_capacity;

    #[test]
    fn accepts_count_within_all_bounds() {
        assert_eq!(checked_decode_capacity(2, 8, 12, 4, 3, 24), Some(2));
    }

    #[test]
    fn accepts_zero_count_without_encoded_bytes() {
        assert_eq!(
            checked_decode_capacity(0, usize::MAX, 0, usize::MAX, 0, 0),
            Some(0)
        );
    }

    #[test]
    fn accepts_exact_allocation_and_encoded_limits() {
        assert_eq!(checked_decode_capacity(3, 8, 12, 4, 3, 24), Some(3));
    }

    #[test]
    fn rejects_count_limit() {
        assert_eq!(checked_decode_capacity(4, 8, 16, 4, 3, 32), None);
    }

    #[test]
    fn rejects_allocation_limit() {
        assert_eq!(checked_decode_capacity(3, 8, 12, 4, 3, 23), None);
    }

    #[test]
    fn rejects_insufficient_encoded_bytes() {
        assert_eq!(checked_decode_capacity(3, 8, 11, 4, 3, 24), None);
    }

    #[test]
    fn rejects_output_multiplication_overflow() {
        assert_eq!(
            checked_decode_capacity(usize::MAX, 2, usize::MAX, 0, usize::MAX, usize::MAX),
            None
        );
    }

    #[test]
    fn rejects_input_multiplication_overflow() {
        assert_eq!(
            checked_decode_capacity(usize::MAX, 0, usize::MAX, 2, usize::MAX, usize::MAX),
            None
        );
    }
}
