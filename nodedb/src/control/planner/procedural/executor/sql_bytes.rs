// SPDX-License-Identifier: BUSL-1.1

//! Low-level byte-oriented helpers for SQL string manipulation.

/// Advance past ASCII whitespace characters starting at position `i`.
pub(super) fn skip_ascii_whitespace(bytes: &[u8], mut i: usize) -> usize {
    while let Some(byte) = bytes.get(i) {
        if !byte.is_ascii_whitespace() {
            break;
        }
        i += 1;
    }
    i
}
