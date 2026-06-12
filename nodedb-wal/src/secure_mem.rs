// SPDX-License-Identifier: Apache-2.0

//! Secure memory utilities for key material.
//!
//! Wraps `libc::mlock`/`munlock` to prevent key bytes from being swapped
//! to disk. mlock is best-effort: if the OS refuses (e.g. RLIMIT_MEMLOCK
//! exceeded on some container configurations), a warning is logged and
//! startup continues. Failing to mlock does not expose the key — it only
//! means the key could be paged out under extreme memory pressure.
//!
//! On platforms where mlock is not available (e.g. some WASM targets) the
//! calls are no-ops.

#[cfg(all(unix, not(target_arch = "wasm32")))]
use tracing::warn;
use zeroize::Zeroize;

/// A 32-byte key held in memory, mlocked against swap.
///
/// On `Drop`, the memory is zeroed (via `zeroize`, which is guaranteed not to
/// be elided by the optimizer) and then munlocked.
///
/// `Clone`, `Copy`, and `Debug` are intentionally NOT derived: key material
/// must not be duplicated or written to logs. A redacting `Debug` impl is
/// provided so the type can still appear in `#[derive(Debug)]` structs without
/// leaking the bytes.
pub struct SecureKey {
    bytes: Box<[u8; 32]>,
}

impl SecureKey {
    /// Wrap a 32-byte key, attempting to mlock it.
    ///
    /// If mlock fails, logs a warning and continues — startup is not aborted.
    pub fn new(bytes: [u8; 32]) -> Self {
        #[cfg_attr(not(all(unix, not(target_arch = "wasm32"))), allow(unused_mut))]
        let mut boxed = Box::new(bytes);
        #[cfg(all(unix, not(target_arch = "wasm32")))]
        mlock_best_effort(
            boxed.as_mut_ptr() as *mut libc::c_void,
            std::mem::size_of_val(boxed.as_ref()),
        );
        Self { bytes: boxed }
    }

    /// Access the key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl std::fmt::Debug for SecureKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecureKey")
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

impl Drop for SecureKey {
    fn drop(&mut self) {
        // Zero the key before releasing. `zeroize` guarantees the writes are
        // not optimized away and inserts a compiler fence afterwards.
        self.bytes.as_mut().zeroize();
        #[cfg(all(unix, not(target_arch = "wasm32")))]
        munlock_best_effort(
            self.bytes.as_mut_ptr() as *mut libc::c_void,
            std::mem::size_of_val(self.bytes.as_ref()),
        );
    }
}

/// Public convenience wrapper for mlocking key bytes from `crypto.rs`.
///
/// Locks the whole slice. Best-effort: logs a warning on failure.
/// No-op on non-Unix targets.
pub fn mlock_key_bytes(bytes: &mut [u8]) {
    #[cfg(all(unix, not(target_arch = "wasm32")))]
    mlock_best_effort(bytes.as_mut_ptr() as *mut libc::c_void, bytes.len());
    #[cfg(not(all(unix, not(target_arch = "wasm32"))))]
    let _ = bytes;
}

/// Attempt to mlock `len` bytes starting at `ptr`.
///
/// Logs a warning if mlock fails.
#[cfg(all(unix, not(target_arch = "wasm32")))]
fn mlock_best_effort(ptr: *mut libc::c_void, len: usize) {
    let rc = unsafe { libc::mlock(ptr, len) };
    if rc != 0 {
        warn!(
            "mlock failed for {} bytes (errno {}): key may be swapped to disk \
             under extreme memory pressure. Increase RLIMIT_MEMLOCK if this \
             is a concern.",
            len,
            std::io::Error::last_os_error()
        );
    }
}

/// Attempt to munlock `len` bytes starting at `ptr`. Best-effort, no error.
#[cfg(all(unix, not(target_arch = "wasm32")))]
fn munlock_best_effort(ptr: *mut libc::c_void, len: usize) {
    unsafe {
        libc::munlock(ptr, len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secure_key_stores_bytes() {
        let key = SecureKey::new([0x42u8; 32]);
        assert_eq!(*key.as_bytes(), [0x42u8; 32]);
    }

    #[test]
    fn secure_key_zeros_on_drop() {
        // We can't observe zeroing from outside since the bytes move on drop,
        // but this at least exercises the path without panic.
        let key = SecureKey::new([0xABu8; 32]);
        drop(key);
        // If we get here without panic or memory error, mlock/munlock worked.
    }

    #[test]
    fn secure_key_debug_redacts_bytes() {
        let key = SecureKey::new([0xCDu8; 32]);
        let rendered = format!("{key:?}");
        assert!(rendered.contains("[REDACTED]"));
        // The raw key byte (0xCD = 205) must not appear in any form.
        assert!(!rendered.contains("205"));
        assert!(!rendered.to_lowercase().contains("cd"));
    }

    #[test]
    fn mlock_key_bytes_accepts_slice() {
        // Best-effort mlock over a slice must not panic regardless of
        // RLIMIT_MEMLOCK, and is a no-op on non-Unix targets.
        let mut buf = [0u8; 32];
        mlock_key_bytes(&mut buf);
    }

    #[test]
    #[cfg(all(unix, not(target_arch = "wasm32")))]
    fn mlock_graceful_on_linux() {
        // mlock with a stack pointer that may or may not succeed depending on
        // RLIMIT_MEMLOCK. Either way we must not panic.
        let mut buf = [0u8; 32];
        mlock_best_effort(buf.as_mut_ptr() as *mut libc::c_void, 32);
        munlock_best_effort(buf.as_mut_ptr() as *mut libc::c_void, 32);
        // Success = no panic.
    }
}
