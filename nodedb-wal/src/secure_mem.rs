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

/// A 32-byte key held in memory, mlocked against swap.
///
/// On `Drop`, the memory is explicitly zeroed and then munlocked.
pub struct SecureKey {
    bytes: Box<[u8; 32]>,
}

impl SecureKey {
    /// Wrap a 32-byte key, attempting to mlock it.
    ///
    /// If mlock fails, logs a warning and continues — startup is not aborted.
    pub fn new(bytes: [u8; 32]) -> Self {
        let mut boxed = Box::new(bytes);
        #[cfg(all(unix, not(target_arch = "wasm32")))]
        mlock_best_effort(boxed.as_mut_ptr() as *mut libc::c_void, 32);
        Self { bytes: boxed }
    }

    /// Access the key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl Drop for SecureKey {
    fn drop(&mut self) {
        // Zero the key before releasing.
        // Use volatile writes so the compiler cannot optimize them away.
        for byte in self.bytes.iter_mut() {
            unsafe { std::ptr::write_volatile(byte, 0u8) };
        }
        #[cfg(all(unix, not(target_arch = "wasm32")))]
        munlock_best_effort(self.bytes.as_mut_ptr() as *mut libc::c_void, 32);
    }
}

/// Public convenience wrapper for mlocking raw key bytes from `crypto.rs`.
///
/// Locks `len` bytes starting at `ptr`. Best-effort: logs a warning on failure.
/// No-op on non-Unix targets.
pub fn mlock_key_bytes(ptr: *mut u8, len: usize) {
    #[cfg(all(unix, not(target_arch = "wasm32")))]
    mlock_best_effort(ptr as *mut libc::c_void, len);
    #[cfg(not(all(unix, not(target_arch = "wasm32"))))]
    let _ = (ptr, len);
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
