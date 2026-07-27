// SPDX-License-Identifier: BUSL-1.1

//! Process-wide panic reporting that never renders application panic payloads.

use std::sync::OnceLock;

static PANIC_HOOK: OnceLock<()> = OnceLock::new();
const PANIC_DIAGNOSTIC: &[u8] = b"nodedb: process panic intercepted\n";

#[cfg(unix)]
fn report_panic() {
    // SAFETY: the byte slice is valid for the duration of the call and
    // `STDERR_FILENO` is the conventional process stderr descriptor. `write`
    // is allocation-free; failures and partial writes are intentionally
    // ignored because panic reporting must never trigger another panic.
    let _ = unsafe {
        libc::write(
            libc::STDERR_FILENO,
            PANIC_DIAGNOSTIC.as_ptr().cast(),
            PANIC_DIAGNOSTIC.len(),
        )
    };
}

#[cfg(not(unix))]
fn report_panic() {
    use std::io::Write as _;

    // The portable fallback performs no formatting and ignores every I/O
    // failure. NodeDB's supported production targets use the Unix path above.
    let _ = std::io::stderr().write_all(PANIC_DIAGNOSTIC);
}

/// Install the payload-redacting process panic hook exactly once.
///
/// The hook intentionally neither chains the default hook nor examines the
/// panic payload or source location. Connection-level boundaries handle
/// expected wire panics; this fixed diagnostic is the last-resort report for
/// every other task.
pub fn install() {
    PANIC_HOOK.get_or_init(|| {
        std::panic::set_hook(Box::new(|_| report_panic()));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_install_state_is_idempotent_without_replacing_global_hook() {
        let state = OnceLock::new();
        assert!(state.set(()).is_ok());
        assert!(state.set(()).is_err());
    }

    #[test]
    fn diagnostic_is_fixed_and_payload_free() {
        assert_eq!(PANIC_DIAGNOSTIC, b"nodedb: process panic intercepted\n");
        assert!(
            !PANIC_DIAGNOSTIC
                .windows(b"secret panic payload".len())
                .any(|window| window == b"secret panic payload")
        );
    }
}
