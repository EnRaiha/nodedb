// SPDX-License-Identifier: BUSL-1.1

//! Process-wide panic reporting that never renders application panic
//! payloads in a release build.

use std::panic::PanicHookInfo;
use std::sync::OnceLock;

static PANIC_HOOK: OnceLock<()> = OnceLock::new();
const PANIC_DIAGNOSTIC: &[u8] = b"nodedb: process panic intercepted\n";

#[cfg(unix)]
fn report_fixed_diagnostic() {
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
fn report_fixed_diagnostic() {
    use std::io::Write as _;

    // The portable fallback performs no formatting and ignores every I/O
    // failure. NodeDB's supported production targets use the Unix path above.
    let _ = std::io::stderr().write_all(PANIC_DIAGNOSTIC);
}

/// The panic payload as text, when it is a string.
fn payload_text<'a>(info: &'a PanicHookInfo<'_>) -> Option<&'a str> {
    let payload = info.payload();
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
}

/// Report where the panic happened, after the fixed diagnostic.
///
/// - The source location names code, never data, so every build reports it.
/// - The payload can carry row values or keys. Only a debug build reports
///   it, so a release server never writes application data to stderr.
/// - The backtrace is captured only when `RUST_BACKTRACE` enables it.
///
/// Every I/O error is ignored: panic reporting must never panic.
fn report_panic(info: &PanicHookInfo<'_>) {
    use std::io::Write as _;

    report_fixed_diagnostic();
    let mut stderr = std::io::stderr().lock();
    if let Some(location) = info.location() {
        let _ = writeln!(
            stderr,
            "nodedb: panic at {}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
    }
    if cfg!(debug_assertions)
        && let Some(message) = payload_text(info)
    {
        let _ = writeln!(stderr, "nodedb: panic message: {message}");
    }
    let backtrace = std::backtrace::Backtrace::capture();
    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        let _ = writeln!(stderr, "nodedb: panic backtrace:\n{backtrace}");
    }
}

/// Install the process panic hook exactly once.
///
/// The hook does not chain the default hook. Connection-level boundaries
/// handle expected wire panics. This report is the last resort for every
/// other task: the fixed diagnostic, the source location, the payload in a
/// debug build only, and the backtrace when it is enabled.
pub fn install() {
    PANIC_HOOK.get_or_init(|| {
        std::panic::set_hook(Box::new(report_panic));
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
