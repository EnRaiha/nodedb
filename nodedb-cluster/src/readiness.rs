// SPDX-License-Identifier: BUSL-1.1

//! systemd readiness signalling for `Type=notify` units.
//!
//! Every function in this module is **infallible at the API layer**.
//! Internal errors from the platform `sd-notify` crate are logged at
//! `warn` level and then discarded — a failed readiness ping must
//! never crash the server, and in practice the only realistic failure
//! modes are running under `Type=simple` or outside systemd entirely,
//! neither of which is a bug.
//!
//! On non-Linux platforms (macOS, Windows) every call is a logged
//! no-op so the server builds and runs unchanged; `sd-notify` itself
//! is only compiled in on Linux via a target-gated Cargo dependency.

use tracing::debug;
#[cfg(target_os = "linux")]
use tracing::warn;

/// Signal that the server is fully initialised.
///
/// Corresponds to systemd's `READY=1`. Call this exactly once, after
/// every listener has been bound (or at least spawned) and, in
/// cluster mode, after `start_cluster` + `start_raft` have returned
/// successfully.
pub fn notify_ready() {
    #[cfg(target_os = "linux")]
    {
        match sd_notify::notify(false, &[sd_notify::NotifyState::Ready]) {
            Ok(()) => debug!("sd_notify READY=1 sent"),
            Err(e) => warn!(error = %e, "sd_notify READY=1 failed"),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        debug!("sd_notify not supported on this platform; notify_ready is a no-op");
    }
}

/// Update the status string shown by `systemctl status`.
///
/// Corresponds to systemd's `STATUS=<msg>`. Safe to call from any
/// phase — passing a short human-readable string such as
/// `"bootstrapping cluster"`, `"joining via 10.0.0.1:9400"`, or
/// `"ready (3 nodes)"` makes cluster startup visibly progress even
/// while `READY=1` has not yet been sent.
pub fn notify_status(msg: &str) {
    #[cfg(target_os = "linux")]
    {
        match sd_notify::notify(false, &[sd_notify::NotifyState::Status(msg)]) {
            Ok(()) => debug!(status = msg, "sd_notify STATUS sent"),
            Err(e) => warn!(error = %e, status = msg, "sd_notify STATUS failed"),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = msg;
        debug!("sd_notify not supported on this platform; notify_status is a no-op");
    }
}

/// Signal that the server is shutting down.
///
/// Corresponds to systemd's `STOPPING=1`. Safe to call from any
/// graceful-shutdown path.
pub fn notify_stopping() {
    #[cfg(target_os = "linux")]
    {
        match sd_notify::notify(false, &[sd_notify::NotifyState::Stopping]) {
            Ok(()) => debug!("sd_notify STOPPING=1 sent"),
            Err(e) => warn!(error = %e, "sd_notify STOPPING=1 failed"),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        debug!("sd_notify not supported on this platform; notify_stopping is a no-op");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests just verify the calls return cleanly without
    // panicking. Under `cargo test` the harness is not a systemd
    // notify socket, so on Linux `sd_notify` returns an error that is
    // logged and swallowed; on non-Linux the no-op path runs. Either
    // way the functions must be infallible at the API layer.

    #[test]
    fn notify_ready_is_infallible() {
        notify_ready();
    }

    #[test]
    fn notify_status_is_infallible() {
        notify_status("starting test harness");
        notify_status("");
        notify_status("ready (1 node)");
    }

    #[test]
    fn notify_stopping_is_infallible() {
        notify_stopping();
    }
}
