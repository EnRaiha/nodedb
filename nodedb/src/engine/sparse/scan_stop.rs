// SPDX-License-Identifier: BUSL-1.1

//! Cooperative stop signal for long collection scans.
//!
//! A scan loop consults the signal between rows and ends the scan when it
//! returns `true`. The scan itself reports nothing about why it stopped — the
//! caller owns the signal, so the caller already knows, and the caller decides
//! whether the short result is an answer or an error. A scan that stops early
//! must never be reported to a client as a complete result.

/// Stop signal for a scan that has no reason to end early.
///
/// Pass `&never_stop` where the caller enforces no deadline.
pub fn never_stop() -> bool {
    false
}
