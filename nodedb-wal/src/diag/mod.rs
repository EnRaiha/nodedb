// SPDX-License-Identifier: Apache-2.0

//! Black-box recorder wiring for the WAL.
//!
//! The failures that matter most in a log — a hole in a segment, a missing
//! segment, a writer poisoned by a failed fsync — are *returned errors*, not
//! panics, so nothing installs a hook for them and they reach an operator as a
//! log line with no context. These entry points file a structured report at the
//! site that detected the failure, while the damaged bytes are still on disk.
//!
//! One report per root cause: a report is filed where the invariant is known
//! and nowhere else. Layers above add breadcrumbs and propagate the error —
//! emitting again on the way up would file several unrelated-looking reports
//! for a single failure, since each would carry its own fingerprint.
//!
//! This crate never calls `faultbox::init`. Choosing a reports directory and a
//! redactor belongs to the binary; everything here is inert until the host
//! application initializes the recorder, so a library emitting these costs
//! nothing on its own. The real implementation compiles only under the
//! `diagnostics` feature and off wasm32 (where there is no filesystem to write
//! a report to); otherwise every entry point is a no-op with the same
//! signature, so call sites never need a `cfg`.

#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
mod context;
#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
mod recording;

#[cfg(not(all(feature = "diagnostics", not(target_arch = "wasm32"))))]
mod inert;

#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
pub use recording::{
    durability_lost, encrypted_record_without_key, mid_file_corruption, out_of_space,
    replay_below_retained_floor, segment_lsn_gap,
};

#[cfg(not(all(feature = "diagnostics", not(target_arch = "wasm32"))))]
pub use inert::{
    durability_lost, encrypted_record_without_key, mid_file_corruption, out_of_space,
    replay_below_retained_floor, segment_lsn_gap,
};
