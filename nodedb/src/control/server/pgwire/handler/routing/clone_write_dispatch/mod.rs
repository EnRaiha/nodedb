// SPDX-License-Identifier: BUSL-1.1

//! Clone CoW write-path interception for the pgwire handler.
//!
//! Hooked into `dispatch_task_loop` before the normal "dispatch_task" call.
//! For `PointUpdate` and `PointDelete` targeting a `Shadowed` or `Materializing`
//! clone, applies the copy-up / tombstone protocol so the source database is
//! never modified.
//!
//! Non-cloned collections and `Materialized` clones return `None` — zero overhead.
//!
//! `entry` is the single hooked-in interception point that routes by plan
//! shape; `document` and `kv` each hold one engine's copy-up/tombstone
//! protocol; `probes` holds the shared Data-Plane read helpers both engines
//! use to check row/key presence and fetch source state; `util` holds small
//! response/error-shaping helpers.

mod document;
mod entry;
mod kv;
mod probes;
mod util;

pub(super) use entry::CloneWriteOutcome;
