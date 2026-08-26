// SPDX-License-Identifier: BUSL-1.1

//! Clone CoW write-path interception for the pgwire handler.
//!
//! Hooked into `dispatch_task_loop` before the normal "dispatch_task" call.
//! For any write targeting a `Shadowed` or `Materializing` clone, applies the
//! copy-up / tombstone protocol so the source database is never modified and
//! no source row survives the write that superseded it.
//!
//! Non-cloned collections and `Materialized` clones return `None` — zero overhead.
//!
//! `entry` is the single hooked-in interception point that routes by plan
//! shape; `document` and `kv` each hold one engine's copy-up/tombstone
//! protocol, with `kv_insert` holding the KV insert-side suppression;
//! `probes` holds the shared Data-Plane read helpers both engines use to check
//! row/key presence and fetch source state; `util` holds small
//! response/error-shaping helpers.

mod document;
mod entry;
mod kv;
mod kv_insert;
mod probes;
mod util;

pub(super) use entry::CloneWriteOutcome;
