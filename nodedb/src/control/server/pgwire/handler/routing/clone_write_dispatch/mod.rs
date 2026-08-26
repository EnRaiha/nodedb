// SPDX-License-Identifier: BUSL-1.1

//! Clone CoW write-path interception for the pgwire handler.
//!
//! Hooked into `dispatch_task_loop` before `dispatch_task`. For a write against a
//! `Shadowed`/`Materializing` clone, applies copy-up/tombstone so the source is
//! never modified. `entry` routes by plan shape; `document`/`kv`/`kv_insert` hold each engine's protocol.

mod document;
mod entry;
mod kv;
mod kv_insert;
mod probes;
mod util;

pub(super) use entry::CloneWriteOutcome;
