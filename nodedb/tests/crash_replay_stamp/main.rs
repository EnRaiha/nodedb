// SPDX-License-Identifier: BUSL-1.1

//! A write still in flight when a checkpoint is written survives a crash.
//!
//! LSNs are node-global, and a write reaches its core out of mint order: two
//! writes to different collections apply in any order. Each engine case runs
//! the same sequence:
//!
//! 1. Write A, an `INSERT` into the held collection, mints its LSN and parks at
//!    the funnel gate. No core holds it. A parks inside its apply entry's
//!    enqueue, so no later entry of its data Raft group starts.
//! 2. Writes B, `INSERT`s into a second collection applied in another data
//!    group, apply with higher LSNs until a checkpoint's replay stamp names
//!    one of them above its prefix. That proves the checkpoint was written
//!    while A was minted and not applied.
//! 3. The test releases A. A applies, and the process aborts before A's
//!    response leaves, so no later checkpoint holds A.
//! 4. After restart, replay must apply A: the stamp does not name it. A stamp
//!    that holds only the highest applied LSN skips A, and A's row is lost.
//!
//! An array or timeseries checkpoint writes only a collection that holds
//! unwritten state. Those cases seed the held collection in boot 1 and start
//! boot 2's checkpoints after A is minted, so the checkpoint that names B also
//! writes the held collection.
//!
//! The WAL-truncation case seals segments below A in boot 1, and the segment
//! that holds A's record while A is parked. The filler collection is applied
//! in a third data group, so its writes apply while A is parked too.
//! Truncation must remove segments below A and keep A's segment, and must
//! remove it once A settled after the restart.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

#[path = "../crash_harness/mod.rs"]
mod crash_harness;

mod case;
mod engines;
mod run;
mod truncation;
