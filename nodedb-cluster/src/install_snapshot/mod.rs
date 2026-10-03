// SPDX-License-Identifier: BUSL-1.1

//! Chunked `InstallSnapshot` transport — leader-side sender and follower-side receiver.
//!
//! # Module layout
//!
//! - [`sender`] — leader chunked send loop; slices snapshot bytes into framed
//!   RPC chunks and emits one `InstallSnapshotRequest` per chunk.
//! - [`receiver`] — follower `PartialSnapshotState` accumulator; writes chunk
//!   bytes to `<data_dir>/recv_snapshots/<group_id>.partial` and validates
//!   the running CRC.
//! - [`finalize`] — CRC-full validation, staging, host apply, Raft log
//!   boundary advance, removal of the staged file.
//! - [`staged`] — the staged-install file that marks an install in progress.
//! - [`recover`] — boot completion of staged installs.
//! - [`gc`] — orphan `.partial` file sweeper; removes stale partials that
//!   predate `orphan_partial_max_age_secs`.

pub mod finalize;
pub mod gc;
pub mod receiver;
pub mod recover;
pub mod sender;
pub mod staged;
pub mod state;

pub use gc::sweep_orphans;
pub use receiver::{ChunkOutcome, PartialSnapshotMap, handle_chunk};
pub use recover::recover_staged_installs;
pub use sender::{SendChunkedParams, send_chunked};
pub use staged::StagedInstall;
pub use state::PartialSnapshotState;
