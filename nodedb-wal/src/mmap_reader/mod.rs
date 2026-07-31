// SPDX-License-Identifier: Apache-2.0

//! Memory-mapped WAL segment reading for Event Plane catchup.

pub mod reader;
pub mod replay;

pub use reader::{MmapRecordIter, MmapWalReader, observability};
pub use replay::{replay_segments_mmap, replay_segments_mmap_limit};
