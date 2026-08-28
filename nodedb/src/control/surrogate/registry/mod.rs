// SPDX-License-Identifier: BUSL-1.1

//! `SurrogateRegistry`, statically split into `LocalCounter`
//! (single-node) and `ClusterCounter` (cross-node HiLo) allocation.

mod cluster;
mod consts;
mod error;
mod local;
mod mode;
mod store;
mod watermark;

pub use cluster::ClusterCounter;
pub use consts::{FLUSH_ELAPSED_THRESHOLD, FLUSH_OPS_THRESHOLD, RESERVE_BATCH_SIZE};
pub use error::{SurrogateAllocError, SurrogatePromotionError};
pub use local::LocalCounter;
pub use mode::SurrogateRegistryMode;
pub use store::SurrogateRegistry;
