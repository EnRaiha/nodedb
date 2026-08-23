// SPDX-License-Identifier: BUSL-1.1

pub mod coordinator;
pub mod gather;
pub mod merge;
pub mod seam;

pub use coordinator::VectorScatterGather;
pub use gather::{DEFAULT_GATHER_TIMEOUT, MergedTopK, VectorGatherError};
pub use merge::{ShardSearchResult, VectorHit, VectorMerger};
pub use seam::{
    MemoryRegion, ShardMessage, ShardMessageKind, ShardMessageReply, ShardRef, ShardSubset,
    VectorSeamError, VectorShardSeam,
};
