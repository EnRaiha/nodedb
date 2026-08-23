// SPDX-License-Identifier: BUSL-1.1

pub mod coordinator;
pub mod gather;
pub mod geofence;
pub mod merge;
pub mod shard_routing;

pub use coordinator::SpatialScatterGather;
pub use gather::{DEFAULT_GATHER_TIMEOUT, MergedSpatialHits, SpatialGatherError};
pub use geofence::GeofenceRegistry;
pub use merge::{ShardSpatialResult, SpatialResultMerger};
pub use shard_routing::ShardSpatialExtent;
