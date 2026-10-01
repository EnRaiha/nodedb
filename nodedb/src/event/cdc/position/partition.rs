// SPDX-License-Identifier: BUSL-1.1

//! Change-feed partitions.
//!
//! A vShard's Raft-applied writes and its Calvin transactions come from two
//! separate replicated logs. Every replica applies each log in order, but the
//! two interleave differently on each replica, so they cannot share one
//! position order. Each log therefore has its own partition: partition `v`
//! carries vShard `v`'s Raft-applied writes, and partition
//! `CALVIN_PARTITION_BASE + v` carries its Calvin transactions.

/// First partition of the Calvin partitions.
pub const CALVIN_PARTITION_BASE: u32 = 1 << 16;

/// The partition of vShard `vshard`'s Calvin transactions.
pub const fn calvin_partition(vshard: u32) -> u32 {
    CALVIN_PARTITION_BASE + vshard
}

/// The vShard a partition belongs to.
pub const fn vshard_of_partition(partition: u32) -> u32 {
    partition % CALVIN_PARTITION_BASE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_calvin_partition_maps_back_to_its_vshard() {
        assert_eq!(vshard_of_partition(calvin_partition(17)), 17);
        assert_eq!(vshard_of_partition(17), 17);
        assert_ne!(calvin_partition(17), 17);
    }
}
