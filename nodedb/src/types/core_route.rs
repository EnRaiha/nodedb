// SPDX-License-Identifier: BUSL-1.1

//! The Data Plane core a vShard lives on.
//!
//! The dispatcher's `VShardRouter` builds its table from this rule, and the
//! Data Plane routes every vShard-owned artifact by it, so both always agree.

use super::VShardId;

/// The core that owns `vshard` on a node with `num_cores` Data Plane cores.
/// A node reporting zero cores routes everything to core 0.
pub fn core_for_vshard(vshard: VShardId, num_cores: usize) -> usize {
    if num_cores == 0 {
        return 0;
    }
    vshard.as_u32() as usize % num_cores
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vshards_spread_round_robin() {
        assert_eq!(core_for_vshard(VShardId::new(0), 4), 0);
        assert_eq!(core_for_vshard(VShardId::new(5), 4), 1);
        assert_eq!(core_for_vshard(VShardId::new(7), 0), 0);
    }
}
