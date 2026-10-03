// SPDX-License-Identifier: BUSL-1.1

//! Garbage collection of archived WAL segments no remaining base needs.
//!
//! A segment is named by its first LSN, so its last LSN is below the next
//! segment's first LSN. A segment is collectable only when a later archived
//! segment starts at or below the floor: then every record it holds is below
//! the floor. The segment holding the floor itself is always kept.

use tracing::info;

use super::node_life::NodeLife;
use crate::storage::cold::ColdStorage;
use crate::types::Lsn;

/// First LSNs of the segments wholly below `floor`, ascending.
///
/// `first_lsns` must be ascending. The result is always a prefix of it, so the
/// segments left form an unbroken suffix of the archive.
pub fn collectable_segments(first_lsns: &[u64], floor: Lsn) -> Vec<u64> {
    first_lsns
        .windows(2)
        .take_while(|pair| pair[1] <= floor.as_u64())
        .map(|pair| pair[0])
        .collect()
}

/// Delete every archived segment of `life` wholly below `floor`, oldest first.
///
/// Stops at the first failed delete, so the archive keeps an unbroken suffix.
/// Returns the number of segments deleted.
pub async fn collect_archived_wal(
    cold: &ColdStorage,
    life: &NodeLife,
    floor: Lsn,
) -> crate::Result<u64> {
    let incarnation = life.incarnation.as_str();
    let archived = cold
        .archived_wal_segments(life.node_id, incarnation, 0)
        .await?;
    let mut first_lsns: Vec<u64> = archived.keys().copied().collect();
    first_lsns.sort_unstable();

    let mut collected = 0;
    for first_lsn in collectable_segments(&first_lsns, floor) {
        let markers = archived
            .get(&first_lsn)
            .map_or(&[][..], |remote| remote.crc32c.as_slice());
        cold.delete_archived_wal_segment(life.node_id, incarnation, first_lsn, markers)
            .await?;
        collected += 1;
    }
    if collected > 0 {
        info!(
            node_id = life.node_id,
            floor = floor.as_u64(),
            collected,
            "archived WAL below the oldest kept base collected"
        );
    }
    Ok(collected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_segments_wholly_below_the_floor_are_collectable() {
        let segments = [1, 100, 200, 300, 400];
        // 250 lies in the segment starting at 200: it and all above stay.
        assert_eq!(collectable_segments(&segments, Lsn::new(250)), [1, 100]);
        // A floor on a segment's first LSN keeps that segment.
        assert_eq!(collectable_segments(&segments, Lsn::new(200)), [1, 100]);
        assert_eq!(collectable_segments(&segments, Lsn::new(199)), [1]);
    }

    #[test]
    fn the_newest_segment_is_never_collectable() {
        assert!(collectable_segments(&[5], Lsn::new(u64::MAX)).is_empty());
        assert_eq!(collectable_segments(&[5, 9], Lsn::new(u64::MAX)), [5]);
        assert!(collectable_segments(&[], Lsn::new(10)).is_empty());
    }

    #[test]
    fn a_floor_below_the_archive_collects_nothing() {
        assert!(collectable_segments(&[100, 200], Lsn::new(50)).is_empty());
    }
}
