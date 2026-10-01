// SPDX-License-Identifier: Apache-2.0

//! Restore-point payload.
//!
//! A cluster restore point names one consistent instant across every Raft
//! group. Each node appends one `RestorePoint` record per group it hosts, at
//! the group's place in its log. A cluster point-in-time restore reads them to
//! restart every group at that place.
//!
//! Payload layout (little-endian): a fixed 56-byte head, then the vShards
//! the group homed at the point.
//!
//! ```text
//! ┌──────┬──────┬──────────┬───────────────┬──────┬────────────┬─────────────────┬───────┬────────────┐
//! │ id   │ hlc  │ group_id │ applied_index │ term │ next_epoch │ epoch_system_ms │ count │ vshard × n │
//! │ u64  │ u64  │ u64      │ u64           │ u64  │ u64        │ u64             │ u32   │ u32 each   │
//! └──────┴──────┴──────────┴───────────────┴──────┴────────────┴─────────────────┴───────┴────────────┘
//! ```

use crate::error::{Result, WalError};

/// Size of a restore-point payload's fixed head on disk.
pub const RESTORE_POINT_PAYLOAD_SIZE: usize = 56;

/// One group's place at a restore point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePointPayload {
    /// The restore point's id, shared by every group and node.
    pub id: u64,
    /// The point's watermark: HLC wall time in nanoseconds. A restore keeps
    /// a record only when its commit HLC is below it.
    pub hlc: u64,
    pub group_id: u64,
    /// The group's log index at the point. Every entry at or below it applied
    /// before the record was appended.
    pub applied_index: u64,
    /// The term of the entry at `applied_index`.
    pub term: u64,
    /// For the Calvin sequencer group, the first epoch the sequencer may
    /// propose after the point. `0` for every other group.
    pub next_epoch: u64,
    /// For the Calvin sequencer group, the highest epoch instant (ms) the
    /// sequencer applied before the point. A restored sequencer mints above
    /// it. `0` for every other group, and for a sequencer that applied no
    /// epoch.
    pub epoch_system_ms: u64,
    /// The vShards the group homed at the point. Empty for the metadata and
    /// sequencer groups. A restore places a record of these vShards that
    /// carries no commit HLC before or after this record in the WAL.
    pub vshards: Vec<u32>,
}

impl RestorePointPayload {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(RESTORE_POINT_PAYLOAD_SIZE + 4 + 4 * self.vshards.len());
        for value in [
            self.id,
            self.hlc,
            self.group_id,
            self.applied_index,
            self.term,
            self.next_epoch,
            self.epoch_system_ms,
        ] {
            buf.extend_from_slice(&value.to_le_bytes());
        }
        let count = u32::try_from(self.vshards.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&count.to_le_bytes());
        for vshard in self.vshards.iter().take(count as usize) {
            buf.extend_from_slice(&vshard.to_le_bytes());
        }
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let refuse = || WalError::InvalidPayload {
            detail: format!(
                "RestorePoint payload of {} bytes is no {RESTORE_POINT_PAYLOAD_SIZE}-byte head, \
                 count and vShard list",
                buf.len()
            ),
        };
        let head = buf.get(..RESTORE_POINT_PAYLOAD_SIZE).ok_or_else(refuse)?;
        let mut fields = [0u64; 7];
        for (field, word) in fields.iter_mut().zip(head.as_chunks::<8>().0) {
            *field = u64::from_le_bytes(*word);
        }
        let [
            id,
            hlc,
            group_id,
            applied_index,
            term,
            next_epoch,
            epoch_system_ms,
        ] = fields;
        let rest = &buf[RESTORE_POINT_PAYLOAD_SIZE..];
        let count: [u8; 4] = rest
            .get(..4)
            .and_then(|b| b.try_into().ok())
            .ok_or_else(refuse)?;
        let count = u32::from_le_bytes(count) as usize;
        let list = &rest[4..];
        if list.len() != count.checked_mul(4).ok_or_else(refuse)? {
            return Err(refuse());
        }
        let vshards = list
            .as_chunks::<4>()
            .0
            .iter()
            .map(|word| u32::from_le_bytes(*word))
            .collect();
        Ok(Self {
            id,
            hlc,
            group_id,
            applied_index,
            term,
            next_epoch,
            epoch_system_ms,
            vshards,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_point_roundtrip() {
        let point = RestorePointPayload {
            id: 3,
            hlc: 1_700_000_000_000_000_000,
            group_id: 7,
            applied_index: 1_234,
            term: 5,
            next_epoch: 0,
            epoch_system_ms: 1_700_000_000_123,
            vshards: vec![4, 9, 1023],
        };
        assert_eq!(
            RestorePointPayload::from_bytes(&point.to_bytes()).unwrap(),
            point
        );
    }

    #[test]
    fn wrong_size_is_refused() {
        assert!(RestorePointPayload::from_bytes(&[0u8; 55]).is_err());
        assert!(
            RestorePointPayload::from_bytes(&[0u8; 56]).is_err(),
            "no count"
        );
        let mut one = vec![0u8; 60];
        one[56] = 1;
        assert!(
            RestorePointPayload::from_bytes(&one).is_err(),
            "a count above the list"
        );
        assert!(RestorePointPayload::from_bytes(&[0u8; 60]).is_ok());
    }
}
