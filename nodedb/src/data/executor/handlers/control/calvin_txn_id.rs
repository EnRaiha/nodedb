// SPDX-License-Identifier: BUSL-1.1

//! Synthetic [`TxnId`] derivation for Calvin static-execute staging.
//!
//! Calvin transactions never run `BEGIN`, so they never mint a session
//! `TxnId` from the per-core monotonic counter (`TxnId::new(1)`,
//! `TxnId::new(2)`, ... — see the session-BEGIN allocator). To stage a
//! Calvin write plan into the shared `txn_overlays` map (keyed by `TxnId`),
//! this module derives a synthetic id directly from the write's own
//! `(epoch, position, vshard)` identity — the same triple
//! [`crate::data::executor::core_loop::CoreLoop::execute_calvin_execute_static`]
//! already uses to key `commit_pending`.
//!
//! Collision safety with session-minted ids: bit 63 is always set here and
//! never set by the session allocator (a plain incrementing counter starting
//! at 1), so the two id spaces are disjoint by construction — not by
//! magnitude coincidence.
//!
//! Bit layout (MSB to LSB), 64 bits total:
//! - bit 63: reserved marker, always `1`
//! - bits 62..30 (33 bits): `epoch`
//! - bits 29..10 (20 bits): `position`
//! - bits 9..0 (10 bits): `vshard`
//!
//! Any field that does not fit its allotted width is REJECTED with a typed
//! error rather than silently truncated — silent truncation would let two
//! distinct `(epoch, position, vshard)` triples collide on the same
//! synthetic `TxnId` and corrupt each other's staged overlay.

use crate::types::TxnId;

const VSHARD_BITS: u32 = 10;
const POSITION_BITS: u32 = 20;
const EPOCH_BITS: u32 = 33;

const VSHARD_MAX: u32 = 1 << VSHARD_BITS;
const POSITION_MAX: u32 = 1 << POSITION_BITS;
const EPOCH_MAX: u64 = 1 << EPOCH_BITS;

const RESERVED_MARKER: u64 = 1 << 63;

/// Derive a synthetic, collision-free [`TxnId`] for a Calvin static-execute
/// write plan from its `(epoch, position, vshard)` identity.
///
/// Returns `Err` (rather than truncating) if any field exceeds its allotted
/// bit width, since a truncated field would silently alias a different
/// transaction's synthetic id.
pub(in crate::data::executor) fn calvin_synthetic_txn_id(
    epoch: u64,
    position: u32,
    vshard: u32,
) -> crate::Result<TxnId> {
    if epoch >= EPOCH_MAX {
        return Err(crate::Error::Internal {
            detail: format!(
                "calvin synthetic txn id: epoch {epoch} exceeds the {EPOCH_BITS}-bit range"
            ),
        });
    }
    if position >= POSITION_MAX {
        return Err(crate::Error::Internal {
            detail: format!(
                "calvin synthetic txn id: position {position} exceeds the \
                 {POSITION_BITS}-bit range"
            ),
        });
    }
    if vshard >= VSHARD_MAX {
        return Err(crate::Error::Internal {
            detail: format!(
                "calvin synthetic txn id: vshard {vshard} exceeds the {VSHARD_BITS}-bit range"
            ),
        });
    }

    let epoch_field = epoch << (POSITION_BITS + VSHARD_BITS);
    let position_field = u64::from(position) << VSHARD_BITS;
    let vshard_field = u64::from(vshard);

    Ok(TxnId::new(
        RESERVED_MARKER | epoch_field | position_field | vshard_field,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sets_the_reserved_marker_bit() {
        let id = calvin_synthetic_txn_id(1, 2, 3).unwrap();
        assert_eq!(id.as_u64() & RESERVED_MARKER, RESERVED_MARKER);
    }

    #[test]
    fn distinct_triples_map_to_distinct_ids() {
        let cases: &[(u64, u32, u32)] = &[
            (0, 0, 0),
            (1, 0, 0),
            (0, 1, 0),
            (0, 0, 1),
            (42, 7, 3),
            (42, 3, 7),
            (7, 42, 3),
            (EPOCH_MAX - 1, POSITION_MAX - 1, VSHARD_MAX - 1),
        ];
        let mut ids = Vec::with_capacity(cases.len());
        for &(epoch, position, vshard) in cases {
            ids.push(calvin_synthetic_txn_id(epoch, position, vshard).unwrap());
        }
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                assert_ne!(
                    ids[i], ids[j],
                    "triples {:?} and {:?} collided on {:?}",
                    cases[i], cases[j], ids[i]
                );
            }
        }
    }

    #[test]
    fn out_of_range_epoch_is_rejected() {
        assert!(calvin_synthetic_txn_id(EPOCH_MAX, 0, 0).is_err());
    }

    #[test]
    fn out_of_range_position_is_rejected() {
        assert!(calvin_synthetic_txn_id(0, POSITION_MAX, 0).is_err());
    }

    #[test]
    fn out_of_range_vshard_is_rejected() {
        assert!(calvin_synthetic_txn_id(0, 0, VSHARD_MAX).is_err());
    }

    #[test]
    fn boundary_values_are_accepted() {
        assert!(calvin_synthetic_txn_id(EPOCH_MAX - 1, POSITION_MAX - 1, VSHARD_MAX - 1).is_ok());
    }
}
