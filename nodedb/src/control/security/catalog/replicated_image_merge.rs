// SPDX-License-Identifier: BUSL-1.1

//! Merges for replicated tables that also take this node's own writes.
//!
//! An install replaces most tables with the image's rows. The tables below
//! also hold values this node writes outside group 0 apply, and each such
//! value is monotonic. Taking the image's row can move it down and reissue
//! an id, a sequence number, or a fencing epoch. So the install keeps, per
//! row, the higher of the local and the image value:
//! - `surrogate_hwm`: the surrogate assigner flushes this node's watermark.
//! - `sync_producer_hwm`: the producer registry flushes this node's
//!   allocator on every registration.
//! - `sync_producers`: registration and fencing write this node's row before
//!   they propose it; the fencing epoch never moves down.
//! - `metadata`: the credential store saves `next_user_id`.
//! - `tenant_id_hwm`: tenant creation allocates the next id on this node.
//! - `topics_ep`: publishing advances a topic's last sequence and position.
//!
//! Two more tables take local writes that are not counters:
//! - `sync_peer_bindings`: a sync session claims a peer id on this node
//!   before the claim replicates. Apply keeps the lower producer id, and the
//!   merge keeps it too. A claim only this node holds stays until it commits.
//! - `sequence_state`: a GAP_FREE reservation logs its outcome as a `log:`
//!   row on the node that ran it. Log rows are node-local, so the merge keeps
//!   this node's log rows and drops the image's. Counter rows follow the
//!   image: on a cluster node only apply writes them, and `RESTART` lowers
//!   them on purpose.
//!
//! `database_hwm`, `surrogate_reserve_index`, and `custom_type_oid_hwm` are
//! written only by group 0 apply on a cluster node. They merge the same way,
//! so no install moves a watermark down even if a local writer appears.

use std::collections::BTreeMap;

use super::replicated_image::RawRows;
use super::sync_producer::{StoredPeerBinding, StoredProducerRegistration};
use super::types::catalog_err;
use crate::event::topic::TopicDef;

/// Merge `local` rows with `image` rows into the rows the table holds after
/// the install.
pub(super) type MergeFn = fn(&RawRows, &RawRows) -> crate::Result<RawRows>;

/// The merge for `label`, or `None` when the image replaces the table.
pub(super) fn merge_for(label: &str) -> Option<MergeFn> {
    let merge: MergeFn = match label {
        "surrogate_hwm" | "custom_type_oid_hwm" => max_u32_rows,
        "sync_producer_hwm" | "database_hwm" | "surrogate_reserve_index" | "tenant_id_hwm" => {
            max_u64_rows
        }
        "metadata" => merge_metadata,
        "sync_producers" => merge_producers,
        "topics_ep" => merge_topics,
        "sync_peer_bindings" => merge_peer_bindings,
        "sequence_state" => merge_sequence_state,
        _ => return None,
    };
    Some(merge)
}

fn fixed<const N: usize>(bytes: &[u8], what: &str) -> crate::Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| catalog_err("replicated image merge", format!("{what}: bad width")))
}

/// Every key of `local` or `image`, each value the higher of the two.
fn max_rows(
    local: &RawRows,
    image: &RawRows,
    higher: fn(&[u8], &[u8]) -> crate::Result<Vec<u8>>,
) -> crate::Result<RawRows> {
    max_rows_with(local, image, higher)
}

fn higher_u32(a: &[u8], b: &[u8]) -> crate::Result<Vec<u8>> {
    let a = u32::from_le_bytes(fixed(a, "u32 counter")?);
    let b = u32::from_le_bytes(fixed(b, "u32 counter")?);
    Ok(a.max(b).to_le_bytes().to_vec())
}

fn higher_u64(a: &[u8], b: &[u8]) -> crate::Result<Vec<u8>> {
    let a = u64::from_le_bytes(fixed(a, "u64 counter")?);
    let b = u64::from_le_bytes(fixed(b, "u64 counter")?);
    Ok(a.max(b).to_le_bytes().to_vec())
}

fn max_u32_rows(local: &RawRows, image: &RawRows) -> crate::Result<RawRows> {
    max_rows(local, image, higher_u32)
}

fn max_u64_rows(local: &RawRows, image: &RawRows) -> crate::Result<RawRows> {
    max_rows(local, image, higher_u64)
}

/// The image's rows, with `next_user_id` the higher of the two.
fn merge_metadata(local: &RawRows, image: &RawRows) -> crate::Result<RawRows> {
    const NEXT_USER_ID: &[u8] = b"next_user_id";
    let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = image.iter().cloned().collect();
    if let Some((_, local_next)) = local.iter().find(|(key, _)| key == NEXT_USER_ID) {
        let kept = match merged.get(NEXT_USER_ID) {
            Some(from_image) => higher_u64(local_next, from_image)?,
            None => local_next.clone(),
        };
        merged.insert(NEXT_USER_ID.to_vec(), kept);
    }
    Ok(merged.into_iter().collect())
}

/// The image's registrations, each fencing epoch the higher of the two.
/// A registration only this node holds stays: its proposal can still
/// commit, and its producer id is already spent.
fn merge_producers(local: &RawRows, image: &RawRows) -> crate::Result<RawRows> {
    let decode = |bytes: &[u8]| -> crate::Result<StoredProducerRegistration> {
        zerompk::from_msgpack(bytes).map_err(|e| catalog_err("decode producer registration", e))
    };
    max_rows_with(local, image, |local_value, image_value| {
        let local_row = decode(local_value)?;
        let mut row = decode(image_value)?;
        row.current_epoch = row.current_epoch.max(local_row.current_epoch);
        zerompk::to_msgpack_vec(&row).map_err(|e| catalog_err("encode producer registration", e))
    })
}

/// The image's bindings, each key owned by the lower producer id of the two.
/// A claim only this node holds stays: its proposal can still commit.
fn merge_peer_bindings(local: &RawRows, image: &RawRows) -> crate::Result<RawRows> {
    let decode = |bytes: &[u8]| -> crate::Result<StoredPeerBinding> {
        zerompk::from_msgpack(bytes).map_err(|e| catalog_err("decode peer binding", e))
    };
    max_rows_with(local, image, |local_value, image_value| {
        let local_owner = decode(local_value)?;
        let image_owner = decode(image_value)?;
        Ok(if local_owner.producer_id < image_owner.producer_id {
            local_value.to_vec()
        } else {
            image_value.to_vec()
        })
    })
}

/// Whether a `sequence_state` key names a GAP_FREE log row. The key is
/// `"{database_id}:{tenant_id}:{name}"`, and a log row's name starts with
/// `log:`, which no SQL identifier produces.
fn is_sequence_log_key(key: &[u8]) -> bool {
    let mut parts = key.splitn(3, |byte| *byte == b':');
    let _database = parts.next();
    let _tenant = parts.next();
    parts.next().is_some_and(|name| name.starts_with(b"log:"))
}

/// The image's counter rows plus this node's own log rows.
fn merge_sequence_state(local: &RawRows, image: &RawRows) -> crate::Result<RawRows> {
    let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = image
        .iter()
        .filter(|(key, _)| !is_sequence_log_key(key))
        .cloned()
        .collect();
    merged.extend(
        local
            .iter()
            .filter(|(key, _)| is_sequence_log_key(key))
            .cloned(),
    );
    Ok(merged.into_iter().collect())
}

/// The image's topics, each keeping the higher last sequence and the later
/// position of the two. A topic the image lacks was dropped and goes.
fn merge_topics(local: &RawRows, image: &RawRows) -> crate::Result<RawRows> {
    let decode = |bytes: &[u8]| -> crate::Result<TopicDef> {
        zerompk::from_msgpack(bytes).map_err(|e| catalog_err("decode topic", e))
    };
    let local: BTreeMap<&[u8], &[u8]> = local
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    image
        .iter()
        .map(|(key, image_value)| {
            let Some(&local_value) = local.get(key.as_slice()) else {
                return Ok((key.clone(), image_value.clone()));
            };
            let local_topic = decode(local_value)?;
            let mut topic = decode(image_value.as_slice())?;
            topic.last_sequence = topic.last_sequence.max(local_topic.last_sequence);
            if (local_topic.last_epoch, local_topic.last_lsn) > (topic.last_epoch, topic.last_lsn) {
                topic.last_epoch = local_topic.last_epoch;
                topic.last_lsn = local_topic.last_lsn;
            }
            let bytes =
                zerompk::to_msgpack_vec(&topic).map_err(|e| catalog_err("encode topic", e))?;
            Ok((key.clone(), bytes))
        })
        .collect()
}

/// Every key of `local` or `image`. A key in both takes
/// `combine(local, image)`.
fn max_rows_with(
    local: &RawRows,
    image: &RawRows,
    combine: impl Fn(&[u8], &[u8]) -> crate::Result<Vec<u8>>,
) -> crate::Result<RawRows> {
    let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = image.iter().cloned().collect();
    for (key, value) in local {
        let kept = match merged.get(key) {
            Some(from_image) => combine(value.as_slice(), from_image.as_slice())?,
            None => value.clone(),
        };
        merged.insert(key.clone(), kept);
    }
    Ok(merged.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, value: u64) -> (Vec<u8>, Vec<u8>) {
        (key.as_bytes().to_vec(), value.to_le_bytes().to_vec())
    }

    #[test]
    fn a_counter_never_moves_down() {
        let merged = max_u64_rows(&vec![row("global", 9)], &vec![row("global", 4)]).unwrap();
        assert_eq!(merged, vec![row("global", 9)]);
        let merged = max_u64_rows(&vec![row("global", 2)], &vec![row("global", 4)]).unwrap();
        assert_eq!(merged, vec![row("global", 4)]);
    }

    #[test]
    fn next_user_id_keeps_the_higher_and_other_rows_follow_the_image() {
        let local = vec![row("next_user_id", 12), row("stale", 1)];
        let image = vec![row("next_user_id", 7), row("fresh", 2)];
        let merged = merge_metadata(&local, &image).unwrap();
        assert_eq!(merged, vec![row("fresh", 2), row("next_user_id", 12)]);
    }

    #[test]
    fn a_fencing_epoch_never_moves_down() {
        let reg = |epoch: u64| StoredProducerRegistration {
            producer_id: 3,
            current_epoch: epoch,
            tenant_id: 1,
            user_id: 1,
            created_ms: 0,
        };
        let enc = |r: &StoredProducerRegistration| zerompk::to_msgpack_vec(r).unwrap();
        let key = b"lite-a".to_vec();
        let merged = merge_producers(
            &vec![(key.clone(), enc(&reg(5)))],
            &vec![(key.clone(), enc(&reg(2)))],
        )
        .unwrap();
        let kept: StoredProducerRegistration = zerompk::from_msgpack(&merged[0].1).unwrap();
        assert_eq!(kept.current_epoch, 5);
    }

    #[test]
    fn a_peer_binding_keeps_the_lower_producer() {
        let enc = |producer_id: u64| {
            zerompk::to_msgpack_vec(&StoredPeerBinding {
                producer_id,
                bound_ms: 0,
            })
            .unwrap()
        };
        let key = b"peer".to_vec();
        let pending = b"pending".to_vec();
        let merged = merge_peer_bindings(
            &vec![(key.clone(), enc(4)), (pending.clone(), enc(9))],
            &vec![(key.clone(), enc(7))],
        )
        .unwrap();
        assert_eq!(merged, vec![(key, enc(4)), (pending, enc(9))]);
    }

    #[test]
    fn sequence_log_rows_stay_node_local() {
        let local = vec![row("2:1:log:s:10:committed", 1), row("2:1:s", 50)];
        let image = vec![row("2:1:log:s:11:committed", 2), row("2:1:s", 40)];
        let merged = merge_sequence_state(&local, &image).unwrap();
        assert_eq!(
            merged,
            vec![row("2:1:log:s:10:committed", 1), row("2:1:s", 40)]
        );
    }
}
