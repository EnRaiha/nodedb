// SPDX-License-Identifier: BUSL-1.1

//! Per-topic, per-partition marks of the committed messages a topic holds.
//!
//! A committed transaction's message is delivered to its topic from the
//! change-feed partition of the record that carries it, in position order.
//! The topic keeps the highest position it appended from each partition. A
//! message at or below the mark is one the topic holds already: a delivery
//! repeated after a lease move appends nothing. The mark moves in the same
//! transaction as the append, and goes with the topic when it is dropped.

use redb::{ReadableTable, TableDefinition, WriteTransaction};

use super::types::catalog_err;
use crate::event::cdc::CdcOffset;
use crate::event::topic::types::PublishOrigin;
use crate::types::DatabaseId;

/// Table: `[database_id: be u64][tenant_id: be u64][name_len: be u16]
/// [topic_name bytes][partition: be u32]` -> `[epoch][index][sequence]`, each
/// a big-endian u64.
pub(super) const TOPIC_PUBLISH_MARKS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.topic_publish_marks");

/// The key prefix of one topic's marks.
fn topic_prefix(database_id: DatabaseId, tenant_id: u64, topic: &str) -> crate::Result<Vec<u8>> {
    let name_len: u16 = topic
        .len()
        .try_into()
        .map_err(|_| catalog_err("topic publish mark", "topic name exceeds u16 length"))?;
    let mut key = Vec::with_capacity(22 + topic.len());
    key.extend_from_slice(&database_id.as_u64().to_be_bytes());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(topic.as_bytes());
    Ok(key)
}

fn encode_mark(position: CdcOffset) -> [u8; 24] {
    let mut bytes = [0u8; 24];
    bytes[..8].copy_from_slice(&position.epoch.to_be_bytes());
    bytes[8..16].copy_from_slice(&position.index.to_be_bytes());
    bytes[16..].copy_from_slice(&position.sequence.to_be_bytes());
    bytes
}

fn decode_mark(bytes: &[u8]) -> crate::Result<CdcOffset> {
    let word = |range: std::ops::Range<usize>| -> crate::Result<u64> {
        bytes
            .get(range)
            .and_then(|slice| slice.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| catalog_err("topic publish mark", "malformed mark"))
    };
    if bytes.len() != 24 {
        return Err(catalog_err("topic publish mark", "malformed mark"));
    }
    Ok(CdcOffset::at(word(0..8)?, word(8..16)?, word(16..24)?))
}

/// Raise the topic's mark for `origin`'s partition to `origin`'s position.
/// Returns `false`, and moves nothing, when the topic holds a message at or
/// above that position already.
pub(super) fn claim_origin(
    txn: &WriteTransaction,
    (database_id, tenant_id, topic): (DatabaseId, u64, &str),
    origin: &PublishOrigin,
) -> crate::Result<bool> {
    let mut key = topic_prefix(database_id, tenant_id, topic)?;
    key.extend_from_slice(&origin.partition.to_be_bytes());
    let mut marks = txn
        .open_table(TOPIC_PUBLISH_MARKS)
        .map_err(|e| catalog_err("open topic_publish_marks", e))?;
    let held = marks
        .get(key.as_slice())
        .map_err(|e| catalog_err("read topic publish mark", e))?
        .map(|mark| decode_mark(mark.value()))
        .transpose()?;
    if held.is_some_and(|held| origin.position <= held) {
        return Ok(false);
    }
    marks
        .insert(key.as_slice(), encode_mark(origin.position).as_slice())
        .map_err(|e| catalog_err("write topic publish mark", e))?;
    Ok(true)
}

/// Remove every mark of one topic.
pub(super) fn forget_topic_marks(
    txn: &WriteTransaction,
    database_id: DatabaseId,
    tenant_id: u64,
    topic: &str,
) -> crate::Result<()> {
    let prefix = topic_prefix(database_id, tenant_id, topic)?;
    let mut marks = txn
        .open_table(TOPIC_PUBLISH_MARKS)
        .map_err(|e| catalog_err("open topic_publish_marks", e))?;
    // A topic's marks follow its prefix. A longer topic name that shares the
    // prefix differs in its length bytes, so it never falls in this range.
    let mut end = prefix.clone();
    end.extend_from_slice(&u32::MAX.to_be_bytes());
    marks
        .retain_in(prefix.as_slice()..=end.as_slice(), |_, _| false)
        .map_err(|e| catalog_err("delete topic publish marks", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::SystemCatalog;

    fn origin(partition: u32, index: u64) -> PublishOrigin {
        PublishOrigin {
            partition,
            position: CdcOffset::data_event(0, index, 1),
        }
    }

    fn claim(catalog: &SystemCatalog, topic: &str, origin: PublishOrigin) -> bool {
        let txn = catalog.db.begin_write().expect("write txn");
        let claimed = claim_origin(&txn, (DatabaseId::DEFAULT, 1, topic), &origin).expect("claim");
        txn.commit().expect("commit");
        claimed
    }

    #[test]
    fn a_position_is_claimed_once_per_topic_and_partition() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        assert!(claim(&catalog, "feed", origin(3, 10)));
        assert!(
            !claim(&catalog, "feed", origin(3, 10)),
            "a repeat is refused"
        );
        assert!(
            !claim(&catalog, "feed", origin(3, 9)),
            "a lower one is refused"
        );
        assert!(claim(&catalog, "feed", origin(3, 11)));
        assert!(
            claim(&catalog, "feed", origin(4, 1)),
            "partitions are apart"
        );
        assert!(claim(&catalog, "feeds", origin(3, 1)), "topics are apart");

        let txn = catalog.db.begin_write().expect("write txn");
        forget_topic_marks(&txn, DatabaseId::DEFAULT, 1, "feed").expect("forget");
        txn.commit().expect("commit");
        assert!(
            claim(&catalog, "feed", origin(3, 1)),
            "a dropped topic has no marks"
        );
        assert!(
            !claim(&catalog, "feeds", origin(3, 1)),
            "another topic keeps its marks"
        );
    }
}
