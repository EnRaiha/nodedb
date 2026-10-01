// SPDX-License-Identifier: BUSL-1.1

//! The durable ledger of committed messages this node holds for delivery.
//!
//! Every replica of a record holds its messages here, keyed by origin, from
//! the moment its Event Plane receives their events. A message leaves the
//! ledger once the replicated delivery cursor of its partition passes it, on
//! every replica alike. The replica that holds the partition's lease delivers
//! from the ledger, so a new lease holder finds every message the cursor has
//! not passed.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::event::cdc::CdcOffset;
use crate::event::topic::types::PublishOrigin;
use crate::wal::RedoPublish;

use super::key::{key_bytes, origin_of_key};

/// Encoded origin -> encoded [`RedoPublish`].
const HELD: TableDefinition<&[u8], &[u8]> = TableDefinition::new("committed_publishes");

fn storage(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "event_plane".into(),
        detail: format!("publish ledger: {detail}"),
    }
}

/// Committed messages not yet past their partition's delivery cursor.
pub struct PublishLedger {
    db: Database,
    /// Receiver refusals of the held cross-node requests, in memory.
    pub outbox_refusals: super::outbox::OutboxRefusals,
}

impl PublishLedger {
    /// Open or create the ledger at `{dir}/publish_ledger.redb`.
    pub fn open(dir: &Path) -> crate::Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| storage(format!("create {}: {e}", dir.display())))?;
        let path = dir.join("publish_ledger.redb");
        let db = Database::create(&path)
            .map_err(|e| storage(format!("open {}: {e}", path.display())))?;
        let txn = db.begin_write().map_err(storage)?;
        txn.open_table(HELD).map_err(storage)?;
        txn.commit().map_err(storage)?;
        Ok(Self {
            db,
            outbox_refusals: super::outbox::OutboxRefusals::default(),
        })
    }

    /// Hold `publish` under `origin`. Holding a held origin again writes the
    /// same row.
    pub fn hold(&self, origin: &PublishOrigin, publish: &RedoPublish) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(publish).map_err(storage)?;
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut held = txn.open_table(HELD).map_err(storage)?;
            held.insert(key_bytes(origin).as_slice(), bytes.as_slice())
                .map_err(storage)?;
        }
        txn.commit().map_err(storage)
    }

    /// The partitions that hold a message.
    pub fn partitions(&self) -> crate::Result<Vec<u32>> {
        let txn = self.db.begin_read().map_err(storage)?;
        let held = txn.open_table(HELD).map_err(storage)?;
        let mut partitions = Vec::new();
        let mut next = Some(0u32);
        // One range probe per partition: the first key at or after the
        // partition's prefix names the next partition that holds a message.
        while let Some(from) = next {
            let start = from.to_be_bytes();
            let Some(entry) = held.range(start.as_slice()..).map_err(storage)?.next() else {
                break;
            };
            let (key, _) = entry.map_err(storage)?;
            let origin = origin_of_key(key.value()).ok_or_else(|| storage("malformed key"))?;
            partitions.push(origin.partition);
            next = origin.partition.checked_add(1);
        }
        Ok(partitions)
    }

    /// Up to `limit` messages of `partition` above `after`, in position
    /// order.
    pub fn held_after(
        &self,
        partition: u32,
        after: CdcOffset,
        limit: usize,
    ) -> crate::Result<Vec<(PublishOrigin, RedoPublish)>> {
        let txn = self.db.begin_read().map_err(storage)?;
        let held = txn.open_table(HELD).map_err(storage)?;
        let from = key_bytes(&PublishOrigin {
            partition,
            position: after,
        });
        let mut messages = Vec::new();
        for entry in held.range(from.as_slice()..).map_err(storage)? {
            let (key, value) = entry.map_err(storage)?;
            let origin = origin_of_key(key.value()).ok_or_else(|| storage("malformed key"))?;
            if origin.partition != partition || messages.len() >= limit {
                break;
            }
            if origin.position <= after {
                continue;
            }
            let publish: RedoPublish = zerompk::from_msgpack(value.value()).map_err(storage)?;
            messages.push((origin, publish));
        }
        Ok(messages)
    }

    /// Remove every message of `partition` at or below `through`. Writes
    /// nothing when there is none.
    pub fn release_through(&self, partition: u32, through: CdcOffset) -> crate::Result<()> {
        let from = key_bytes(&PublishOrigin {
            partition,
            position: CdcOffset::ZERO,
        });
        let upto = key_bytes(&PublishOrigin {
            partition,
            position: through,
        });
        {
            let txn = self.db.begin_read().map_err(storage)?;
            let held = txn.open_table(HELD).map_err(storage)?;
            let mut passed = held
                .range(from.as_slice()..=upto.as_slice())
                .map_err(storage)?;
            if passed.next().is_none() {
                return Ok(());
            }
        }
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut held = txn.open_table(HELD).map_err(storage)?;
            held.retain_in(from.as_slice()..=upto.as_slice(), |_, _| false)
                .map_err(storage)?;
        }
        txn.commit().map_err(storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::topic::committed::event::tests::publish;

    fn origin(partition: u32, index: u64, ordinal: u64) -> PublishOrigin {
        PublishOrigin {
            partition,
            position: CdcOffset::data_event(0, index, ordinal),
        }
    }

    #[test]
    fn held_messages_come_back_in_position_order_above_the_cursor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = PublishLedger::open(dir.path()).expect("open");
        ledger
            .hold(&origin(2, 9, 1), &publish("feed", "c"))
            .expect("hold");
        ledger
            .hold(&origin(2, 7, 2), &publish("feed", "b"))
            .expect("hold");
        ledger
            .hold(&origin(2, 7, 1), &publish("feed", "a"))
            .expect("hold");
        ledger
            .hold(&origin(5, 1, 1), &publish("other", "x"))
            .expect("hold");
        // A replayed event holds the same message again.
        ledger
            .hold(&origin(2, 7, 1), &publish("feed", "a"))
            .expect("hold");

        assert_eq!(ledger.partitions().expect("partitions"), vec![2, 5]);
        let payloads = |after| -> Vec<String> {
            ledger
                .held_after(2, after, 10)
                .expect("read")
                .into_iter()
                .map(|(_, publish)| publish.payload)
                .collect()
        };
        assert_eq!(payloads(CdcOffset::ZERO), vec!["a", "b", "c"]);
        assert_eq!(payloads(origin(2, 7, 1).position), vec!["b", "c"]);
        assert_eq!(
            ledger
                .held_after(2, CdcOffset::ZERO, 1)
                .expect("read")
                .len(),
            1
        );
    }

    #[test]
    fn release_removes_only_the_partition_prefix_through_the_cursor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = PublishLedger::open(dir.path()).expect("open");
        ledger
            .hold(&origin(2, 7, 1), &publish("feed", "a"))
            .expect("hold");
        ledger
            .hold(&origin(2, 9, 1), &publish("feed", "c"))
            .expect("hold");
        ledger
            .hold(&origin(3, 1, 1), &publish("feed", "x"))
            .expect("hold");

        ledger
            .release_through(2, origin(2, 7, 1).position)
            .expect("release");
        let left = ledger.held_after(2, CdcOffset::ZERO, 10).expect("read");
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].1.payload, "c");
        assert_eq!(ledger.partitions().expect("partitions"), vec![2, 3]);

        // The ledger survives a reopen.
        drop(ledger);
        let reopened = PublishLedger::open(dir.path()).expect("reopen");
        assert_eq!(reopened.partitions().expect("partitions"), vec![2, 3]);
    }
}
