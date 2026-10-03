// SPDX-License-Identifier: BUSL-1.1

//! The durable ledger of events whose actions this node holds for firing.
//!
//! Every replica holds each firing event here under its partition and
//! position, from the moment its Event Plane receives it. An event leaves the
//! ledger once its partition's replicated firing cursor passes it, on every
//! replica alike. The owner of the partition fires from the ledger, so a new
//! owner finds every event the cursor has not passed.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::event::cdc::CdcOffset;
use crate::event::topic::committed::key::{key_bytes, origin_of_key};
use crate::event::topic::types::PublishOrigin;

use super::held::HeldAction;

/// Encoded `(partition, position)` -> encoded [`HeldAction`].
const HELD: TableDefinition<&[u8], &[u8]> = TableDefinition::new("held_trigger_actions");

fn storage(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "event_plane".into(),
        detail: format!("trigger action ledger: {detail}"),
    }
}

fn key(partition: u32, position: CdcOffset) -> [u8; crate::event::topic::committed::key::KEY_LEN] {
    key_bytes(&PublishOrigin {
        partition,
        position,
    })
}

/// Firing events not yet past their partition's firing cursor.
pub struct ActionLedger {
    db: Database,
}

impl ActionLedger {
    /// Open or create the ledger at `{dir}/trigger_action_ledger.redb`.
    pub fn open(dir: &Path) -> crate::Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| storage(format!("create {}: {e}", dir.display())))?;
        let path = dir.join("trigger_action_ledger.redb");
        let db = Database::create(&path)
            .map_err(|e| storage(format!("open {}: {e}", path.display())))?;
        let txn = db.begin_write().map_err(storage)?;
        txn.open_table(HELD).map_err(storage)?;
        txn.commit().map_err(storage)?;
        Ok(Self { db })
    }

    /// Hold `action` at `position` of `partition`. Holding a held position
    /// again writes the same row.
    pub fn hold(
        &self,
        partition: u32,
        position: CdcOffset,
        action: &HeldAction,
    ) -> crate::Result<()> {
        self.hold_many(&[(partition, position, action.clone())])
    }

    /// Hold every `(partition, position, action)` of `rows` in one commit.
    pub fn hold_many(&self, rows: &[(u32, CdcOffset, HeldAction)]) -> crate::Result<()> {
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut held = txn.open_table(HELD).map_err(storage)?;
            for (partition, position, action) in rows {
                let bytes = action.to_bytes()?;
                held.insert(key(*partition, *position).as_slice(), bytes.as_slice())
                    .map_err(storage)?;
            }
        }
        txn.commit().map_err(storage)
    }

    /// Every event `partition` holds, each as its position and encoded row.
    pub fn rows_of(&self, partition: u32) -> crate::Result<Vec<(CdcOffset, Vec<u8>)>> {
        let txn = self.db.begin_read().map_err(storage)?;
        let held = txn.open_table(HELD).map_err(storage)?;
        let from = key(partition, CdcOffset::ZERO);
        let upto = key(partition, CdcOffset::at(u64::MAX, u64::MAX, u64::MAX));
        let mut rows = Vec::new();
        for entry in held
            .range(from.as_slice()..=upto.as_slice())
            .map_err(storage)?
        {
            let (key, value) = entry.map_err(storage)?;
            let origin = origin_of_key(key.value()).ok_or_else(|| storage("malformed key"))?;
            rows.push((origin.position, value.value().to_vec()));
        }
        Ok(rows)
    }

    /// Replace what `partition` holds at or below `through` with `rows`, in
    /// one commit. A snapshot install takes the builder's events there.
    /// With no `through`, `rows` join what the partition holds.
    pub fn replace_through(
        &self,
        partition: u32,
        through: Option<CdcOffset>,
        rows: &[(CdcOffset, Vec<u8>)],
    ) -> crate::Result<()> {
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut held = txn.open_table(HELD).map_err(storage)?;
            if let Some(through) = through {
                let from = key(partition, CdcOffset::ZERO);
                let upto = key(partition, through);
                held.retain_in(from.as_slice()..=upto.as_slice(), |_, _| false)
                    .map_err(storage)?;
            }
            for (position, bytes) in rows {
                // A row that does not decode fails every owner's firing.
                HeldAction::from_bytes(bytes)?;
                held.insert(key(partition, *position).as_slice(), bytes.as_slice())
                    .map_err(storage)?;
            }
        }
        txn.commit().map_err(storage)
    }

    /// The partitions that hold an event.
    pub fn partitions(&self) -> crate::Result<Vec<u32>> {
        let txn = self.db.begin_read().map_err(storage)?;
        let held = txn.open_table(HELD).map_err(storage)?;
        let mut partitions = Vec::new();
        let mut next = Some(0u32);
        // One range probe per partition: the first key at or after the
        // partition's prefix names the next partition that holds an event.
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

    /// The highest position `partition` holds.
    pub fn tail(&self, partition: u32) -> crate::Result<Option<CdcOffset>> {
        let txn = self.db.begin_read().map_err(storage)?;
        let held = txn.open_table(HELD).map_err(storage)?;
        let from = key(partition, CdcOffset::ZERO);
        let upto = key(partition, CdcOffset::at(u64::MAX, u64::MAX, u64::MAX));
        let last = held
            .range(from.as_slice()..=upto.as_slice())
            .map_err(storage)?
            .next_back();
        match last {
            None => Ok(None),
            Some(entry) => {
                let (key, _) = entry.map_err(storage)?;
                let origin = origin_of_key(key.value()).ok_or_else(|| storage("malformed key"))?;
                Ok(Some(origin.position))
            }
        }
    }

    /// Up to `limit` events of `partition` above `after`, in position order.
    pub fn held_after(
        &self,
        partition: u32,
        after: CdcOffset,
        limit: usize,
    ) -> crate::Result<Vec<(CdcOffset, HeldAction)>> {
        let txn = self.db.begin_read().map_err(storage)?;
        let held = txn.open_table(HELD).map_err(storage)?;
        let from = key(partition, after);
        let mut events = Vec::new();
        for entry in held.range(from.as_slice()..).map_err(storage)? {
            let (key, value) = entry.map_err(storage)?;
            let origin = origin_of_key(key.value()).ok_or_else(|| storage("malformed key"))?;
            if origin.partition != partition || events.len() >= limit {
                break;
            }
            if origin.position <= after {
                continue;
            }
            events.push((origin.position, HeldAction::from_bytes(value.value())?));
        }
        Ok(events)
    }

    /// Remove every event of `partition` at or below `through`. Writes
    /// nothing when there is none.
    pub fn release_through(&self, partition: u32, through: CdcOffset) -> crate::Result<()> {
        let from = key(partition, CdcOffset::ZERO);
        let upto = key(partition, through);
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

    fn action(row: &str) -> HeldAction {
        HeldAction {
            database_id: 0,
            tenant_id: 1,
            vshard_id: 2,
            collection: "orders".into(),
            op: 1,
            count: 1,
            row: Some(row.into()),
            source: crate::event::types::EventSource::User.wal_code(),
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            commit_hlc: None,
        }
    }

    fn at(index: u64, ordinal: u64) -> CdcOffset {
        CdcOffset::data_event(0, index, ordinal)
    }

    #[test]
    fn held_events_come_back_in_position_order_above_the_cursor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = ActionLedger::open(dir.path()).expect("open");
        ledger.hold(2, at(9, 1), &action("c")).expect("hold");
        ledger.hold(2, at(7, 2), &action("b")).expect("hold");
        ledger.hold(2, at(7, 1), &action("a")).expect("hold");
        ledger.hold(5, at(1, 1), &action("x")).expect("hold");
        // A replayed event holds the same row again.
        ledger.hold(2, at(7, 1), &action("a")).expect("hold");

        assert_eq!(ledger.partitions().expect("partitions"), vec![2, 5]);
        assert_eq!(ledger.tail(2).expect("tail"), Some(at(9, 1)));
        assert_eq!(ledger.tail(3).expect("tail"), None);
        let rows = |after| -> Vec<String> {
            ledger
                .held_after(2, after, 10)
                .expect("read")
                .into_iter()
                .filter_map(|(_, held)| held.row)
                .collect()
        };
        assert_eq!(rows(CdcOffset::ZERO), vec!["a", "b", "c"]);
        assert_eq!(rows(at(7, 1)), vec!["b", "c"]);
    }

    #[test]
    fn release_removes_only_the_partition_prefix_through_the_cursor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = ActionLedger::open(dir.path()).expect("open");
        ledger.hold(2, at(7, 1), &action("a")).expect("hold");
        ledger.hold(2, at(9, 1), &action("c")).expect("hold");
        ledger.hold(3, at(1, 1), &action("x")).expect("hold");

        ledger.release_through(2, at(7, 1)).expect("release");
        let left = ledger.held_after(2, CdcOffset::ZERO, 10).expect("read");
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].1.row.as_deref(), Some("c"));

        drop(ledger);
        let reopened = ActionLedger::open(dir.path()).expect("reopen");
        assert_eq!(reopened.partitions().expect("partitions"), vec![2, 3]);
    }

    #[test]
    fn a_snapshot_replaces_the_rows_through_its_cut_and_keeps_later_ones() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = ActionLedger::open(dir.path()).expect("open");
        ledger.hold(2, at(3, 1), &action("stale")).expect("hold");
        ledger.hold(2, at(20, 1), &action("later")).expect("hold");
        let carried = vec![(at(5, 1), action("carried").to_bytes().expect("encode"))];
        ledger
            .replace_through(2, Some(CdcOffset::whole_write(0, 10)), &carried)
            .expect("replace");
        let rows: Vec<String> = ledger
            .held_after(2, CdcOffset::ZERO, 10)
            .expect("read")
            .into_iter()
            .filter_map(|(_, held)| held.row)
            .collect();
        assert_eq!(rows, vec!["carried", "later"]);
        assert_eq!(ledger.rows_of(2).expect("rows").len(), 2);
        // A row that does not decode is refused.
        assert!(
            ledger
                .replace_through(2, None, &[(at(30, 1), vec![0xc1])])
                .is_err()
        );
    }
}
