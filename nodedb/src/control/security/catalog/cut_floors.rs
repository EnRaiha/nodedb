// SPDX-License-Identifier: BUSL-1.1

//! `_system.cut_floors`: the cut barriers each data group applied on this
//! node, by log index, with each barrier's watermark.
//!
//! Every entry a group's log places after a barrier records a commit HLC
//! above the barrier's watermark. After a restart the apply loop applies the
//! entries above the group's durable applied index again, and some follow a
//! barrier it applied before the restart. These rows give those barriers
//! back, whatever a checkpoint truncated from the WAL. The table is local:
//! every replica writes the same rows from the same log.

use redb::{ReadableDatabase, ReadableTable, TableError};

use super::types::{SystemCatalog, catalog_err};

/// Redb table: group id -> MessagePack [`StoredFloors`].
pub(super) const CUT_FLOORS: redb::TableDefinition<u64, &[u8]> =
    redb::TableDefinition::new("_system.cut_floors");

/// One barrier a group applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredBarrier {
    /// The barrier's log index.
    pub index: u64,
    /// The barrier's watermark: HLC wall time in nanoseconds.
    pub watermark: u64,
}

#[derive(Debug, Clone, Default, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct StoredFloors {
    /// In index order.
    barriers: Vec<StoredBarrier>,
}

fn decode(bytes: &[u8]) -> crate::Result<StoredFloors> {
    zerompk::from_msgpack(bytes).map_err(|e| catalog_err("decode cut floors", e))
}

impl SystemCatalog {
    /// Record the barrier `barrier` group `group_id` applied.
    ///
    /// Entries apply in log order and a restart resumes above the durable
    /// applied index `durable_applied`. So of the barriers at or below it,
    /// only the one with the highest watermark still binds an entry: the
    /// rest go.
    pub fn put_cut_floor(
        &self,
        group_id: u64,
        barrier: StoredBarrier,
        durable_applied: u64,
    ) -> crate::Result<()> {
        crate::fail_point_err!("cut_floor::before_persist", |detail: String| {
            crate::Error::Internal {
                detail: format!("fail point: {detail}"),
            }
        });
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("cut floor write txn", e))?;
        {
            let mut table = txn
                .open_table(CUT_FLOORS)
                .map_err(|e| catalog_err("open cut floors", e))?;
            let mut floors = match table
                .get(group_id)
                .map_err(|e| catalog_err("get cut floors", e))?
            {
                Some(bytes) => decode(bytes.value())?,
                None => StoredFloors::default(),
            };
            floors.barriers.retain(|held| held.index != barrier.index);
            floors.barriers.push(barrier);
            floors.barriers.sort_by_key(|held| held.index);
            let settled = floors
                .barriers
                .iter()
                .filter(|held| held.index <= durable_applied)
                .map(|held| held.watermark)
                .max();
            if let Some(watermark) = settled {
                let highest = floors
                    .barriers
                    .iter()
                    .filter(|held| held.index <= durable_applied)
                    .map(|held| held.index)
                    .max()
                    .unwrap_or(0);
                floors.barriers.retain(|held| held.index > durable_applied);
                floors.barriers.insert(
                    0,
                    StoredBarrier {
                        index: highest,
                        watermark,
                    },
                );
            }
            let bytes = zerompk::to_msgpack_vec(&floors)
                .map_err(|e| catalog_err("encode cut floors", e))?;
            table
                .insert(group_id, bytes.as_slice())
                .map_err(|e| catalog_err("insert cut floors", e))?;
        }
        txn.commit().map_err(|e| catalog_err("cut floor commit", e))
    }

    /// Every recorded barrier, as `(group_id, barrier)`.
    pub fn load_cut_floors(&self) -> crate::Result<Vec<(u64, StoredBarrier)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("cut floor read txn", e))?;
        let table = match txn.open_table(CUT_FLOORS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(catalog_err("open cut floors", e)),
        };
        let mut all = Vec::new();
        for row in table
            .range(..)
            .map_err(|e| catalog_err("range cut floors", e))?
        {
            let (group_id, value) = row.map_err(|e| catalog_err("read cut floors", e))?;
            let group_id = group_id.value();
            all.extend(
                decode(value.value())?
                    .barriers
                    .into_iter()
                    .map(|barrier| (group_id, barrier)),
            );
        }
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn barrier(index: u64, watermark: u64) -> StoredBarrier {
        StoredBarrier { index, watermark }
    }

    #[test]
    fn settled_barriers_fold_into_the_one_that_still_binds() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        catalog.put_cut_floor(3, barrier(10, 100), 0).unwrap();
        catalog.put_cut_floor(3, barrier(20, 200), 5).unwrap();
        catalog.put_cut_floor(4, barrier(7, 70), 0).unwrap();
        assert_eq!(
            catalog.load_cut_floors().unwrap(),
            [
                (3, barrier(10, 100)),
                (3, barrier(20, 200)),
                (4, barrier(7, 70))
            ]
        );

        // Durable through 25: both barriers of group 3 lie below every entry
        // a restart delivers again, and the higher watermark binds them all.
        catalog.put_cut_floor(3, barrier(30, 300), 25).unwrap();
        assert_eq!(
            catalog.load_cut_floors().unwrap(),
            [
                (3, barrier(20, 200)),
                (3, barrier(30, 300)),
                (4, barrier(7, 70))
            ]
        );
    }
}
