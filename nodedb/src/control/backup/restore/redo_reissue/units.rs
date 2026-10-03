// SPDX-License-Identifier: BUSL-1.1

//! The units a restored row or edge re-issues as.

use nodedb_physical::physical_plan::{RestoredEdgeVersion, RestoredRow};

use crate::control::surrogate::CarriedIdentity;
use crate::types::{DatabaseId, HomedRecord, RecordHomes, VShardId};
use crate::wal::{RedoRowChange, RedoSubRecord};

/// One restored row: its sub-records in apply order, the identities every
/// replica binds before it installs them, the row change its install
/// publishes, and the row its writers lock. A unit never splits across two
/// transactions.
#[derive(Clone)]
pub(super) struct RowUnit {
    pub ops: Vec<RedoSubRecord>,
    pub identities: Vec<CarriedIdentity>,
    /// The change events the unit's install publishes: a restored row that
    /// ends live is an insert.
    pub changes: Vec<RedoRowChange>,
    /// The row the unit writes, as its writers lock it.
    pub rows: Vec<RestoredRow>,
}

impl RowUnit {
    /// The unit's encoded size, for sizing the batches it goes into.
    pub(super) fn byte_len(&self) -> usize {
        self.ops.iter().map(|op| op.payload.len()).sum::<usize>() + identities_len(&self.identities)
    }
}

/// Units of one collection that all write one vShard.
pub(super) struct CollectionUnits {
    /// The destination database.
    pub database_id: DatabaseId,
    /// Bare catalog name of the collection.
    pub collection: String,
    /// The vShard every unit writes.
    pub vshard_id: VShardId,
    pub units: Vec<RowUnit>,
}

impl CollectionUnits {
    /// Rows of `collection`, written to its home vShard.
    pub(super) fn rows(database_id: DatabaseId, collection: String, units: Vec<RowUnit>) -> Self {
        let vshard_id = RecordHomes::of(HomedRecord::Row(nodedb_types::CollectionKey::from_bare(
            database_id,
            &collection,
        )))
        .owner();
        Self {
            database_id,
            collection,
            vshard_id,
            units,
        }
    }
}

/// One restored edge version, the identities of its endpoints, and the
/// homes it re-issues to.
#[derive(Clone)]
pub(super) struct EdgeUnit {
    pub version: RestoredEdgeVersion,
    pub identities: Vec<CarriedIdentity>,
    pub homes: RecordHomes,
}

impl EdgeUnit {
    /// The unit's encoded size, for sizing the batches it goes into.
    pub(super) fn byte_len(&self) -> usize {
        let version = &self.version;
        version.collection.len()
            + version.src_id.len()
            + version.label.len()
            + version.dst_id.len()
            + version.properties.as_ref().map_or(0, Vec::len)
            + identities_len(&self.identities)
    }
}

/// The restored edge versions of one collection, in key order: each edge's
/// versions in system-time order.
pub(super) struct CollectionEdges {
    /// The destination database.
    pub database_id: DatabaseId,
    /// Bare catalog name of the collection.
    pub collection: String,
    pub units: Vec<EdgeUnit>,
}

fn identities_len(identities: &[CarriedIdentity]) -> usize {
    identities
        .iter()
        .map(|identity| identity.collection.len() + identity.pk_bytes.len())
        .sum()
}
