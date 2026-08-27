// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the snapshot builder→applier round-trip test binaries
//! (`snapshot_round_trip` and `snapshot_round_trip_crdt`). This lives in a
//! subdirectory module so it is shared source compiled into each test binary,
//! not picked up as a test binary of its own. Each binary uses a different
//! subset, so unused-in-one-binary items are allowed.

#![allow(dead_code)]

use nodedb_cluster::routing::RoutingTable;

/// The single data group every vShard maps into under `uniform(1, ..)`.
pub const DATA_GROUP_ID: u64 = 1;

/// Extract the first column of the first `Row` message.
pub fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

/// Build a uniform single-data-group routing table for a single node.
pub fn single_node_routing() -> RoutingTable {
    RoutingTable::uniform(1, &[1], 1)
}
