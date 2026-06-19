// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH_STATS` table definition, row payload types, key builders, and the
//! `CollectionStats` public return type.
//!
//! Key shape: `(db: u64, tenant: u64, key: String)` where `key` is
//! `"<collection>\x00<kind>[\x00<discriminator>]"`.
//!
//! Row kinds:
//! - `"<collection>\x00summary"` → [`SummaryRow`]
//! - `"<collection>\x00label\x00<label>"` → [`LabelRow`]
//! - `"<collection>\x00node\x00<node_id>"` → [`NodeRow`]

use serde::{Deserialize, Serialize};

use redb::TableDefinition;

/// `GRAPH_STATS` table: database- and tenant-qualified stat rows.
/// Key: `(db_u64, tid_u64, "<collection>\x00<kind>[\x00<discriminator>]")`
/// Value: zerompk-encoded payload (SummaryRow | LabelRow | NodeRow).
pub const GRAPH_STATS: TableDefinition<(u64, u64, &str), &[u8]> =
    TableDefinition::new("graph_stats_v2");

/// Legacy (pre-database-scoping) `(tid, key)` stats table. Read only by the
/// `migrate_edges_v2` rewrite; never touched by live paths.
pub const GRAPH_STATS_LEGACY: TableDefinition<(u64, &str), &[u8]> =
    TableDefinition::new("graph_stats");

// ── Row payload types ─────────────────────────────────────────────────────────

/// Aggregate counters for a `(tenant, collection)` pair. Written as the
/// `summary` row kind.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SummaryRow {
    pub edge_count: u64,
    pub distinct_node_count: u64,
    pub distinct_label_count: u64,
}

impl SummaryRow {
    pub fn zero() -> Self {
        Self {
            edge_count: 0,
            distinct_node_count: 0,
            distinct_label_count: 0,
        }
    }

    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("encode SummaryRow: {e}"),
        })
    }

    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("decode SummaryRow: {e}"),
        })
    }
}

/// Per-label edge count. Written as the `label` row kind.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct LabelRow {
    pub count: u64,
}

impl LabelRow {
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("encode LabelRow: {e}"),
        })
    }

    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("decode LabelRow: {e}"),
        })
    }
}

/// Per-node reference count. Written as the `node` row kind.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NodeRow {
    pub refcount: u32,
}

impl NodeRow {
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("encode NodeRow: {e}"),
        })
    }

    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("decode NodeRow: {e}"),
        })
    }
}

// ── Key builders ──────────────────────────────────────────────────────────────

/// Key for the summary row of a collection.
pub fn summary_key(collection: &str) -> String {
    format!("{collection}\x00summary")
}

/// Key for a per-label count row.
pub fn label_key(collection: &str, label: &str) -> String {
    format!("{collection}\x00label\x00{label}")
}

/// Key for a per-node refcount row.
pub fn node_key(collection: &str, node_id: &str) -> String {
    format!("{collection}\x00node\x00{node_id}")
}

/// Prefix that covers all stat rows for a given collection.
pub fn collection_stat_prefix(collection: &str) -> String {
    format!("{collection}\x00")
}

/// Prefix that covers all label rows for a given collection.
pub fn label_prefix(collection: &str) -> String {
    format!("{collection}\x00label\x00")
}

// ── Public return type ────────────────────────────────────────────────────────

/// Stats snapshot for a single `(tenant, collection)` pair.
///
/// Live snapshot queries are O(1) for the summary fields plus O(distinct_labels)
/// for the `labels` vec. Historical snapshot queries (`as_of = Some(ts)`) fall
/// back to a full EDGES prefix scan and are O(edges-in-collection).
///
/// Historical snapshot queries are implemented via the `as_of` parameter on
/// [`EdgeStore::collection_stats`] and [`EdgeStore::tenant_stats`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CollectionStats {
    pub collection: String,
    pub edge_count: u64,
    pub distinct_node_count: u64,
    pub distinct_label_count: u64,
    /// Per-label edge counts, sorted ascending by label name for determinism.
    pub labels: Vec<(String, u64)>,
}

impl CollectionStats {
    pub fn zero(collection: String) -> Self {
        Self {
            collection,
            edge_count: 0,
            distinct_node_count: 0,
            distinct_label_count: 0,
            labels: Vec::new(),
        }
    }
}
