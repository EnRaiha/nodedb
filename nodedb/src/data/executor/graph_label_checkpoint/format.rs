// SPDX-License-Identifier: BUSL-1.1

//! On-disk types for the CSR graph node-label checkpoint.

use serde::{Deserialize, Serialize};

/// On-disk format version.
///
/// A file stamped with any other version is refused rather than misparsed.
/// Refusing costs a WAL replay; misparsing would install labels on the wrong
/// nodes, which no later record corrects — a `MATCH (a:Person)` would then
/// return rows that were never labeled `Person`.
pub(crate) const GRAPH_LABEL_CKPT_FORMAT_VERSION: u16 = 1;

/// One `(database, tenant)` CSR partition's labeled nodes.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct GraphLabelPartition {
    /// The partition's database id (`DatabaseId::as_u64`).
    pub database_id: u64,
    /// The partition's tenant id (`TenantId::as_u64`).
    ///
    /// Stored next to the database id rather than encoded into a filename
    /// because the whole core lives in one file — see the module docs.
    pub tenant_id: u64,
    /// `(node_name, label_names)` for every node whose label bitset is
    /// non-zero, sorted by node name.
    ///
    /// Names, never local ids: ids are assigned by CSR build order and are not
    /// stable across restarts, so an id-keyed restore would relabel the wrong
    /// nodes. Nodes with no labels are omitted — their bitset is zero and
    /// carries nothing.
    pub nodes: Vec<(String, Vec<String>)>,
}

/// A core's whole graph node-label state, as one file.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct GraphLabelCheckpointFile {
    /// Always [`GRAPH_LABEL_CKPT_FORMAT_VERSION`] when written; validated on
    /// load.
    pub format_version: u16,
    /// The LSN this state is durable THROUGH (inclusive) — the core watermark
    /// at flush time.
    ///
    /// This is what `execute_checkpoint` folds into the minimum it reports, and
    /// what a restart restores `graph_label_durable_lsn` from so a later failed
    /// flush clamps to a real point instead of pinning truncation at zero.
    pub durable_through_lsn: u64,
    /// Every CSR partition on this core that has at least one labeled node,
    /// sorted by `(database_id, tenant_id)` so identical state always encodes
    /// to identical bytes.
    pub partitions: Vec<GraphLabelPartition>,
}
