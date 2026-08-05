// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::{DatabaseId, TenantId};

/// A node identity row ordered by `(database, tenant, node)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedNodeRecord {
    pub database: DatabaseId,
    pub tenant: TenantId,
    pub node: String,
    pub surrogate: u32,
}

/// A durable edge-index row ordered by `(database, tenant, key)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedEdgeRecord {
    pub database: DatabaseId,
    pub tenant: TenantId,
    pub key: String,
    pub value: Vec<u8>,
}

/// A graph-statistics row ordered by `(database, tenant, key)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedStatsRecord {
    pub database: DatabaseId,
    pub tenant: TenantId,
    pub key: String,
    pub value: Vec<u8>,
}

/// Resource policy for [`super::super::EdgeStore::restore_sorted_at_path`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SortedRestoreOptions {
    /// Optional redb cache budget in bytes. `None` retains redb's default.
    pub cache_size: Option<usize>,
    /// Preferred bottom-up page packing target. Zero retains redb's base page.
    pub target_page_size: usize,
}
