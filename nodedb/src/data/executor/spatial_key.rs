// SPDX-License-Identifier: BUSL-1.1

//! Shared key type for per-field spatial R-tree indexes.

use crate::types::TenantId;
use nodedb_types::DatabaseId;

/// Identifies a per-field spatial R-tree index: `(database, tenant,
/// collection, field)`. Shared by `spatial_indexes`/undo-entry capture sites
/// so the 4-tuple isn't spelled out at every call site.
pub(in crate::data::executor) type SpatialIndexKey = (DatabaseId, TenantId, String, String);
