// SPDX-License-Identifier: BUSL-1.1

//! Restored graph edges as edge units.
//!
//! A backup carries every edge version under its versioned key,
//! `"{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}"`. Each
//! version re-issues at its original `system_from`, so the restored edge keeps
//! its history and its valid-from time. A tombstone version re-issues as a
//! tombstone at its `system_from`. Every replica updates its CSR index and its
//! node identities as it installs each version.
//!
//! Each version re-issues to both endpoint homes, `from_key(src)` and
//! `from_key(dst)`, as every live edge write reaches them. Endpoint identities
//! bind through the routed surrogate exchange, as a live edge write binds
//! them.
//!
//! The key's collection is the name the source Data Plane stored it under.
//! Each version re-issues under the destination-qualified name, and binds its
//! node identities under the bare name in the destination database.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::RestoredEdgeVersion;

use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::engine::graph::edge_store::{
    EdgeValuePayload, is_gdpr_erasure, is_tombstone, parse_versioned_edge_key,
};
use crate::types::{DatabaseId, RecordHomes, TenantId, TraceId};

use super::super::target::{DatabaseTarget, RestoredName};
use super::units::{CollectionEdges, EdgeUnit};

fn malformed(key: &str) -> crate::Error {
    let prefix: String = key.chars().take(64).collect();
    crate::Error::Serialization {
        format: "backup".into(),
        detail: format!("restore: edge key '{prefix:?}' is malformed"),
    }
}

/// The identity of node `node_id` in edge collection `collection`.
///
/// The backup's bind wins: the restore rebinds it on this node before any
/// re-issue. A node the backup carries no bind for is bound on the leader of
/// the collection's home, as a live edge write binds it. Either way the batch
/// carries the identity, and every replica of each home binds it first-wins.
async fn node_identity(
    state: &SharedState,
    database_id: DatabaseId,
    tenant: TenantId,
    collection: &str,
    node_id: &str,
) -> crate::Result<CarriedIdentity> {
    let key = nodedb_types::CollectionKey::from_bare(database_id, collection);
    // The backup's bind is in this node's catalog. Any other key goes to the
    // home through the async exchange, which answers an existing bind too.
    let surrogate = match state
        .surrogate_assigner
        .lookup_bound(key, tenant, node_id.as_bytes())?
    {
        Some(surrogate) => surrogate,
        None => {
            assign_surrogate_routed(state, key, tenant, node_id.as_bytes(), TraceId::ZERO).await?
        }
    };
    Ok(CarriedIdentity {
        collection: collection.to_string(),
        pk_bytes: node_id.as_bytes().to_vec(),
        surrogate,
    })
}

/// One edge version of the collection `name` as a unit.
async fn edge_unit(
    state: &SharedState,
    database_id: DatabaseId,
    tenant: TenantId,
    name: &RestoredName,
    key: &str,
    value: &[u8],
) -> crate::Result<EdgeUnit> {
    let (_, src_id, label, dst_id, system_from) =
        parse_versioned_edge_key(key).ok_or_else(|| malformed(key))?;
    let collection = name.bare.as_str();
    if is_gdpr_erasure(value) {
        return Err(crate::Error::Serialization {
            format: "backup".into(),
            detail: format!(
                "restore: edge version {system_from} in '{collection}' is an erasure marker, \
                 which no write path records"
            ),
        });
    }
    let properties = if is_tombstone(value) {
        None
    } else {
        Some(EdgeValuePayload::decode(value)?.properties)
    };
    let src = node_identity(state, database_id, tenant, collection, src_id).await?;
    let dst = node_identity(state, database_id, tenant, collection, dst_id).await?;
    Ok(EdgeUnit {
        version: RestoredEdgeVersion {
            collection: name.stored.to_string(),
            src_id: src_id.to_string(),
            label: label.to_string(),
            dst_id: dst_id.to_string(),
            src_surrogate: src.surrogate.as_u32(),
            dst_surrogate: dst.surrogate.as_u32(),
            system_from,
            properties,
        },
        homes: RecordHomes::edge(src_id, dst_id),
        identities: vec![src, dst],
    })
}

/// Every restored edge version of `tenant_id` in one database, one unit per
/// version, grouped by edge collection in key order: each edge's versions in
/// system-time order. The edge section of a database's data section holds
/// that database's edges only.
///
/// Returns the number of distinct edge versions and the grouped units.
pub(super) async fn edge_units(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    edges: Vec<(String, Vec<u8>)>,
) -> crate::Result<(usize, Vec<CollectionEdges>)> {
    let database_id = target.dest;
    let tenant = TenantId::new(tenant_id);
    let mut by_key: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (key, value) in edges {
        by_key.insert(key, value);
    }
    let mut grouped: BTreeMap<String, Vec<EdgeUnit>> = BTreeMap::new();
    for (key, value) in &by_key {
        let (stored, ..) = parse_versioned_edge_key(key).ok_or_else(|| malformed(key))?;
        let name = target.resolve(stored)?;
        let unit = edge_unit(state, database_id, tenant, &name, key, value).await?;
        grouped.entry(name.bare).or_default().push(unit);
    }
    let versions = by_key.len();
    let collections = grouped
        .into_iter()
        .map(|(collection, units)| CollectionEdges {
            database_id,
            collection,
            units,
        })
        .collect();
    Ok((versions, collections))
}
