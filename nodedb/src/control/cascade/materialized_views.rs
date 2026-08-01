// SPDX-License-Identifier: BUSL-1.1

//! Materialized-view enumeration for `PurgeCollection` cascade.
//!
//! An MV's `source` points at the collection whose CDC stream feeds
//! it. When the source is hard-deleted the MV is no longer definable
//! and must be dropped. This walks the MV graph transitively: an MV
//! that sources another MV (which sources the purged collection) is
//! itself a dependent.
//!
//! `MAX_DEPTH` bounds the transitive walk — a legitimate MV DAG will
//! never approach 32 levels, so exceeding it means a cycle (or an
//! adversarial definition) and the orchestrator treats it as an
//! error.

use std::collections::HashSet;

use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;

/// Safety bound on MV-of-MV chain depth.
pub const MAX_DEPTH: usize = 32;

/// Enumerate MVs whose source (direct or transitive) is
/// `(tenant_id, root_collection)`. Returns MV names only, sorted.
///
/// Returns `NodeDbError::cascade_cycle` if the MV graph exceeds
/// `MAX_DEPTH` levels — the orchestrator surfaces that up as a purge
/// blocker rather than silently truncating.
pub fn find_mvs_sourcing(
    catalog: &SystemCatalog,
    tenant_id: u64,
    root_collection: &str,
) -> crate::Result<Vec<String>> {
    let all = catalog.load_all_materialized_views()?;

    // Build adjacency: source-name → [mv names]. Only consider the
    // target tenant — MV definitions are tenant-scoped.
    use std::collections::HashMap;
    let mut by_source: HashMap<String, Vec<String>> = HashMap::new();
    for mv in all.iter().filter(|m| m.tenant_id == tenant_id) {
        by_source
            .entry(mv.source.clone())
            .or_default()
            .push(mv.name.clone());
    }

    let mut found: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = vec![root_collection.to_string()];
    let mut depth = 0usize;
    while !frontier.is_empty() {
        depth += 1;
        if depth > MAX_DEPTH {
            return Err(crate::Error::CascadeCycle {
                tenant_id,
                root: root_collection.to_string(),
                depth: MAX_DEPTH,
            });
        }
        let mut next: Vec<String> = Vec::new();
        for src in frontier {
            if let Some(mvs) = by_source.get(&src) {
                for mv_name in mvs {
                    if found.insert(mv_name.clone()) {
                        next.push(mv_name.clone());
                    }
                }
            }
        }
        frontier = next;
    }

    let mut out: Vec<String> = found.into_iter().collect();
    out.sort();
    Ok(out)
}

/// Enumerate streaming MVs whose source stream is attached to
/// `(database_id, tenant_id, root_collection)`, including any MV-of-MV chain.
///
/// Streaming definitions name a CDC stream rather than a collection. Seed the
/// graph with every stream on the collection, then walk source-stream → MV-name
/// edges. The database filter is essential: identical stream and MV names are
/// valid in different databases for the same tenant.
pub fn find_streaming_mvs_sourcing(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    root_collection: &str,
) -> crate::Result<Vec<String>> {
    let streams = catalog.load_all_change_streams()?;
    let mut frontier: Vec<String> = streams
        .into_iter()
        .filter(|stream| {
            stream.database_id == database_id
                && stream.tenant_id == tenant_id
                && stream.collection == root_collection
        })
        .map(|stream| stream.name)
        .collect();
    // Preserve compatibility with definitions whose source stream reused the
    // collection name before explicit stream lineage was recorded.
    frontier.push(root_collection.to_string());

    let mut by_source: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for mv in catalog
        .load_all_streaming_mvs()?
        .into_iter()
        .filter(|mv| mv.database_id == database_id && mv.tenant_id == tenant_id)
    {
        by_source.entry(mv.source_stream).or_default().push(mv.name);
    }

    let mut found = HashSet::new();
    let mut depth = 0usize;
    while !frontier.is_empty() {
        depth += 1;
        if depth > MAX_DEPTH {
            return Err(crate::Error::CascadeCycle {
                tenant_id,
                root: root_collection.to_string(),
                depth: MAX_DEPTH,
            });
        }
        let mut next = Vec::new();
        for source in frontier {
            if let Some(mvs) = by_source.get(&source) {
                for name in mvs {
                    if found.insert(name.clone()) {
                        next.push(name.clone());
                    }
                }
            }
        }
        frontier = next;
    }

    let mut out: Vec<_> = found.into_iter().collect();
    out.sort();
    Ok(out)
}
