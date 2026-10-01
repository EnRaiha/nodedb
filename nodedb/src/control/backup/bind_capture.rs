// SPDX-License-Identifier: BUSL-1.1

//! Capture a tenant's PK→surrogate binds from the nodes that hold them.
//!
//! A bind lives on its collection's home vShard and, in a collection that
//! holds edges, on the key vShard of its key (see [`RecordHomes`]). With more
//! nodes than the replication factor no one node holds every bind. The capture
//! asks the source node of each vShard, the node a backup reads that vShard's
//! data from, for the binds with a home among its vShards, over the same
//! `ExecuteRequest` path as the per-node data snapshot. A bind two sources
//! return is kept once.

use std::collections::{BTreeMap, HashSet};

use futures::future::join_all;
use nodedb_physical::physical_plan::ClusterEventOp;
use nodedb_types::{CollectionKey, DatabaseId};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{HomedRecord, RecordHomes, SurrogateBindEntry, TenantId};

use super::node_snapshot::{is_self, snapshot_remote};
use super::orchestrator::source_assignment;

/// This node's binds of `tenant_id`'s `collections` in `database_id` with a
/// home among `vshards`.
pub(crate) fn local_binds(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    vshards: &HashSet<u32>,
    collections: &[String],
) -> Result<Vec<SurrogateBindEntry>, Error> {
    let catalog = state.credentials.catalog();
    let mut binds = Vec::new();
    for name in collections {
        let holds_edges = catalog
            .get_collection(database_id, tenant_id, name)?
            .is_some_and(|coll| coll.has_implicit_edges);
        let key = CollectionKey::from_bare(database_id, name);
        // Without edges every bind homes on the collection's vShard.
        if !holds_edges && !vshards.contains(&key.vshard().as_u32()) {
            continue;
        }
        for (pk, surrogate) in
            catalog.scan_surrogates_for_collection(key, TenantId::new(tenant_id))?
        {
            let homes = RecordHomes::of(HomedRecord::Bind {
                collection: key,
                key: &pk,
                holds_edges,
            });
            if homes.intersects(vshards) {
                binds.push(SurrogateBindEntry {
                    database_id: database_id.as_u64(),
                    tenant_id,
                    collection: name.clone(),
                    pk,
                    surrogate: surrogate.as_u32(),
                });
            }
        }
    }
    Ok(binds)
}

/// Every bind of `tenant_id`'s `collections` in `database_id`, each from a
/// source node that holds it. Any node error fails the capture.
pub(crate) async fn capture_binds(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    collections: &[String],
) -> Result<Vec<SurrogateBindEntry>, Error> {
    let answers = join_all(source_assignment(state)?.into_iter().map(
        |(node_id, vshards)| async move {
            let binds = if is_self(state, node_id) {
                local_binds(state, tenant_id, database_id, &vshards, collections)?
            } else {
                remote_binds(
                    state,
                    node_id,
                    tenant_id,
                    database_id,
                    &vshards,
                    collections,
                )
                .await?
            };
            Ok::<_, Error>((vshards, binds))
        },
    ))
    .await;
    let mut parts = Vec::with_capacity(answers.len());
    for answer in answers {
        parts.push(answer?);
    }
    Ok(merge_binds(parts))
}

/// Ask `node_id` for its binds with a home among `vshards`.
async fn remote_binds(
    state: &SharedState,
    node_id: u64,
    tenant_id: u64,
    database_id: DatabaseId,
    vshards: &HashSet<u32>,
    collections: &[String],
) -> Result<Vec<SurrogateBindEntry>, Error> {
    let mut vshards: Vec<u32> = vshards.iter().copied().collect();
    vshards.sort_unstable();
    let plan = PhysicalPlan::ClusterEvent(ClusterEventOp::SurrogateBinds {
        tenant_id,
        database_id,
        vshards,
        collections: collections.to_vec(),
    });
    let body = snapshot_remote(state, node_id, tenant_id, database_id, &plan).await?;
    decode_binds(&body, node_id)
}

/// Encode binds for the wire.
pub(crate) fn encode_binds(binds: Vec<SurrogateBindEntry>) -> Result<Vec<u8>, Error> {
    zerompk::to_msgpack_vec(&binds).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("surrogate bind capture: encode: {e}"),
    })
}

fn decode_binds(bytes: &[u8], node_id: u64) -> Result<Vec<SurrogateBindEntry>, Error> {
    zerompk::from_msgpack(bytes).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("surrogate bind capture: decode the answer of node {node_id}: {e}"),
    })
}

/// Merge the binds each source returned, keeping one per key.
///
/// A source whose vShards hold the bind's collection home wins: a row's
/// identity is bound there. A different surrogate from another source is a
/// split identity, logged and dropped.
fn merge_binds(parts: Vec<(HashSet<u32>, Vec<SurrogateBindEntry>)>) -> Vec<SurrogateBindEntry> {
    type BindKey = (u64, u64, String, Vec<u8>);
    let mut merged: BTreeMap<BindKey, SurrogateBindEntry> = BTreeMap::new();
    let mut deferred = Vec::new();
    for (vshards, binds) in parts {
        for bind in binds {
            let home =
                CollectionKey::from_bare(DatabaseId::new(bind.database_id), &bind.collection)
                    .vshard()
                    .as_u32();
            if vshards.contains(&home) {
                let key = bind_key(&bind);
                merged.insert(key, bind);
            } else {
                deferred.push(bind);
            }
        }
    }
    for bind in deferred {
        let key = bind_key(&bind);
        match merged.get(&key) {
            Some(kept) if kept.surrogate != bind.surrogate => {
                tracing::warn!(
                    collection = %bind.collection,
                    kept = kept.surrogate,
                    dropped = bind.surrogate,
                    "surrogate bind capture: two sources bind one key to different surrogates"
                );
            }
            Some(_) => {}
            None => {
                merged.insert(key, bind);
            }
        }
    }
    merged.into_values().collect()
}

fn bind_key(bind: &SurrogateBindEntry) -> (u64, u64, String, Vec<u8>) {
    (
        bind.database_id,
        bind.tenant_id,
        bind.collection.clone(),
        bind.pk.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bind(collection: &str, pk: &str, surrogate: u32) -> SurrogateBindEntry {
        SurrogateBindEntry {
            database_id: 0,
            tenant_id: 1,
            collection: collection.into(),
            pk: pk.as_bytes().to_vec(),
            surrogate,
        }
    }

    #[test]
    fn a_bind_two_sources_return_is_kept_once() {
        let home = CollectionKey::from_bare(DatabaseId::DEFAULT, "g")
            .vshard()
            .as_u32();
        let owner: HashSet<u32> = [home].into_iter().collect();
        let other: HashSet<u32> = [(home + 1) % 1024].into_iter().collect();
        let merged = merge_binds(vec![
            (other.clone(), vec![bind("g", "a", 9), bind("g", "b", 5)]),
            (owner, vec![bind("g", "a", 7)]),
        ]);
        assert_eq!(merged, vec![bind("g", "a", 7), bind("g", "b", 5)]);
    }

    #[test]
    fn binds_round_trip_the_wire() {
        let binds = vec![bind("g", "a", 7)];
        let bytes = encode_binds(binds.clone()).expect("encode");
        assert_eq!(decode_binds(&bytes, 2).expect("decode"), binds);
    }
}
