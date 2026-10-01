// SPDX-License-Identifier: BUSL-1.1

//! Refuse a re-issue that binds a carried surrogate another key holds.
//!
//! Binds are first-wins per key, and nothing else stops two keys of one
//! collection from naming one surrogate: the second key's row installs
//! over the first's. Before any bind, the re-issue asks this node and the home
//! leader of each carried surrogate which key holds it. A different key fails
//! the re-issue with a constraint error naming the collection, the key and the
//! surrogate. Nothing is bound when the check fails.

use std::collections::{BTreeMap, HashMap};

use futures::future::join_all;
use nodedb_physical::physical_plan::ClusterEventOp;
use nodedb_types::{CollectionKey, DatabaseId, Surrogate};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{HomedRecord, RecordHomes, TenantId};

use super::super::node_snapshot::snapshot_remote;

/// The constraint a surrogate conflict names.
pub(crate) const SURROGATE_IDENTITY: &str = "surrogate_identity";

/// A surrogate a re-issue binds, with the key it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CarriedBind {
    /// Bare catalog name of the collection.
    pub collection: String,
    pub pk: Vec<u8>,
    pub surrogate: u32,
}

/// `(collection, surrogate, pk)`: the key a node binds a surrogate to.
type Holder = (String, u32, Vec<u8>);

/// The key this node binds each `(collection, surrogate)` of `entries` to, for
/// the entries it binds at all.
pub(crate) fn local_holders(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    entries: &[(String, u32)],
) -> Result<Vec<Holder>, Error> {
    let catalog = state.credentials.catalog();
    let mut holders = Vec::new();
    for (collection, surrogate) in entries {
        if let Some(pk) = catalog.get_pk_for_surrogate(
            CollectionKey::from_bare(database_id, collection),
            TenantId::new(tenant_id),
            Surrogate::new(*surrogate),
        )? {
            holders.push((collection.clone(), *surrogate, pk));
        }
    }
    Ok(holders)
}

/// Fail when any carried surrogate is bound to a key other than the one it
/// carries, on this node or on the leader of one of its homes, or when the
/// re-issue itself carries it for two keys.
///
/// `carried` is an owned `Vec`, never a generic iterator: an iterator of
/// borrowing closures in this future's type makes the caller's future fail
/// its `Send` check.
pub(crate) async fn check_carried_binds(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    carried: Vec<CarriedBind>,
) -> Result<(), Error> {
    let mut expected: BTreeMap<(String, u32), Vec<u8>> = BTreeMap::new();
    for bind in carried {
        let slot = (bind.collection.clone(), bind.surrogate);
        match expected.get(&slot) {
            Some(pk) if *pk != bind.pk => return Err(conflict(&bind, pk)),
            Some(_) => {}
            None => {
                expected.insert(slot, bind.pk);
            }
        }
    }
    if expected.is_empty() {
        return Ok(());
    }

    let by_node = asked_nodes(state, tenant_id, database_id, &expected)?;
    let answers = join_all(by_node.into_iter().map(|(node_id, entries)| async move {
        if node_id == state.node_id {
            local_holders(state, tenant_id, database_id, &entries)
        } else {
            remote_holders(state, node_id, tenant_id, database_id, entries).await
        }
    }))
    .await;
    for answer in answers {
        for (collection, surrogate, holder) in answer? {
            if let Some(pk) = expected.get(&(collection.clone(), surrogate))
                && *pk != holder
            {
                let bind = CarriedBind {
                    collection,
                    pk: pk.clone(),
                    surrogate,
                };
                return Err(conflict(&bind, &holder));
            }
        }
    }
    Ok(())
}

/// The entries each node answers for: this node answers for all of them, and
/// the leader of each home of a carried bind answers for that bind.
fn asked_nodes(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    expected: &BTreeMap<(String, u32), Vec<u8>>,
) -> Result<BTreeMap<u64, Vec<(String, u32)>>, Error> {
    let all: Vec<(String, u32)> = expected.keys().cloned().collect();
    let mut by_node: BTreeMap<u64, Vec<(String, u32)>> = BTreeMap::new();
    by_node.insert(state.node_id, all);
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Ok(by_node);
    };
    let catalog = state.credentials.catalog();
    let mut holds_edges: HashMap<String, bool> = HashMap::new();
    for ((collection, surrogate), pk) in expected {
        let edges = match holds_edges.get(collection) {
            Some(edges) => *edges,
            None => {
                let edges = catalog
                    .get_collection(database_id, tenant_id, collection)?
                    .is_some_and(|coll| coll.has_implicit_edges);
                holds_edges.insert(collection.clone(), edges);
                edges
            }
        };
        let homes = RecordHomes::of(HomedRecord::Bind {
            collection: CollectionKey::from_bare(database_id, collection),
            key: pk,
            holds_edges: edges,
        });
        let table = routing.read().unwrap_or_else(|p| p.into_inner());
        for home in homes.iter() {
            let leader = table
                .group_for_vshard(home.as_u32())
                .ok()
                .and_then(|group| table.group_info(group))
                .and_then(|info| {
                    std::iter::once(info.leader)
                        .chain(info.members.iter().copied())
                        .find(|node| *node != 0)
                });
            if let Some(node) = leader
                && node != state.node_id
            {
                by_node
                    .entry(node)
                    .or_default()
                    .push((collection.clone(), *surrogate));
            }
        }
    }
    Ok(by_node)
}

/// Ask `node_id` which key it binds each entry to.
async fn remote_holders(
    state: &SharedState,
    node_id: u64,
    tenant_id: u64,
    database_id: DatabaseId,
    entries: Vec<(String, u32)>,
) -> Result<Vec<Holder>, Error> {
    let plan = PhysicalPlan::ClusterEvent(ClusterEventOp::SurrogateHolders {
        tenant_id,
        database_id,
        entries,
    });
    let body = snapshot_remote(state, node_id, tenant_id, database_id, &plan).await?;
    decode_holders(&body, node_id)
}

/// Encode holders for the wire.
pub(crate) fn encode_holders(holders: Vec<Holder>) -> Result<Vec<u8>, Error> {
    zerompk::to_msgpack_vec(&holders).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("surrogate holders: encode: {e}"),
    })
}

fn decode_holders(bytes: &[u8], node_id: u64) -> Result<Vec<Holder>, Error> {
    zerompk::from_msgpack(bytes).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("surrogate holders: decode the answer of node {node_id}: {e}"),
    })
}

/// The conflict error of `bind`, whose surrogate `holder` already holds. The
/// detail is the text a client reads, so it names the collection too.
fn conflict(bind: &CarriedBind, holder: &[u8]) -> Error {
    Error::RejectedConstraint {
        collection: bind.collection.clone(),
        constraint: SURROGATE_IDENTITY.into(),
        detail: format!(
            "{SURROGATE_IDENTITY}: restored key '{}' of collection '{}' carries surrogate {}, \
             which key '{}' already holds; nothing was re-issued",
            String::from_utf8_lossy(&bind.pk),
            bind.collection,
            bind.surrogate,
            String::from_utf8_lossy(holder)
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bind(pk: &str, surrogate: u32) -> CarriedBind {
        CarriedBind {
            collection: "docs".into(),
            pk: pk.as_bytes().to_vec(),
            surrogate,
        }
    }

    #[test]
    fn a_conflict_names_the_collection_key_and_surrogate() {
        let err = conflict(&bind("a", 7), b"b");
        let Error::RejectedConstraint {
            collection,
            constraint,
            detail,
        } = err
        else {
            panic!("a surrogate conflict is a constraint error");
        };
        assert_eq!(collection, "docs");
        assert_eq!(constraint, SURROGATE_IDENTITY);
        assert!(detail.contains("'a'") && detail.contains("7") && detail.contains("'b'"));
        assert!(
            detail.contains("'docs'"),
            "the client-visible detail names the collection: {detail}"
        );
    }

    #[test]
    fn holders_round_trip_the_wire() {
        let holders = vec![("docs".to_string(), 7u32, b"a".to_vec())];
        let bytes = encode_holders(holders.clone()).expect("encode");
        assert_eq!(decode_holders(&bytes, 2).expect("decode"), holders);
    }
}
