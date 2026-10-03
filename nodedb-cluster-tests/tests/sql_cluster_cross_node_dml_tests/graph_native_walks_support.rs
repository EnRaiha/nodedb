// SPDX-License-Identifier: BUSL-1.1

//! Steps the native graph walk tests share: the data-group map of an RF1
//! cluster, node names that cover every group, and native walk calls.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use nodedb_test_support::native_harness::{open_trust_session, send_request};
use nodedb_types::id::VShardId;
use nodedb_types::protocol::{NativeResponse, OpCode, ResponseStatus, TextFields};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

use crate::common::cluster_harness::{TestCluster, wait_for};

pub(super) const MIN_NODES: usize = 24;
/// The trust superuser both harnesses bootstrap.
const SUPERUSER: &str = "nodedb";

/// The data groups of `cluster` (group 0 excluded) and the group each vShard
/// belongs to.
pub(super) fn data_groups(cluster: &TestCluster) -> (BTreeSet<u64>, HashMap<u32, u64>) {
    let routing = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let groups: BTreeSet<u64> = routing
        .group_ids()
        .into_iter()
        .filter(|g| *g != 0)
        .collect();
    let mut map = HashMap::new();
    for &g in &groups {
        for vs in routing.vshards_for_group(g) {
            map.insert(vs, g);
        }
    }
    (groups, map)
}

/// Wait until every group in `groups` has exactly one replica in `cluster`.
pub(super) async fn wait_one_replica_per_group(cluster: &TestCluster, groups: &BTreeSet<u64>) {
    wait_for(
        "each data group has exactly one replica",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            groups.iter().all(|&g| {
                cluster
                    .nodes
                    .iter()
                    .filter(|node| node.replicates_data_group(g))
                    .count()
                    == 1
            })
        },
    )
    .await;
}

/// Names `{prefix}0, {prefix}1, …`, enough that their key vShards cover every
/// data group.
pub(super) fn names(
    prefix: &str,
    groups: &BTreeSet<u64>,
    group_of: &HashMap<u32, u64>,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut covered = BTreeSet::new();
    let mut i = 0usize;
    while out.len() < MIN_NODES || covered != *groups {
        let name = format!("{prefix}{i}");
        let vshard = VShardId::from_key(name.as_bytes()).as_u32();
        covered.insert(group_of.get(&vshard).copied().unwrap_or(0));
        out.push(name);
        i += 1;
    }
    out
}

/// `len` names `{prefix}0, {prefix}1, …` whose consecutive entries home to
/// different data groups, so every hop of a chain over them crosses groups.
pub(super) fn spread_chain(prefix: &str, len: usize, group_of: &HashMap<u32, u64>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut last_group = None;
    let mut i = 0usize;
    while out.len() < len {
        let name = format!("{prefix}{i}");
        let group = group_of
            .get(&VShardId::from_key(name.as_bytes()).as_u32())
            .copied();
        if group != last_group {
            last_group = group;
            out.push(name);
        }
        i += 1;
        assert!(i < 100_000, "no spread chain of {len} names for '{prefix}'");
    }
    out
}

/// A native session authenticated as the trust superuser, in JSON framing.
pub(super) async fn native_session(port: u16) -> TcpStream {
    open_trust_session(port, SUPERUSER).await
}

/// Every node name in a walk response, in response order. A cell can hold a
/// name, a JSON text of names, or an array of either.
pub(super) fn walk_names(response: &NativeResponse) -> Vec<String> {
    fn from_json(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::String(s) => out.push(s.clone()),
            serde_json::Value::Array(items) => items.iter().for_each(|v| from_json(v, out)),
            serde_json::Value::Object(map) => map.values().for_each(|v| from_json(v, out)),
            _ => {}
        }
    }
    fn from_value(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(s) => match sonic_rs::from_str::<serde_json::Value>(s) {
                Ok(json @ (serde_json::Value::Array(_) | serde_json::Value::Object(_))) => {
                    from_json(&json, out)
                }
                _ => out.push(s.clone()),
            },
            Value::Array(items) => items.iter().for_each(|v| from_value(v, out)),
            Value::Object(map) => map.values().for_each(|v| from_value(v, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    for row in response.rows.iter().flatten() {
        row.iter().for_each(|cell| from_value(cell, &mut out));
    }
    out
}

/// Send `op` with `fields` as request `seq` and return the response. Panics
/// unless the status is `Ok`.
pub(super) async fn walk(
    stream: &mut TcpStream,
    seq: u64,
    op: OpCode,
    fields: TextFields,
) -> NativeResponse {
    let response = send_request(stream, seq, op, fields).await;
    assert_eq!(
        response.status,
        ResponseStatus::Ok,
        "native {op:?} must succeed: {response:?}"
    );
    response
}
