// SPDX-License-Identifier: BUSL-1.1

//! Server-side `JoinRequest` handler: build a `JoinResponse` from current cluster state.
//!
//! Called by [`crate::raft_loop::handle_rpc`] on the group-0 leader when a
//! `JoinRequest` arrives. This function is the single source of truth for how
//! a new node is admitted into the topology wire response; the Raft conf-change
//! that actually replicates membership across groups is driven separately from
//! the RPC arm.
//!
//! Semantics:
//!
//! - **New node**: added to topology as `Active`, full wire response returned.
//! - **Known node, same address**: idempotent. The entry changes only to
//!   normalize its state to `Active` or to adopt a newly advertised SWIM address.
//! - **Known node, different address**: rejected with `success: false`. This
//!   catches node-id reuse (operator error or a ghost node coming back with a
//!   stale id on a new address).
//! - **Invalid `listen_addr` or `swim_addr` in the request**: rejected with
//!   `success: false`.

use std::net::SocketAddr;

use tracing::warn;

use crate::routing::RoutingTable;
use crate::rpc_codec::{JoinGroupInfo, JoinNodeInfo, JoinRequest, JoinResponse};
use crate::topology::{CLUSTER_WIRE_FORMAT_VERSION, ClusterTopology, NodeInfo, NodeState};

/// Build a `JoinResponse` for an incoming `JoinRequest`.
///
/// See module docs for semantics. Mutates `topology` only when the node is
/// newly admitted; idempotent for re-joins with the same address.
///
/// `cluster_id` is the id of the cluster this node belongs to — the
/// join flow reads it from the local catalog and threads it through so
/// the joining node can persist it and take the `restart()` path on a
/// subsequent boot. Zero is a valid placeholder when the server's
/// catalog has not yet been populated; rejection responses also carry
/// zero.
pub fn handle_join_request(
    req: &JoinRequest,
    topology: &mut ClusterTopology,
    routing: &RoutingTable,
    cluster_id: u64,
) -> JoinResponse {
    // Validate the wire version carried in the JOIN payload (belt-and-suspenders
    // check; the transport-level handshake already negotiated a compatible version
    // before this RPC was dispatched). The `wire_version` field here is the
    // cluster-wide schema version (`CLUSTER_WIRE_FORMAT_VERSION`), distinct from
    // the transport-level RPC frame version. We require an exact match because
    // this build uses floor == ceiling (no backward-compat window in the schema).
    if req.wire_version != CLUSTER_WIRE_FORMAT_VERSION {
        warn!(
            node_id = req.node_id,
            joiner_wire_version = req.wire_version,
            expected_wire_version = CLUSTER_WIRE_FORMAT_VERSION,
            "join request rejected: joiner cluster wire_version mismatch"
        );
        return reject(format!(
            "joiner wire_version {} does not match this cluster's wire_version {} — \
             all nodes must run one build before 1.0; restart every node on the same build",
            req.wire_version, CLUSTER_WIRE_FORMAT_VERSION
        ));
    }

    // Wire shapes can change without a `CLUSTER_WIRE_FORMAT_VERSION` bump (see
    // `nodedb_types::wire_version`), so the version check above cannot by
    // itself prevent two different builds from joining the same cluster and
    // misdecoding each other. Build identity is the invariant that actually
    // protects this — require an exact match.
    let local_build_id = nodedb_types::wire_version::WIRE_BUILD_ID;
    if req.build_id != local_build_id {
        warn!(
            node_id = req.node_id,
            joiner_build_id = %req.build_id,
            local_build_id,
            "join request rejected: joiner build_id mismatch"
        );
        return reject(format!(
            "joiner build {} does not match this cluster's build {local_build_id} — \
             all nodes must run one build before 1.0; restart every node on the same build",
            req.build_id
        ));
    }

    // Validate the listen address early.
    let addr: SocketAddr = match req.listen_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            return reject(format!("invalid listen_addr '{}': {e}", req.listen_addr));
        }
    };

    let swim_addr: Option<SocketAddr> = match req.swim_addr.as_deref() {
        Some(raw) => match raw.parse() {
            Ok(a) => Some(a),
            Err(e) => return reject(format!("invalid swim_addr '{raw}': {e}")),
        },
        None => None,
    };

    let spki_pin: Option<[u8; 32]> = match req.spki_pin.as_deref() {
        Some(bytes) if bytes.len() == 32 => {
            let mut pin = [0u8; 32];
            pin.copy_from_slice(bytes);
            Some(pin)
        }
        Some(_) => return reject("spki_pin must contain exactly 32 bytes".into()),
        None => None,
    };

    // Collision / idempotency check — both require reading the existing entry.
    if let Some(existing) = topology.get_node(req.node_id) {
        let existing_addr = existing.addr.clone();
        if existing_addr != req.listen_addr {
            // Same id, different address — reject.
            return reject(format!(
                "node_id {} already registered with different address {} (request: {})",
                req.node_id, existing_addr, req.listen_addr
            ));
        }
        if existing.spki_pin != spki_pin {
            return reject(format!(
                "node_id {} is already registered with a different SPKI pin",
                req.node_id
            ));
        }
        // Same id, same address, same identity: normalize to Active and adopt
        // the SWIM address the node advertises now. A restarted node may have
        // bound a different one.
        let needs_active = existing.state != NodeState::Active;
        let advertised = swim_addr.map(|a| a.to_string());
        let swim_changed = existing.swim_addr != advertised;
        if (needs_active || swim_changed)
            && let Some(mut entry) = topology.get_node(req.node_id).cloned()
        {
            entry.state = NodeState::Active;
            entry.swim_addr = advertised;
            // `add_node` replaces the entry and bumps the topology version,
            // so the change reaches peers through the topology broadcast.
            topology.add_node(entry);
        }
        return build_response(topology, routing, cluster_id);
    }

    if let Some(pin) = spki_pin
        && let Some(owner) = topology
            .all_nodes()
            .find(|node| node.spki_pin == Some(pin) && node.node_id != req.node_id)
    {
        return reject(format!(
            "SPKI pin is already registered to node_id {}",
            owner.node_id
        ));
    }

    // Brand new node — admit as Active. Stamp the joiner's own
    // wire version and identity fields onto its NodeInfo so every
    // peer that replays this topology has the correct version and
    // identity pins.
    topology.add_node(
        NodeInfo::new(req.node_id, addr, NodeState::Active)
            .with_wire_version(req.wire_version)
            .with_spiffe_id(req.spiffe_id.clone())
            .with_spki_pin(spki_pin)
            .with_swim_addr(swim_addr),
    );
    build_response(topology, routing, cluster_id)
}

/// Build a successful `JoinResponse` from the current topology and routing.
fn build_response(
    topology: &ClusterTopology,
    routing: &RoutingTable,
    cluster_id: u64,
) -> JoinResponse {
    let nodes: Vec<JoinNodeInfo> = topology.all_nodes().map(NodeInfo::to_wire).collect();

    let groups: Vec<JoinGroupInfo> = routing
        .group_members()
        .iter()
        .map(|(&gid, info)| JoinGroupInfo {
            group_id: gid,
            leader: info.leader,
            members: info.members.clone(),
            learners: info.learners.clone(),
        })
        .collect();

    JoinResponse {
        success: true,
        error: String::new(),
        cluster_id,
        nodes,
        vshard_to_group: routing.vshard_to_group().to_vec(),
        groups,
    }
}

/// Build a rejection response with the given error message.
fn reject(error: String) -> JoinResponse {
    JoinResponse {
        success: false,
        error,
        cluster_id: 0,
        nodes: vec![],
        vshard_to_group: vec![],
        groups: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topo_with_one_node() -> ClusterTopology {
        let mut topology = ClusterTopology::new();
        topology.add_node(NodeInfo::new(
            1,
            "10.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        topology
    }

    #[test]
    fn handle_join_request_adds_node() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(2, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: None,
            swim_addr: None,
        };

        let resp = handle_join_request(&req, &mut topology, &routing, 42);

        assert!(resp.success);
        assert_eq!(resp.nodes.len(), 2);
        assert_eq!(resp.vshard_to_group.len(), 1024);
        // uniform(2, ...) creates 2 data groups + 1 metadata group = 3 total.
        assert_eq!(resp.groups.len(), 3);

        assert!(topology.contains(2));
        assert_eq!(topology.node_count(), 2);
    }

    #[test]
    fn handle_join_request_idempotent() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: None,
            swim_addr: None,
        };

        let _ = handle_join_request(&req, &mut topology, &routing, 42);
        let resp = handle_join_request(&req, &mut topology, &routing, 42);

        assert!(resp.success);
        assert_eq!(resp.nodes.len(), 2); // Still 2, not 3.
        assert_eq!(topology.node_count(), 2);
    }

    /// A second join with the same id+addr must not mutate topology at all
    /// (no duplicate entries, no state reset). Verify by capturing
    /// `node_count` and the node ordering between calls.
    #[test]
    fn handle_join_request_idempotent_no_mutation() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: None,
            swim_addr: None,
        };

        let resp1 = handle_join_request(&req, &mut topology, &routing, 7);
        let ids_before: Vec<u64> = topology.all_nodes().map(|n| n.node_id).collect();
        let count_before = topology.node_count();

        let resp2 = handle_join_request(&req, &mut topology, &routing, 7);
        assert_eq!(resp1.cluster_id, 7);
        assert_eq!(resp2.cluster_id, 7);
        let ids_after: Vec<u64> = topology.all_nodes().map(|n| n.node_id).collect();

        assert!(resp1.success && resp2.success);
        assert_eq!(count_before, topology.node_count());
        assert_eq!(ids_before, ids_after);
        assert_eq!(resp2.nodes.len(), 2);
        // Node 2 must still be Active.
        let n2 = topology.get_node(2).unwrap();
        assert_eq!(n2.state, NodeState::Active);
    }

    /// Same id, different address → reject.
    #[test]
    fn handle_join_request_rejects_id_collision() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);

        // First join: node 2 at 10.0.0.2:9400.
        let req1 = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: None,
            swim_addr: None,
        };
        let resp1 = handle_join_request(&req1, &mut topology, &routing, 11);
        assert!(resp1.success);

        // Second join: same id, different address — must be rejected.
        let req2 = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.99:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: None,
            swim_addr: None,
        };
        let resp2 = handle_join_request(&req2, &mut topology, &routing, 11);

        assert!(!resp2.success);
        assert!(
            resp2.error.contains("already registered"),
            "error should mention collision: {}",
            resp2.error
        );
        // Topology must not be clobbered.
        assert_eq!(topology.node_count(), 2);
        let n2 = topology.get_node(2).unwrap();
        assert_eq!(n2.addr, "10.0.0.2:9400");
    }

    #[test]
    fn handle_join_rejects_spki_owned_by_another_node() {
        let pin = [0x5a; 32];
        let mut topology = topo_with_one_node();
        topology.get_node_mut(1).unwrap().spki_pin = Some(pin);
        let routing = RoutingTable::uniform(1, &[1], 1);
        let request = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: Some("spiffe://nodedb/node/2".into()),
            spki_pin: Some(pin.to_vec()),
            swim_addr: None,
        };

        let response = handle_join_request(&request, &mut topology, &routing, 11);
        assert!(!response.success);
        assert!(response.error.contains("already registered to node_id 1"));
        assert!(!topology.contains(2));
    }

    /// A joiner running a different build is rejected with the
    /// operator-facing "restart every node" message, not a rolling-upgrade
    /// claim.
    #[test]
    fn handle_join_rejects_build_id_mismatch() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: "some-other-build".into(),
            spiffe_id: None,
            spki_pin: None,
            swim_addr: None,
        };

        let resp = handle_join_request(&req, &mut topology, &routing, 42);

        assert!(!resp.success);
        assert!(resp.error.contains("some-other-build"));
        assert!(
            resp.error
                .contains("all nodes must run one build before 1.0"),
            "rejection error must name the fix: {}",
            resp.error
        );
        assert!(!topology.contains(2));
    }

    #[test]
    fn handle_join_invalid_addr() {
        let mut topology = ClusterTopology::new();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "not-a-valid-address".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: None,
            swim_addr: None,
        };

        let resp = handle_join_request(&req, &mut topology, &routing, 42);
        assert!(!resp.success);
        assert!(!resp.error.is_empty());
    }

    fn join_with_swim(swim_addr: &str) -> JoinRequest {
        JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: None,
            swim_addr: Some(swim_addr.into()),
        }
    }

    /// The joiner's SWIM address lands in its topology entry and in the wire
    /// response every peer seeds SWIM from.
    #[test]
    fn join_carries_the_swim_address() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let resp =
            handle_join_request(&join_with_swim("10.0.0.2:9401"), &mut topology, &routing, 1);

        assert!(resp.success, "{}", resp.error);
        let entry = topology.get_node(2).expect("joiner admitted");
        assert_eq!(entry.swim_socket_addr(), "10.0.0.2:9401".parse().ok());
        let wire = resp
            .nodes
            .iter()
            .find(|n| n.node_id == 2)
            .expect("joiner in response");
        assert_eq!(wire.swim_addr.as_deref(), Some("10.0.0.2:9401"));
    }

    /// A known node that re-advertises a different SWIM address updates its
    /// entry and bumps the topology version so peers pick the change up.
    #[test]
    fn rejoin_with_a_new_swim_address_updates_the_entry() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);
        let first =
            handle_join_request(&join_with_swim("10.0.0.2:9401"), &mut topology, &routing, 1);
        assert!(first.success, "{}", first.error);
        let version_before = topology.version();

        let second =
            handle_join_request(&join_with_swim("10.0.0.2:9501"), &mut topology, &routing, 1);

        assert!(second.success, "{}", second.error);
        assert_eq!(
            topology.get_node(2).and_then(NodeInfo::swim_socket_addr),
            "10.0.0.2:9501".parse().ok()
        );
        assert!(topology.version() > version_before);
    }

    #[test]
    fn join_rejects_an_invalid_swim_address() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);
        let resp = handle_join_request(
            &join_with_swim("not-an-address"),
            &mut topology,
            &routing,
            1,
        );
        assert!(!resp.success);
        assert!(resp.error.contains("swim_addr"), "{}", resp.error);
        assert!(!topology.contains(2));
    }
}
