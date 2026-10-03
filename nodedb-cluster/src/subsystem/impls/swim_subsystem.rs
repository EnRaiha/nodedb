// SPDX-License-Identifier: BUSL-1.1

//! [`SwimSubsystem`] — wraps the SWIM failure detector lifecycle.
//!
//! This is the root subsystem (no dependencies) that spawns the SWIM
//! run loop with all membership subscribers attached **before** the
//! UDP socket starts exchanging probes. This eliminates the first-rumour
//! race: `spawn_with_members` adds subscribers to the
//! `FailureDetector` before `detector.run()` is called, so the very
//! first `on_state_change` callback fires on the first probe round.
//!
//! The UDP socket is bound before cluster startup so bootstrap, join, and
//! restart can advertise its address. The detector is seeded from every
//! peer's advertised SWIM address in topology, with real node ids.
//!
//! Subscribers attached here:
//! - [`RoutingLivenessHook`] — invalidates routing leader hints when
//!   SWIM marks a peer Suspect / Dead / Left.
//!
//! The rebalancer kick hook is wired separately by
//! [`RebalancerSubsystem`] via the kick `Arc<Notify>` returned during
//! construction.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use nodedb_types::NodeId;
use tokio::sync::watch;

use crate::routing::RoutingTable;
use crate::routing_liveness::{NodeIdResolver, RoutingLivenessHook};
use crate::swim::bootstrap::{SwimHandle, spawn_with_members};
use crate::swim::config::SwimConfig;
use crate::swim::detector::{Transport, UdpTransport};
use crate::swim::incarnation_store::IncarnationStore;
use crate::swim::subscriber::MembershipSubscriber;
use crate::topology::{ClusterTopology, NodeState};

use super::super::context::BootstrapCtx;
use super::super::errors::{BootstrapError, ShutdownError};
use super::super::health::SubsystemHealth;
use super::super::r#trait::{ClusterSubsystem, SubsystemHandle};

/// Configuration required to start the SWIM failure detector.
#[derive(Clone)]
pub struct SwimSubsystemConfig {
    /// SWIM protocol tuning.
    pub swim: SwimConfig,
    /// This node's stable identity (used in member records).
    pub local_id: NodeId,
    /// Persists self-refutation incarnation bumps across restarts.
    /// Production passes a catalog-backed store.
    pub incarnation_store: Option<Arc<dyn IncarnationStore>>,
}

/// What the host supplies to run SWIM: the socket it bound before cluster
/// startup, and extra subscribers to attach before the first probe.
pub struct SwimWiring {
    pub transport: Arc<UdpTransport>,
    pub subscribers: Vec<Arc<dyn MembershipSubscriber>>,
}

/// Owns the SWIM failure detector lifetime.
///
/// `RoutingLivenessHook` is attached as a subscriber before the run
/// loop starts; other subsystems (Rebalancer) attach additional
/// subscribers via `extra_subscribers` at construction time.
pub struct SwimSubsystem {
    cfg: SwimSubsystemConfig,
    routing: Arc<RwLock<RoutingTable>>,
    topology: Arc<RwLock<ClusterTopology>>,
    /// UDP socket bound before cluster startup, so its address could be
    /// advertised in this node's topology entry.
    transport: Arc<UdpTransport>,
    extra_subscribers: Vec<Arc<dyn MembershipSubscriber>>,
    /// Running detector after `start()`. Shared with the subsystem handle's
    /// task; whichever of it and `shutdown()` runs first stops the detector.
    handle: Arc<Mutex<Option<SwimHandle>>>,
}

impl SwimSubsystem {
    pub fn new(
        cfg: SwimSubsystemConfig,
        routing: Arc<RwLock<RoutingTable>>,
        topology: Arc<RwLock<ClusterTopology>>,
        transport: Arc<UdpTransport>,
        extra_subscribers: Vec<Arc<dyn MembershipSubscriber>>,
    ) -> Self {
        Self {
            cfg,
            routing,
            topology,
            transport,
            extra_subscribers,
            handle: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl ClusterSubsystem for SwimSubsystem {
    fn name(&self) -> &'static str {
        "swim"
    }

    fn dependencies(&self) -> &'static [&'static str] {
        &[]
    }

    async fn start(&self, _ctx: &BootstrapCtx) -> Result<SubsystemHandle, BootstrapError> {
        // Build a resolver that maps SWIM NodeId strings to routing-table
        // numeric ids by scanning the live topology.
        let topology = Arc::clone(&self.topology);
        let resolver: NodeIdResolver = Arc::new(move |node_id| {
            let topo = topology.read().unwrap_or_else(|p| p.into_inner());
            // Topology nodes are keyed by numeric u64; the SWIM NodeId
            // carries the same numeric id encoded as a decimal string in
            // production (placeholder "seed:…" entries are handled by the
            // `None` return below which is silently ignored by the hook).
            node_id
                .as_str()
                .parse::<u64>()
                .ok()
                .filter(|&id| topo.get_node(id).is_some())
        });

        // The local id is the node's numeric id as a decimal string.
        let local_node_id = self.cfg.local_id.as_str().parse::<u64>().map_err(|e| {
            BootstrapError::SubsystemStart {
                name: "swim",
                cause: Box::new(e),
            }
        })?;
        let routing_hook = Arc::new(RoutingLivenessHook::new(
            Arc::clone(&self.routing),
            resolver,
            local_node_id,
        ));

        let mut subscribers: Vec<Arc<dyn MembershipSubscriber>> = vec![routing_hook];
        subscribers.extend(self.extra_subscribers.iter().cloned());

        let local_addr = self.transport.local_addr();
        let peers = topology_swim_peers(
            &self.topology.read().unwrap_or_else(|p| p.into_inner()),
            &self.cfg.local_id,
        );

        let swim_handle = spawn_with_members(
            self.cfg.swim.clone(),
            self.cfg.local_id.clone(),
            local_addr,
            peers,
            Arc::clone(&self.transport) as Arc<dyn Transport>,
            subscribers,
            self.cfg.incarnation_store.clone(),
        )
        .await
        .map_err(|e| BootstrapError::SubsystemStart {
            name: "swim",
            cause: Box::new(e),
        })?;

        {
            let mut guard = self.handle.lock().unwrap_or_else(|p| p.into_inner());
            *guard = Some(swim_handle);
        }

        // The subsystem handle's task owns the detector's lifetime: a shutdown
        // signal, or the handle being dropped, stops the detector so its
        // socket stops answering probes.
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let slot = Arc::clone(&self.handle);
        let join = tokio::spawn(async move {
            // A closed channel means the handle was dropped: stop as well.
            let _ = shutdown_rx.wait_for(|stop| *stop).await;
            let taken = slot.lock().unwrap_or_else(|p| p.into_inner()).take();
            if let Some(swim_handle) = taken {
                swim_handle.shutdown().await;
            }
        });
        let subsystem_handle = SubsystemHandle::new("swim", join, shutdown_tx);

        Ok(subsystem_handle)
    }

    async fn shutdown(&self, deadline: Instant) -> Result<(), ShutdownError> {
        let maybe_handle = {
            let mut guard = self.handle.lock().unwrap_or_else(|p| p.into_inner());
            guard.take()
        };
        let Some(swim_handle) = maybe_handle else {
            return Ok(());
        };

        let timeout = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(timeout, swim_handle.shutdown()).await {
            Ok(()) => Ok(()),
            Err(_elapsed) => Err(ShutdownError::DeadlineExceeded { name: "swim" }),
        }
    }

    fn health(&self) -> SubsystemHealth {
        let guard = self.handle.lock().unwrap_or_else(|p| p.into_inner());
        if guard.is_some() {
            SubsystemHealth::Running
        } else {
            SubsystemHealth::Stopped
        }
    }
}

/// SWIM seeds for every other node in `topology` that advertises a SWIM
/// address. The QUIC address is never used: it is a different socket.
fn topology_swim_peers(topology: &ClusterTopology, local_id: &NodeId) -> Vec<(NodeId, SocketAddr)> {
    topology
        .all_nodes()
        .filter(|n| n.state != NodeState::Decommissioned)
        .filter_map(|n| {
            // A decimal u64 is always a valid id: non-empty, short, no NUL.
            let id = NodeId::from_validated(n.node_id.to_string());
            if &id == local_id {
                return None;
            }
            n.swim_socket_addr().map(|addr| (id, addr))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_codec::MacKey;
    use crate::topology::NodeInfo;

    fn dummy_cfg() -> SwimSubsystemConfig {
        SwimSubsystemConfig {
            swim: crate::swim::config::SwimConfig {
                probe_interval: std::time::Duration::from_millis(100),
                probe_timeout: std::time::Duration::from_millis(40),
                indirect_probes: 2,
                suspicion_mult: 4,
                min_suspicion: std::time::Duration::from_millis(500),
                initial_incarnation: crate::swim::incarnation::Incarnation::ZERO,
                max_piggyback: 6,
                fanout_lambda: 3,
            },
            local_id: NodeId::try_new("1").expect("test fixture"),
            incarnation_store: None,
        }
    }

    async fn subsystem() -> SwimSubsystem {
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap(), MacKey::zero())
            .await
            .expect("bind a free port");
        let routing = Arc::new(RwLock::new(RoutingTable::uniform(1, &[1], 1)));
        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        SwimSubsystem::new(dummy_cfg(), routing, topology, Arc::new(transport), vec![])
    }

    #[tokio::test]
    async fn swim_subsystem_name_and_deps() {
        let s = subsystem().await;
        assert_eq!(s.name(), "swim");
        assert!(s.dependencies().is_empty());
    }

    #[tokio::test]
    async fn health_is_stopped_before_start() {
        let s = subsystem().await;
        assert_eq!(s.health(), SubsystemHealth::Stopped);
    }

    /// Seeds come from each peer's advertised SWIM address, never its QUIC
    /// address, and skip this node, decommissioned nodes, and nodes with none.
    #[test]
    fn seeds_are_the_advertised_swim_addresses() {
        let mut topo = ClusterTopology::new();
        topo.add_node(
            NodeInfo::new(1, "10.0.0.1:9400".parse().unwrap(), NodeState::Active)
                .with_swim_addr("10.0.0.1:9401".parse().ok()),
        );
        topo.add_node(
            NodeInfo::new(2, "10.0.0.2:9400".parse().unwrap(), NodeState::Active)
                .with_swim_addr("10.0.0.2:9401".parse().ok()),
        );
        topo.add_node(NodeInfo::new(
            3,
            "10.0.0.3:9400".parse().unwrap(),
            NodeState::Active,
        ));
        topo.add_node(
            NodeInfo::new(
                4,
                "10.0.0.4:9400".parse().unwrap(),
                NodeState::Decommissioned,
            )
            .with_swim_addr("10.0.0.4:9401".parse().ok()),
        );

        let local = NodeId::try_new("1").expect("test fixture");
        let peers = topology_swim_peers(&topo, &local);
        assert_eq!(
            peers,
            vec![(
                NodeId::try_new("2").expect("test fixture"),
                "10.0.0.2:9401".parse::<SocketAddr>().unwrap()
            )]
        );
    }
}
