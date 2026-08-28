// SPDX-License-Identifier: BUSL-1.1

//! Static Local/Cluster split, decided once at process start from
//! configured cluster membership — never from live topology gossip.

use super::cluster::ClusterCounter;
use super::local::LocalCounter;

/// Which counter backs a `SurrogateRegistry`.
pub enum SurrogateRegistryMode {
    Local(LocalCounter),
    Cluster(ClusterCounter),
}

impl SurrogateRegistryMode {
    pub fn is_cluster(&self) -> bool {
        matches!(self, SurrogateRegistryMode::Cluster(_))
    }
}
