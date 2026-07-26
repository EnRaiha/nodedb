// SPDX-License-Identifier: BUSL-1.1

//! Native (MessagePack) protocol client construction for [`TestClusterNode`].
//!
//! `NativeClient::connect` / `PoolConfig::default()` authenticate as trust
//! user `"admin"`, but the cluster harness bootstraps exactly ONE trust
//! identity per node — [`super::lifecycle::HARNESS_SUPERUSER`] (see
//! `lifecycle::spawn_full`'s `credentials.bootstrap_trust_superuser(...)`),
//! the same identity the pre-wired pgwire `client` field connects as. A bare
//! `NativeClient::connect` against this harness therefore ALWAYS fails auth
//! with `trust user 'admin' does not exist`. Use the helpers below instead
//! of hand-rolling a `PoolConfig` at each test call site.

use nodedb_client::NativeClient;
use nodedb_client::native::pool::PoolConfig;
use nodedb_types::protocol::AuthMethod;

use super::lifecycle::{HARNESS_SUPERUSER, TestClusterNode};

impl TestClusterNode {
    /// A `NativeClient` pool-connected to this node's native listener,
    /// authenticated as the harness's bootstrapped trust superuser.
    pub fn native_client(&self) -> NativeClient {
        self.native_client_with(PoolConfig::default())
    }

    /// Same as [`Self::native_client`], but starting from a caller-supplied
    /// `PoolConfig` for fields the harness doesn't dictate — e.g. a test
    /// pinning `max_size: 1` so every call rides one socket/session for an
    /// in-transaction sequence. `addr` and `auth` are always overridden to
    /// this node's native port and the harness superuser, so callers cannot
    /// reintroduce the `admin` mismatch by way of `base`.
    pub fn native_client_with(&self, base: PoolConfig) -> NativeClient {
        NativeClient::new(PoolConfig {
            addr: format!("127.0.0.1:{}", self.native_port),
            auth: AuthMethod::Trust {
                username: HARNESS_SUPERUSER.to_string(),
            },
            ..base
        })
    }
}
