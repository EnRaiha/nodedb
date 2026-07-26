// SPDX-License-Identifier: BUSL-1.1

//! Native (MessagePack) protocol client construction for [`TestClusterNode`].
//!
//! `PoolConfig` has no default identity (trust auth is passwordless, so a
//! defaulted identity is a privilege-escalation footgun — see
//! `nodedb_client::native::pool::PoolConfig::new`'s doc comment). The cluster
//! harness bootstraps exactly ONE trust identity per node —
//! [`super::lifecycle::HARNESS_SUPERUSER`] (see `lifecycle::spawn_full`'s
//! `credentials.bootstrap_trust_superuser(...)`), the same identity the
//! pre-wired pgwire `client` field connects as. Use the helpers below instead
//! of hand-rolling a `PoolConfig` at each test call site.

use nodedb_client::NativeClient;
use nodedb_client::native::pool::PoolConfig;
use nodedb_types::protocol::AuthMethod;

use super::lifecycle::{HARNESS_SUPERUSER, TestClusterNode};

impl TestClusterNode {
    /// A `NativeClient` pool-connected to this node's native listener,
    /// authenticated as the harness's bootstrapped trust superuser.
    pub fn native_client(&self) -> NativeClient {
        self.native_client_with(|base| base)
    }

    /// Same as [`Self::native_client`], but letting the caller tune fields
    /// the harness doesn't dictate — e.g. a test pinning `max_size: 1` so
    /// every call rides one socket/session for an in-transaction sequence.
    ///
    /// `configure` receives a `PoolConfig` whose `addr`/`auth` are already
    /// this node's native port and the harness superuser; it's applied
    /// *after* those fields are set, so a `configure` closure can only add
    /// tuning on top — e.g. `|base| PoolConfig { max_size: 1, ..base }` —
    /// never reintroduce the identity-less-default mismatch.
    pub fn native_client_with(
        &self,
        configure: impl FnOnce(PoolConfig) -> PoolConfig,
    ) -> NativeClient {
        let base = PoolConfig::new(
            format!("127.0.0.1:{}", self.native_port),
            AuthMethod::Trust {
                username: HARNESS_SUPERUSER.to_string(),
            },
        );
        NativeClient::new(configure(base))
    }
}
