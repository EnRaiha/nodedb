// SPDX-License-Identifier: BUSL-1.1

//! Drop-releasable wrapper around the node's driving `tokio_postgres::Client`.
//!
//! Mirrors `pgwire_harness::TestClient`: wrapping the client in an `Option`
//! behind a `Deref` means every existing `node.client.simple_query(...)` /
//! `node.client.query(...)` call site across the cluster integration tests
//! keeps compiling unchanged (autoderef resolves through to
//! `tokio_postgres::Client`), while `graceful_shutdown_wal_only` gets a
//! `.take()` escape hatch to drop the connection FIRST — before touching any
//! redb handle — so the server-side pgwire session task drops its
//! `Arc<SharedState>` clone promptly instead of lingering until the
//! `conn_handle` task is aborted.

pub struct ClusterTestClient(Option<tokio_postgres::Client>);

impl ClusterTestClient {
    pub(super) fn new(client: tokio_postgres::Client) -> Self {
        Self(Some(client))
    }

    /// Take the client out, dropping the caller's handle to it. Used by
    /// `graceful_shutdown_wal_only` to close the driving connection before
    /// awaiting any other task.
    pub(super) fn take(&mut self) -> Option<tokio_postgres::Client> {
        self.0.take()
    }

    fn as_ref(&self) -> &tokio_postgres::Client {
        self.0.as_ref().expect("cluster test client already closed")
    }
}

impl std::ops::Deref for ClusterTestClient {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}
