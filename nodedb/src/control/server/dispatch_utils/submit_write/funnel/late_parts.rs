// SPDX-License-Identifier: BUSL-1.1

//! The parts of a grouped write whose final response arrives after its
//! caller stopped waiting.
//!
//! The core can still apply the write. Its parts must then journal in the
//! order its rows reached storage, so the write keeps its guards and its
//! order fence until the final response arrives and the parts are appended.

use std::sync::Arc;

use tokio::sync::oneshot;

use crate::bridge::envelope::{Response, Status};
use crate::control::server::wal_dispatch::{self, GroupOrigin, WriteSetTarget};
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::WalManager;

use super::dispatch::DeferredGuards;

/// Where a grouped write journals its parts from a late final response.
pub(super) struct LateParts {
    pub response: oneshot::Receiver<Response>,
    pub wal: Arc<WalManager>,
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    pub collection: String,
    pub origin: GroupOrigin,
    pub apply_key: u64,
    pub event_source: crate::event::EventSource,
    pub commit_hlc: Option<u64>,
}

impl LateParts {
    /// Hold `guards` until the final response arrives, journal the parts it
    /// reports, then release them. A channel that closes with no final
    /// response leaves the outcome unknown: the write set the core stored
    /// settles at boot.
    pub(super) fn spawn(self, guards: DeferredGuards) {
        tokio::spawn(async move {
            let Ok(response) = self.response.await else {
                drop(guards);
                return;
            };
            if response.status == Status::Ok || !response.write_set.is_empty() {
                let appender = self
                    .wal
                    .appender(self.apply_key)
                    .with_event_source(self.event_source);
                let appender = match self.commit_hlc {
                    Some(hlc) => appender.with_commit_hlc(hlc),
                    None => appender,
                };
                let appended = wal_dispatch::append_group_parts(
                    appender,
                    WriteSetTarget {
                        tenant_id: self.tenant_id,
                        vshard_id: self.vshard_id,
                        database_id: self.database_id,
                        collection: &self.collection,
                        origin: self.origin,
                    },
                    &response.write_set,
                );
                if let Err(error) = appended {
                    tracing::error!(
                        %error,
                        origin = self.origin.lsn.as_u64(),
                        "a late write's parts failed to journal; the write set its core \
                         stored settles at boot"
                    );
                }
            }
            drop(guards);
        });
    }
}
