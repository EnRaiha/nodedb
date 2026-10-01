// SPDX-License-Identifier: BUSL-1.1

//! The cluster node's [`HomeSurrogateAuthority`]: the `SurrogateAssigner`
//! asks the routed exchange for the value a key's collection home bound.
//!
//! Every method awaits the routed exchange: a Raft proposal on the home
//! leader, or an RPC to it. No method blocks a runtime worker.

use std::sync::{Arc, Weak};

use futures::FutureExt;
use futures::future::BoxFuture;
use nodedb_types::{CollectionKey, Surrogate, TenantId};

use crate::control::state::SharedState;
use crate::control::surrogate::HomeSurrogateAuthority;
use crate::types::TraceId;

use super::resolve::{assign_surrogates_routed, lookup_surrogates_routed};

/// Resolves keys through `surrogate_exchange`, at the leader of each key's
/// collection home.
pub struct RoutedHomeAuthority {
    state: Weak<SharedState>,
}

impl RoutedHomeAuthority {
    pub fn new(state: Weak<SharedState>) -> Self {
        Self { state }
    }

    fn state(&self) -> crate::Result<Arc<SharedState>> {
        self.state.upgrade().ok_or_else(|| crate::Error::Internal {
            detail: "surrogate home authority: the node is shutting down".into(),
        })
    }
}

impl HomeSurrogateAuthority for RoutedHomeAuthority {
    fn assign<'a>(
        &'a self,
        key: CollectionKey<'a>,
        tenant_id: TenantId,
        pks: &'a [&'a [u8]],
    ) -> BoxFuture<'a, crate::Result<Vec<Surrogate>>> {
        async move {
            let state = self.state()?;
            assign_surrogates_routed(&state, key, tenant_id, pks, TraceId::ZERO).await
        }
        .boxed()
    }

    fn lookup_many<'a>(
        &'a self,
        key: CollectionKey<'a>,
        tenant_id: TenantId,
        pks: &'a [&'a [u8]],
    ) -> BoxFuture<'a, crate::Result<Vec<Option<Surrogate>>>> {
        async move {
            let state = self.state()?;
            lookup_surrogates_routed(&state, key, tenant_id, pks, TraceId::ZERO).await
        }
        .boxed()
    }
}
