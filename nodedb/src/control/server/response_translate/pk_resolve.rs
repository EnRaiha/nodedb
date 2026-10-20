// SPDX-License-Identifier: BUSL-1.1

//! Surrogate → primary-key resolution, the one naming step every search
//! response translator runs.
//!
//! A collection's bindings live on its home vShard's group. This node's
//! catalog answers first. In a cluster, every surrogate it binds no key to
//! goes to the home leader in one `SurrogateHolders` request. A surrogate
//! neither binds keeps no key, and the caller keeps the row's decimal.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::ClusterEventOp;
use nodedb_types::{CollectionKey, DatabaseId, Surrogate, TenantId};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::dispatcher::{DispatchRouteParams, dispatch_route};
use crate::control::gateway::live_leaders::resolve_live_decision;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::graph_dispatch::cluster_resolve::gateway_shared;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::types::TraceId;

/// `(collection, surrogate, pk)`: one answer of a `SurrogateHolders` request.
type Holder = (String, u32, Vec<u8>);

/// The primary keys a set of surrogates resolved to.
#[derive(Debug, Default)]
pub(crate) struct SurrogateKeys(HashMap<Surrogate, String>);

impl SurrogateKeys {
    /// The key `surrogate` resolved to. `None` when no binding names it.
    pub(crate) fn get(&self, surrogate: Surrogate) -> Option<&str> {
        self.0.get(&surrogate).map(String::as_str)
    }
}

/// The client identity of a row bound to `pk_bytes`. A headless row binds
/// the big-endian bytes of its own surrogate and carries the decimal
/// identity a minted row carries. Any other key is its UTF-8 text.
pub(crate) fn pk_identity(pk_bytes: &[u8], surrogate: Surrogate) -> Option<String> {
    if pk_bytes == surrogate.as_u32().to_be_bytes() {
        return Some(surrogate.as_u32().to_string());
    }
    std::str::from_utf8(pk_bytes).ok().map(str::to_owned)
}

/// Resolve every surrogate of `surrogates` to its primary key in
/// `collection`, the plan's database-qualified name. A name that does not
/// de-qualify resolves nothing. A catalog read error or a refused home
/// request fails the call: a row never answers a key nobody vouched for.
pub(crate) async fn resolve_surrogate_pks(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    surrogates: impl IntoIterator<Item = Surrogate>,
) -> crate::Result<SurrogateKeys> {
    let mut keys = SurrogateKeys::default();
    let Ok(key) = CollectionKey::from_qualified_str(database_id, collection) else {
        return Ok(keys);
    };
    let mut seen: HashSet<Surrogate> = HashSet::new();
    let wanted: Vec<Surrogate> = surrogates
        .into_iter()
        .filter(|surrogate| seen.insert(*surrogate))
        .collect();
    // Every surrogate of the response resolves in one catalog read.
    let bound = state
        .credentials
        .catalog()
        .get_pks_for_surrogates(key, tenant_id, &wanted)?;
    let mut misses: Vec<u32> = Vec::new();
    for (surrogate, pk) in wanted.into_iter().zip(bound) {
        match pk.and_then(|pk| pk_identity(&pk, surrogate)) {
            Some(identity) => {
                keys.0.insert(surrogate, identity);
            }
            None => misses.push(surrogate.as_u32()),
        }
    }
    if misses.is_empty() {
        return Ok(keys);
    }
    for (_, raw, pk) in home_holders(state, database_id, tenant_id, key, misses).await? {
        let surrogate = Surrogate::new(raw);
        if let Some(identity) = pk_identity(&pk, surrogate) {
            keys.0.insert(surrogate, identity);
        }
    }
    Ok(keys)
}

/// The keys the collection home leader binds `surrogates` to. Empty on a
/// single node and when this node leads the home: its catalog answered.
async fn home_holders(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    key: CollectionKey<'_>,
    surrogates: Vec<u32>,
) -> crate::Result<Vec<Holder>> {
    if state.cluster_routing.is_none() {
        return Ok(Vec::new());
    }
    let home = key.vshard().as_u32();
    let decision = resolve_live_decision(state, home);
    if matches!(decision, RouteDecision::Local) {
        return Ok(Vec::new());
    }
    let bare = key.name();
    let plan = PhysicalPlan::ClusterEvent(ClusterEventOp::SurrogateHolders {
        tenant_id: tenant_id.as_u64(),
        database_id,
        entries: surrogates
            .into_iter()
            .map(|surrogate| (bare.to_owned(), surrogate))
            .collect(),
    });
    let shared = gateway_shared(state)?;
    let outcome = dispatch_route(DispatchRouteParams {
        route: TaskRoute {
            plan,
            decision,
            vshard_id: home,
        },
        shared: &shared,
        tenant_id,
        database_id,
        trace_id: TraceId::ZERO,
        deadline_ms: StatementDeadline::of_statement(state).hop_budget_ms(),
        version_set: &GatewayVersionSet::from_pairs(Vec::new()),
        txn_id: None,
        linearizable: false,
        user_roles: &[],
    })
    .await?;
    let mut holders = Vec::new();
    for payload in outcome
        .payloads
        .iter()
        .filter(|payload| !payload.is_empty())
    {
        let mut answer: Vec<Holder> =
            zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "surrogate holders of '{}' from its home leader do not decode: {e}",
                    key.name()
                ),
            })?;
        holders.append(&mut answer);
    }
    Ok(holders)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_keyed_binding_resolves_to_decimal() {
        let surrogate = Surrogate::new(7);
        assert_eq!(
            pk_identity(&7u32.to_be_bytes(), surrogate).as_deref(),
            Some("7")
        );
    }

    #[test]
    fn a_text_key_resolves_to_itself() {
        assert_eq!(
            pk_identity(b"lonely", Surrogate::new(7)).as_deref(),
            Some("lonely")
        );
    }

    #[test]
    fn a_non_utf8_key_resolves_to_nothing() {
        assert_eq!(pk_identity(&[0xff, 0xfe], Surrogate::new(7)), None);
    }
}
