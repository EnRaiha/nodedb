// SPDX-License-Identifier: BUSL-1.1

//! The per-identity resolution rule and the top-level plan walk.

use std::cell::RefCell;

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::{CollectionKey, Surrogate};

use super::super::SurrogateAssigner;
use super::carried::CarriedIdentity;
use crate::types::{DatabaseId, TenantId};

/// Binds carried identities against one node's catalog under one tenancy scope.
pub struct IdentityBinder<'a> {
    assigner: &'a SurrogateAssigner,
    database_id: DatabaseId,
    tenant_id: TenantId,
    /// `Some` when the walk also lists every identity it binds.
    recorded: Option<RefCell<Vec<CarriedIdentity>>>,
}

impl<'a> IdentityBinder<'a> {
    pub fn new(
        assigner: &'a SurrogateAssigner,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> Self {
        Self {
            assigner,
            database_id,
            tenant_id,
            recorded: None,
        }
    }

    /// A binder that also lists every identity it binds, with the
    /// authoritative surrogate.
    pub(super) fn recording(
        assigner: &'a SurrogateAssigner,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> Self {
        Self {
            recorded: Some(RefCell::new(Vec::new())),
            ..Self::new(assigner, database_id, tenant_id)
        }
    }

    /// The identities a recording binder bound, in walk order.
    pub(super) fn into_recorded(self) -> Vec<CarriedIdentity> {
        self.recorded.map(RefCell::into_inner).unwrap_or_default()
    }

    /// The canonical key of a collection named on a plan. Plans carry the
    /// database-qualified name, so it is de-qualified here.
    pub(super) fn plan_key<'c>(&self, qualified: &'c str) -> crate::Result<CollectionKey<'c>> {
        Ok(CollectionKey::from_qualified_str(
            self.database_id,
            qualified,
        )?)
    }

    /// The canonical key of a collection named by its bare catalog name, as
    /// an array plan names its array.
    pub(super) fn bare_key<'c>(&self, bare: &'c str) -> CollectionKey<'c> {
        CollectionKey::from_bare(self.database_id, bare)
    }

    fn record(&self, key: CollectionKey<'_>, pk_bytes: &[u8], surrogate: Surrogate) {
        if let Some(recorded) = &self.recorded {
            recorded.borrow_mut().push(CarriedIdentity {
                collection: key.name().to_string(),
                pk_bytes: pk_bytes.to_vec(),
                surrogate,
            });
        }
    }

    /// The authoritative surrogate for `(key, pk_bytes)`.
    ///
    /// `carried` came from the coordinator that planned the write: install it
    /// first-wins and return the bound value, which is the carried one or an
    /// earlier binding. Every writer binds its row's surrogate before the plan
    /// leaves the coordinator, so `Surrogate::ZERO` names no row and the write
    /// is refused.
    pub(super) fn resolve(
        &self,
        key: CollectionKey<'_>,
        pk_bytes: &[u8],
        carried: Surrogate,
    ) -> crate::Result<Surrogate> {
        if carried == Surrogate::ZERO {
            return Err(crate::Error::Internal {
                detail: format!(
                    "write of key '{}' in '{}' carries no surrogate; every write carries the \
                     one its coordinator bound",
                    String::from_utf8_lossy(pk_bytes),
                    key.name()
                ),
            });
        }
        let bound = self.assigner.bind(key, self.tenant_id, pk_bytes, carried)?;
        self.record(key, pk_bytes, bound);
        Ok(bound)
    }

    /// [`Self::resolve`] for a row with no user key: the surrogate self-keys by
    /// its own big-endian bytes, the same key `assign_anonymous` binds under.
    pub(super) fn resolve_self_keyed(
        &self,
        key: CollectionKey<'_>,
        carried: Surrogate,
    ) -> crate::Result<Surrogate> {
        self.resolve(key, &carried.as_u32().to_be_bytes(), carried)
    }

    /// [`Self::resolve`] writing the authoritative value back into `slot`.
    pub(super) fn resolve_in_place(
        &self,
        key: CollectionKey<'_>,
        pk_bytes: &[u8],
        slot: &mut Surrogate,
    ) -> crate::Result<()> {
        *slot = self.resolve(key, pk_bytes, *slot)?;
        Ok(())
    }

    /// [`Self::resolve_in_place`] for a write that mutates a row it never
    /// creates. A carried surrogate is bound first-wins, and a carried
    /// `Surrogate::ZERO` is refused. `None` names a key the coordinator found
    /// unbound: the catalog is read only, and the slot takes an existing
    /// binding or stays `None`.
    pub(super) fn resolve_existing_in_place(
        &self,
        key: CollectionKey<'_>,
        pk_bytes: &[u8],
        slot: &mut Option<Surrogate>,
    ) -> crate::Result<()> {
        *slot = match *slot {
            Some(carried) => Some(self.resolve(key, pk_bytes, carried)?),
            None => self.assigner.lookup_bound(key, self.tenant_id, pk_bytes)?,
        };
        Ok(())
    }

    /// [`Self::resolve_self_keyed`] writing the authoritative value back into `slot`.
    pub(super) fn resolve_self_keyed_in_place(
        &self,
        key: CollectionKey<'_>,
        slot: &mut Surrogate,
    ) -> crate::Result<()> {
        *slot = self.resolve_self_keyed(key, *slot)?;
        Ok(())
    }
}

/// Bind every identity `plan` carries and rewrite each surrogate slot with the
/// authoritative value. Exhaustive over every op family that carries a
/// `(collection, key, surrogate)` triple; the rest carry no identity to bind.
pub fn bind_plan_identities(
    assigner: &SurrogateAssigner,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: &mut PhysicalPlan,
) -> crate::Result<()> {
    bind_with(&IdentityBinder::new(assigner, database_id, tenant_id), plan)
}

/// Bind every identity `plan` carries through `binder`.
pub(super) fn bind_with(binder: &IdentityBinder<'_>, plan: &mut PhysicalPlan) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Document(op) => super::document::bind(binder, op),
        PhysicalPlan::Kv(op) => super::kv::bind(binder, op),
        PhysicalPlan::Graph(op) => super::graph::bind(binder, op),
        PhysicalPlan::Vector(op) => super::vector::bind(binder, op),
        PhysicalPlan::Crdt(op) => super::crdt::bind(binder, op),
        PhysicalPlan::Array(op) => super::array::bind(binder, op),
        PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::RestoreRedo(batch)) => {
            super::restore::bind(binder, batch)
        }
        // Columnar-family rows are keyed by the surrogate alone; text, spatial,
        // timeseries, query, other meta and cluster plans carry no pk binding.
        PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_physical::physical_plan::{ArrayOp, GraphOp, KvOp};
    use nodedb_types::{CollectionKey, QualifiedCollection, Surrogate};

    use super::bind_plan_identities;
    use crate::control::planner::sql_plan_convert::test_support::test_assigner;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::{DatabaseId, TenantId};

    const TENANT: TenantId = TenantId::new(1);

    fn kv_put(surrogate: Surrogate) -> nodedb_physical::physical_plan::PhysicalPlan {
        nodedb_physical::physical_plan::PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            key: b"k".to_vec(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    fn array_put(surrogate: Surrogate) -> nodedb_physical::physical_plan::PhysicalPlan {
        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(1)],
            attrs: vec![CellValue::Int64(1)],
            surrogate,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }];
        nodedb_physical::physical_plan::PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TENANT, "grid"),
            cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        })
    }

    fn edge_put(src: Surrogate) -> nodedb_physical::physical_plan::PhysicalPlan {
        nodedb_physical::physical_plan::PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            src_id: "a".into(),
            label: "l".into(),
            dst_id: "b".into(),
            properties: Vec::new(),
            src_surrogate: src,
            dst_surrogate: Surrogate::new(2),
        })
    }

    /// `Surrogate::ZERO` names no row on any engine: the binder refuses the
    /// plan and installs nothing in the catalog.
    #[test]
    fn a_carried_zero_is_refused_on_every_engine() {
        let assigner = test_assigner();
        for mut plan in [
            kv_put(Surrogate::ZERO),
            array_put(Surrogate::ZERO),
            edge_put(Surrogate::ZERO),
        ] {
            let result = bind_plan_identities(&assigner, DatabaseId::DEFAULT, TENANT, &mut plan);
            assert!(result.is_err(), "{plan:?} carries ZERO and must be refused");
        }
        let kv_key = CollectionKey::from_bare(DatabaseId::DEFAULT, "kv");
        assert_eq!(
            assigner.lookup_bound(kv_key, TENANT, b"k").expect("lookup"),
            None
        );
        let graph_key = CollectionKey::from_bare(DatabaseId::DEFAULT, "g");
        assert_eq!(
            assigner
                .lookup_bound(graph_key, TENANT, b"a")
                .expect("lookup"),
            None
        );
    }

    /// A bound surrogate installs first-wins and the plan keeps it.
    #[test]
    fn a_carried_surrogate_binds() {
        let assigner = test_assigner();
        let mut plan = kv_put(Surrogate::new(7));
        bind_plan_identities(&assigner, DatabaseId::DEFAULT, TENANT, &mut plan).expect("bind");
        let kv_key = CollectionKey::from_bare(DatabaseId::DEFAULT, "kv");
        assert_eq!(
            assigner.lookup_bound(kv_key, TENANT, b"k").expect("lookup"),
            Some(Surrogate::new(7))
        );
    }
}
