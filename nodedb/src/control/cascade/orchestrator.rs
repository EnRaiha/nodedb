// SPDX-License-Identifier: BUSL-1.1

//! Cascade orchestrator — composes the per-kind enumerators into a
//! single `collect_dependents` call.
//!
//! The caller (DROP COLLECTION handler) uses the result in two ways:
//!
//! 1. **Default (no CASCADE)**: non-empty result → reject with
//!    `NodeDbError::dependent_objects_exist` listing every dependent
//!    by kind and name.
//! 2. **With CASCADE**: emit `Delete*` catalog entries for every
//!    dependent in the same metadata-raft commit as the
//!    `PurgeCollection`, so there is no window where a dependent
//!    points at a missing collection.
//!
//! Cycle detection: `visited: HashSet<(kind, name)>` is threaded
//! through every enumerator call. The MV walker has its own internal
//! bound (`materialized_views::MAX_DEPTH`); exceeding it bubbles up
//! as `NodeDbError::CascadeCycle` and the orchestrator forwards it.

use std::collections::HashSet;

use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;

use super::{change_streams, materialized_views, rls, schedules, sequences, triggers};

/// Kind of dependent object returned by the orchestrator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DependentKind {
    Sequence,
    Trigger,
    RlsPolicy,
    MaterializedView,
    StreamingMaterializedView,
    ChangeStream,
    Schedule,
}

impl DependentKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sequence => "sequence",
            Self::Trigger => "trigger",
            Self::RlsPolicy => "rls_policy",
            Self::MaterializedView => "materialized_view",
            Self::StreamingMaterializedView => "streaming_materialized_view",
            Self::ChangeStream => "change_stream",
            Self::Schedule => "schedule",
        }
    }
}

/// One cascade-dependent object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dependent {
    pub kind: DependentKind,
    pub name: String,
}

/// Enumerate every catalog object that transitively depends on
/// `(tenant_id, root_collection)`. Sorted deterministically by
/// `(kind, name)` so error messages and cascade commits are stable.
///
/// `visited` is an out-parameter the caller owns so repeated calls
/// (e.g. a transactional DROP of several collections) don't re-walk
/// the same subgraphs.
pub fn collect_dependents(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    root_collection: &str,
    visited: &mut HashSet<(DependentKind, String)>,
) -> crate::Result<Vec<Dependent>> {
    let mut out: Vec<Dependent> = Vec::new();

    for name in sequences::find_implicit_sequences(catalog, tenant_id, root_collection)? {
        if visited.insert((DependentKind::Sequence, name.clone())) {
            out.push(Dependent {
                kind: DependentKind::Sequence,
                name,
            });
        }
    }
    for name in triggers::find_triggers_on(catalog, database_id, tenant_id, root_collection)? {
        if visited.insert((DependentKind::Trigger, name.clone())) {
            out.push(Dependent {
                kind: DependentKind::Trigger,
                name,
            });
        }
    }
    for name in rls::find_rls_policies_on(catalog, tenant_id, root_collection)? {
        if visited.insert((DependentKind::RlsPolicy, name.clone())) {
            out.push(Dependent {
                kind: DependentKind::RlsPolicy,
                name,
            });
        }
    }
    for name in materialized_views::find_mvs_sourcing(catalog, tenant_id, root_collection)? {
        if visited.insert((DependentKind::MaterializedView, name.clone())) {
            out.push(Dependent {
                kind: DependentKind::MaterializedView,
                name,
            });
        }
    }
    for name in materialized_views::find_streaming_mvs_sourcing(
        catalog,
        database_id,
        tenant_id,
        root_collection,
    )? {
        if visited.insert((DependentKind::StreamingMaterializedView, name.clone())) {
            out.push(Dependent {
                kind: DependentKind::StreamingMaterializedView,
                name,
            });
        }
    }
    for name in
        change_streams::find_change_streams_on(catalog, database_id, tenant_id, root_collection)?
    {
        if visited.insert((DependentKind::ChangeStream, name.clone())) {
            out.push(Dependent {
                kind: DependentKind::ChangeStream,
                name,
            });
        }
    }
    for name in
        schedules::find_schedules_referencing(catalog, database_id, tenant_id, root_collection)?
    {
        if visited.insert((DependentKind::Schedule, name.clone())) {
            out.push(Dependent {
                kind: DependentKind::Schedule,
                name,
            });
        }
    }

    out.sort();
    Ok(out)
}

/// Convenience builder for the
/// [`crate::Error::DependentObjectsExist`] variant so call sites
/// don't repeat the kind-string conversion.
pub fn dependents_error(tenant_id: u64, root_name: &str, deps: &[Dependent]) -> crate::Error {
    let pairs: Vec<(String, String)> = deps
        .iter()
        .map(|d| (d.kind.as_str().to_string(), d.name.clone()))
        .collect();
    crate::Error::DependentObjectsExist {
        tenant_id,
        root_kind: "collection",
        root_name: root_name.to_string(),
        dependent_count: deps.len(),
        dependents: pairs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn cat() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&tmp.path().join("system.redb")).unwrap();
        (cat, tmp)
    }

    #[test]
    fn empty_catalog_has_no_dependents() {
        let (c, _t) = cat();
        let mut visited = HashSet::new();
        let deps = collect_dependents(&c, DatabaseId::DEFAULT, 1, "users", &mut visited).unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn visited_suppresses_duplicates_across_calls() {
        let (c, _t) = cat();
        use crate::control::security::catalog::sequence_types::StoredSequence;
        c.put_sequence(&StoredSequence::new(1, "users_id_seq".into(), "o".into()))
            .unwrap();

        let mut visited = HashSet::new();
        let first = collect_dependents(&c, DatabaseId::DEFAULT, 1, "users", &mut visited).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].kind, DependentKind::Sequence);

        let second = collect_dependents(&c, DatabaseId::DEFAULT, 1, "users", &mut visited).unwrap();
        assert!(second.is_empty(), "visited set must suppress re-emission");
    }

    #[test]
    fn streaming_mvs_are_database_scoped_in_collection_cascades() {
        use crate::control::security::catalog::StoredMaterializedView;
        use crate::event::cdc::stream_def::{
            ChangeStreamDef, CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig,
            StreamFormat,
        };
        use crate::event::kafka::KafkaDeliveryConfig;
        use crate::event::streaming_mv::StreamingMvDef;
        use crate::event::webhook::WebhookConfig;

        let (catalog, _tmp) = cat();
        for database_id in [DatabaseId::new(1), DatabaseId::new(2)] {
            catalog
                .put_change_stream(&ChangeStreamDef {
                    database_id,
                    tenant_id: 7,
                    name: "orders_stream".into(),
                    collection: "orders".into(),
                    op_filter: OpFilter::all(),
                    format: StreamFormat::Json,
                    retention: RetentionConfig::default(),
                    compaction: CompactionConfig::default(),
                    webhook: WebhookConfig::default(),
                    late_data: LateDataPolicy::default(),
                    kafka: KafkaDeliveryConfig::default(),
                    owner: "admin".into(),
                    created_at: 0,
                    subscriber_roles: Vec::new(),
                })
                .unwrap();
            catalog
                .put_streaming_mv(&StreamingMvDef {
                    database_id,
                    tenant_id: 7,
                    name: "orders_summary".into(),
                    source_stream: "orders_stream".into(),
                    group_by_columns: Vec::new(),
                    aggregates: Vec::new(),
                    filter_expr: None,
                    owner: "admin".into(),
                    created_at: 0,
                })
                .unwrap();
        }

        // Periodic and streaming MVs live in separate catalogs and may share a
        // name. The cascade must retain both instead of collapsing them in the
        // visited set.
        catalog
            .put_materialized_view(&StoredMaterializedView {
                tenant_id: 7,
                name: "orders_summary".into(),
                source: "orders".into(),
                query_sql: "SELECT 1".into(),
                refresh_mode: "auto".into(),
                owner: "admin".into(),
                created_at: 0,
                descriptor_version: 0,
                modification_hlc: Default::default(),
            })
            .unwrap();

        let mut visited = HashSet::new();
        let deps =
            collect_dependents(&catalog, DatabaseId::new(1), 7, "orders", &mut visited).unwrap();
        assert_eq!(
            deps,
            vec![
                Dependent {
                    kind: DependentKind::MaterializedView,
                    name: "orders_summary".into(),
                },
                Dependent {
                    kind: DependentKind::StreamingMaterializedView,
                    name: "orders_summary".into(),
                },
                Dependent {
                    kind: DependentKind::ChangeStream,
                    name: "orders_stream".into(),
                },
            ]
        );
    }

    #[test]
    fn dependents_error_names_every_dependent() {
        let deps = vec![
            Dependent {
                kind: DependentKind::Trigger,
                name: "t1".into(),
            },
            Dependent {
                kind: DependentKind::Sequence,
                name: "s1".into(),
            },
        ];
        let e = dependents_error(1, "users", &deps);
        match e {
            crate::Error::DependentObjectsExist {
                dependent_count,
                dependents,
                root_name,
                ..
            } => {
                assert_eq!(dependent_count, 2);
                assert_eq!(root_name, "users");
                assert!(dependents.iter().any(|(k, n)| k == "trigger" && n == "t1"));
                assert!(dependents.iter().any(|(k, n)| k == "sequence" && n == "s1"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
