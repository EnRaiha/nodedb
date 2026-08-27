// SPDX-License-Identifier: BUSL-1.1

//! Shared fixtures and helpers for `catalog_integrity_*` integration tests.
//!
//! Every parent-replicated catalog type has a `make_*` constructor that
//! produces a minimally-valid record owned by the test-fixture `admin`
//! user in tenant 1. The applier-contract, verifier-coverage, and
//! referential-check tests all depend on these fixtures, so they live
//! in a single module that every test file pulls in via
//! `mod catalog_integrity_helpers;`.

#![allow(dead_code)]

use nodedb::control::cluster::recovery_check::divergence::DivergenceKind;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;
use nodedb::control::security::catalog::auth_types::StoredUser;
use nodedb::control::security::catalog::function_types::{
    FunctionParam, FunctionVolatility, StoredFunction,
};
use nodedb::control::security::catalog::procedure_types::{
    ParamDirection, ProcedureParam, ProcedureRoutability, StoredProcedure,
};
use nodedb::control::security::catalog::sequence_types::StoredSequence;
use nodedb::control::security::catalog::trigger_types::{
    StoredTrigger, TriggerEvents, TriggerGranularity, TriggerTiming,
};
use nodedb::control::security::catalog::{StoredCollection, StoredMaterializedView, SystemCatalog};
use nodedb::event::cdc::stream_def::{ChangeStreamDef, OpFilter, RetentionConfig, StreamFormat};
use nodedb::event::scheduler::types::{MissedPolicy, ScheduleDef, ScheduleScope};
use nodedb::types::DatabaseId;

pub const ADMIN: &str = "admin";
pub const TENANT: u64 = 1;

pub fn make_catalog() -> (tempfile::TempDir, SystemCatalog) {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
    put_admin_user(&catalog);
    (dir, catalog)
}

pub fn put_admin_user(catalog: &SystemCatalog) {
    let user = StoredUser {
        user_id: 1,
        username: ADMIN.to_string(),
        tenant_id: TENANT,
        password_hash: "argon2id$dummy".to_string(),
        scram_salt: vec![],
        scram_salted_password: vec![],
        roles: vec!["superuser".to_string()],
        is_superuser: true,
        is_active: true,
        is_service_account: false,
        created_at: 0,
        updated_at: 0,
        password_expires_at: 0,
        must_change_password: false,
        password_changed_at: 0,
        default_database_id: 0,
        accessible_databases: vec![],
    };
    catalog.put_user(&user).unwrap();
}

pub fn make_collection(name: &str) -> StoredCollection {
    StoredCollection::new(TENANT, name, ADMIN)
}

pub fn make_function(name: &str) -> StoredFunction {
    StoredFunction {
        tenant_id: TENANT,
        database_id: DatabaseId::DEFAULT,
        name: name.to_string(),
        parameters: vec![FunctionParam {
            name: "x".into(),
            data_type: "INT".into(),
        }],
        return_type: "INT".into(),
        body_sql: "SELECT x".into(),
        compiled_body_sql: None,
        volatility: FunctionVolatility::Immutable,
        security: Default::default(),
        language: Default::default(),
        wasm_hash: None,
        wasm_module: None,
        dependencies: vec![],
        wasm_fuel: 1_000_000,
        wasm_memory: 16 * 1024 * 1024,
        owner: ADMIN.into(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: Default::default(),
    }
}

pub fn make_procedure(name: &str) -> StoredProcedure {
    StoredProcedure {
        tenant_id: TENANT,
        database_id: DatabaseId::DEFAULT,
        name: name.into(),
        parameters: vec![ProcedureParam {
            name: "cutoff".into(),
            data_type: "INT".into(),
            direction: ParamDirection::In,
        }],
        body_sql: "BEGIN END".into(),
        max_iterations: 1_000_000,
        timeout_secs: 60,
        routability: ProcedureRoutability::default(),
        owner: ADMIN.into(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: Default::default(),
    }
}

pub fn make_trigger(name: &str, collection: &str) -> StoredTrigger {
    StoredTrigger {
        tenant_id: TENANT,
        database_id: DatabaseId::DEFAULT,
        collection: collection.into(),
        name: name.into(),
        timing: TriggerTiming::After,
        events: TriggerEvents {
            on_insert: true,
            on_update: false,
            on_delete: false,
        },
        granularity: TriggerGranularity::Row,
        when_condition: None,
        body_sql: "BEGIN END".into(),
        priority: 0,
        enabled: true,
        execution_mode: Default::default(),
        security: Default::default(),
        batch_mode: Default::default(),
        owner: ADMIN.into(),
        created_at: 0,
        descriptor_version: 1,
        modification_hlc: Default::default(),
    }
}

pub fn make_mv(name: &str) -> StoredMaterializedView {
    make_mv_sourced(name, "source_coll")
}

pub fn make_mv_sourced(name: &str, source: &str) -> StoredMaterializedView {
    StoredMaterializedView {
        tenant_id: TENANT,
        name: name.into(),
        source: source.into(),
        query_sql: format!("SELECT * FROM {source}"),
        refresh_mode: "auto".into(),
        owner: ADMIN.into(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: Default::default(),
    }
}

pub fn make_sequence(name: &str) -> StoredSequence {
    StoredSequence::new(TENANT, name.into(), ADMIN.into())
}

pub fn make_schedule(name: &str) -> ScheduleDef {
    ScheduleDef {
        tenant_id: TENANT,
        database_id: DatabaseId::DEFAULT.as_u64(),
        name: name.into(),
        cron_expr: "*/5 * * * *".into(),
        body_sql: "SELECT 1".into(),
        scope: ScheduleScope::Normal,
        missed_policy: MissedPolicy::Skip,
        allow_overlap: true,
        enabled: true,
        target_collection: None,
        owner: ADMIN.into(),
        created_at: 0,
    }
}

pub fn make_stream(name: &str) -> ChangeStreamDef {
    ChangeStreamDef {
        database_id: nodedb::types::DatabaseId::new(7),
        tenant_id: TENANT,
        name: name.into(),
        collection: "*".into(),
        op_filter: OpFilter::all(),
        format: StreamFormat::Json,
        retention: RetentionConfig::default(),
        compaction: Default::default(),
        webhook: Default::default(),
        late_data: Default::default(),
        kafka: Default::default(),
        owner: ADMIN.into(),
        created_at: 0,
        subscriber_roles: Vec::new(),
    }
}

pub fn owner_row_present(catalog: &SystemCatalog, object_type: &str, name: &str) -> bool {
    let owners = catalog.load_all_owners().unwrap();
    owners.iter().any(|o| {
        o.object_type == object_type
            && o.tenant_id == TENANT
            && o.object_name == name
            && o.owner_username == ADMIN
    })
}

pub fn find_orphan(catalog: &SystemCatalog, expected_kind: &str) -> Option<DivergenceKind> {
    verify_redb_integrity(catalog)
        .into_iter()
        .map(|d| d.kind)
        .find(|k| {
            matches!(
                k,
                DivergenceKind::OrphanRow { kind, expected_parent_kind: "owner", .. }
                    if *kind == expected_kind
            )
        })
}
