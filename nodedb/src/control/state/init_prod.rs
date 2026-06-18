// SPDX-License-Identifier: BUSL-1.1

//! SharedState::open — production constructor loading from disk.

use nodedb_types::DatabaseId;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use nodedb_types::config::TuningConfig;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::control::security::permission::PermissionStore;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::security::role::RoleStore;
use crate::control::security::tenant::{TenantIsolation, TenantQuota};
use crate::control::server::sync::dlq::{DlqConfig, SyncDlq};
use crate::wal::WalManager;

use super::SharedState;

impl SharedState {
    /// Create shared state with persistent credential store (for production).
    pub fn open(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        catalog_path: &std::path::Path,
        auth_config: &crate::config::auth::AuthConfig,
        tuning: TuningConfig,
        quiesce: Arc<crate::bridge::quiesce::CollectionQuiesce>,
        array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    ) -> crate::Result<Arc<Self>> {
        let mut credentials = CredentialStore::open(catalog_path)?;

        // Share the credential store's already-open catalog (one redb file
        // handle). Opening a second `SystemCatalog` on the same path is rejected
        // by redb, which would silently disable durable fencing.
        let producer_registry = {
            use crate::control::sync_producer::registry::SyncProducerRegistry;
            match credentials.catalog() {
                Some(catalog) => match SyncProducerRegistry::open(Arc::new(catalog.clone())) {
                    Ok(reg) => Some(Arc::new(reg)),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "SharedState::open: SyncProducerRegistry::open failed; \
                             sync handshake will use in-memory fork detection only"
                        );
                        None
                    }
                },
                None => None,
            }
        };

        credentials.set_lockout_policy_with_grace(
            auth_config.max_failed_logins,
            auth_config.lockout_duration_secs,
            auth_config.password_expiry_days,
            auth_config.password_expiry_grace_days,
        );
        credentials.set_argon2_config(auth_config.argon2.clone());

        let api_keys = ApiKeyStore::new();
        let roles = RoleStore::new();
        let permissions = PermissionStore::new();
        let blacklist = crate::control::security::blacklist::store::BlacklistStore::new();
        let trigger_registry = crate::control::trigger::TriggerRegistry::new();
        let stream_registry = Arc::new(crate::event::cdc::StreamRegistry::new());
        let group_registry = crate::event::cdc::GroupRegistry::new();
        let schedule_registry = Arc::new(crate::event::scheduler::ScheduleRegistry::new());
        let synonym_registry = Arc::new(crate::control::synonym::SynonymRegistry::new());
        let custom_type_registry = Arc::new(crate::control::custom_type::CustomTypeRegistry::new());
        let retention_policy_registry =
            Arc::new(crate::engine::timeseries::retention_policy::RetentionPolicyRegistry::new());
        let alert_registry = Arc::new(crate::event::alert::AlertRegistry::new());
        let alert_hysteresis = Arc::new(crate::event::alert::hysteresis::HysteresisManager::new());
        let ep_topic_registry = crate::event::topic::EpTopicRegistry::new();
        let mv_registry = Arc::new(crate::event::streaming_mv::MvRegistry::new());
        let sequence_registry = Arc::new(crate::control::sequence::SequenceRegistry::new());
        let rls_store = RlsPolicyStore::new();
        let mut audit_start_seq = 1u64;
        if let Some(catalog) = credentials.catalog() {
            api_keys.load_from(catalog)?;
            roles.load_from(catalog)?;
            permissions.load_from(catalog)?;
            blacklist.load_from(catalog)?;
            trigger_registry.load_all(catalog);
            stream_registry.load_from_catalog(catalog);
            group_registry.load_from_catalog(catalog);
            schedule_registry.load_from_catalog(catalog);
            if let Err(e) = synonym_registry.reload_from_catalog(catalog) {
                tracing::warn!(error = %e, "boot: failed to load synonym groups from catalog");
            }
            if let Err(e) = custom_type_registry.reload_from_catalog(catalog) {
                tracing::warn!(error = %e, "boot: failed to load custom types from catalog");
            }
            if let Ok(rp_defs) = catalog.load_all_retention_policies() {
                retention_policy_registry.load(rp_defs);
            }
            alert_registry.load_from_catalog(catalog);
            ep_topic_registry.load_from_catalog(catalog);
            mv_registry.load_from_catalog(catalog);
            sequence_registry.load_from_catalog(catalog);
            match catalog.load_all_rls_policies() {
                Ok(stored) => {
                    let mut loaded = 0usize;
                    for s in &stored {
                        match s.to_runtime() {
                            Ok(p) => {
                                rls_store.install_replicated_policy(p);
                                loaded += 1;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    name = %s.name,
                                    collection = %s.collection,
                                    error = %e,
                                    "boot replay: skipped invalid RLS policy"
                                );
                            }
                        }
                    }
                    if loaded > 0 {
                        tracing::info!(rls_policies = loaded, "loaded RLS policies from catalog");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "failed to load RLS policies"),
            }
            let max_seq = catalog.load_audit_max_seq()?;
            if max_seq > 0 {
                audit_start_seq = max_seq + 1;
            }
        }

        let mut audit_log = AuditLog::new(10_000);
        audit_log.set_next_seq(audit_start_seq);

        // Bootstrap the database-id registry from the persisted hwm.
        // On a fresh server this starts at USER_DB_START (1024); on restart
        // it seeds from the persisted hwm so post-restart allocations cannot
        // collide with pre-restart ones.
        let database_registry = {
            let hwm = if let Some(catalog) = credentials.catalog() {
                catalog.get_database_hwm().unwrap_or(0)
            } else {
                0
            };
            crate::control::database::DatabaseRegistry::from_persisted_hwm(hwm)
        };

        // Bootstrap the global surrogate registry from the persisted
        // hwm. On a fresh database this seeds `next = 1`; on restart
        // it seeds `next = persisted_hwm + 1` so post-restart
        // allocations cannot collide with pre-restart ones.
        let surrogate_registry_handle: crate::control::surrogate::SurrogateRegistryHandle = {
            let initial = if let Some(catalog) = credentials.catalog() {
                // Seed BOTH the global watermark `G` and the applied-reserve
                // cursor so cluster-mode metadata-log replay skips every
                // `SurrogateReserve` already folded into `G` (no restart
                // double-count). Single-node history has cursor 0, so the
                // single-node path (which never proposes `SurrogateReserve`)
                // is unaffected.
                let hwm = catalog.get_surrogate_hwm()?;
                let reserve_index = catalog.get_surrogate_reserve_index()?;
                crate::control::surrogate::SurrogateRegistry::from_persisted(hwm, reserve_index)
            } else {
                crate::control::surrogate::SurrogateRegistry::new()
            };
            Arc::new(std::sync::RwLock::new(initial))
        };

        // Wrap the credential store in an Arc up front so the surrogate
        // assigner (and the SharedState field) can share the same handle.
        let credentials = Arc::new(credentials);
        let surrogate_wal_appender: Arc<dyn crate::control::surrogate::SurrogateWalAppender> =
            Arc::new(crate::control::surrogate::WalSurrogateAppender::new(
                Arc::clone(&wal),
            ));
        let surrogate_assigner = Arc::new(crate::control::surrogate::SurrogateAssigner::new(
            Arc::clone(&surrogate_registry_handle),
            Arc::clone(&credentials),
            surrogate_wal_appender,
        ));

        // Pre-load permission tree definitions before wrapping in RwLock
        // (avoids blocking_write() which panics inside async runtimes).
        let mut permission_cache =
            crate::control::security::permission_tree::PermissionCache::new();
        if let Some(catalog) = credentials.catalog()
            && let Ok(collections) = catalog.load_all_collections(DatabaseId::DEFAULT)
        {
            for coll in &collections {
                if let Some(ref def_json) = coll.permission_tree_def
                    && let Ok(def) = sonic_rs::from_str::<
                        crate::control::security::permission_tree::PermissionTreeDef,
                    >(def_json)
                {
                    permission_cache.register_tree_def(coll.tenant_id, &coll.name, def);
                }
            }
        }

        let shutdown = Arc::new(crate::control::shutdown::ShutdownWatch::new());
        let loop_registry = Arc::new(crate::control::shutdown::LoopRegistry::new());
        // A pre-fired placeholder gate is installed here. `main.rs` replaces
        // it after `open()` returns by swapping via `Arc::get_mut`, installing
        // the real gate from the `StartupSequencer` it constructs.
        let startup_gate = crate::control::startup::StartupGate::pre_fired();
        // Create system metrics up-front so the CDC router can register
        // per-stream drop counters into the same registry that the HTTP
        // /metrics endpoint reads.
        let system_metrics = Arc::new(crate::control::metrics::SystemMetrics::new());

        let shared_audit = Arc::new(Mutex::new(audit_log));
        let prod_session_registry =
            Arc::new(crate::control::security::sessions::SessionRegistry::new());
        let (si_bus, uc_bus, bus_consumer_task) = super::buses_init::init_security_buses(
            Arc::clone(&shared_audit),
            Arc::clone(&prod_session_registry),
        );
        let bus_consumer_handle = Some(bus_consumer_task);

        // Wire the security buses into the credential store so mutations
        // automatically publish to the in-process channels.
        credentials.set_buses(
            Arc::new(
                crate::control::security::buses::SessionInvalidationBus::from_existing(
                    si_bus.sender(),
                ),
            ),
            Arc::new(
                crate::control::security::buses::UserChangeBus::from_existing(uc_bus.sender()),
            ),
        );

        let state = Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            quiesce,
            http_client: Arc::new(reqwest::Client::new()),
            credentials: Arc::clone(&credentials),
            audit: shared_audit,
            api_keys,
            roles,
            permissions,
            trigger_registry,
            array_catalog,
            array_sync_op_log: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                std::sync::Arc::new(crate::control::array_sync::OriginOpLog::open(data_dir)?)
            },
            array_ack_registry: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                crate::control::array_sync::ArrayAckRegistry::open(data_dir)?
            },
            array_snapshot_store: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                crate::control::array_sync::OriginSnapshotStore::open(data_dir)?
            },
            array_snapshot_hlcs: std::sync::Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            array_gc_handle: None,
            session_invalidation_bus: si_bus,
            user_change_bus: uc_bus,
            bus_consumer_handle,
            array_sync_schemas: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                let schema_db = {
                    let dir = data_dir.join("array_sync");
                    std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
                        engine: "array_sync".into(),
                        detail: format!("create array_sync dir: {e}"),
                    })?;
                    let path = dir.join("schema_docs.redb");
                    std::sync::Arc::new(redb::Database::create(&path).map_err(|e| {
                        crate::Error::Storage {
                            engine: "array_sync".into(),
                            detail: format!("schema_registry db open: {e}"),
                        }
                    })?)
                };
                let replica_id = nodedb_array::sync::ReplicaId::new(0);
                let hlc_gen =
                    std::sync::Arc::new(nodedb_array::sync::HlcGenerator::new(replica_id));
                std::sync::Arc::new(crate::control::array_sync::OriginSchemaRegistry::open(
                    schema_db, replica_id, hlc_gen,
                )?)
            },
            array_delivery: std::sync::Arc::new(
                crate::control::array_sync::ArrayDeliveryRegistry::new(),
            ),
            array_subscriber_cursors: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                let cursor_db = {
                    let dir = data_dir.join("array_sync");
                    std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
                        engine: "array_sync".into(),
                        detail: format!("create array_sync dir for cursors: {e}"),
                    })?;
                    let path = dir.join("subscriber_cursors.redb");
                    std::sync::Arc::new(redb::Database::create(&path).map_err(|e| {
                        crate::Error::Storage {
                            engine: "array_sync".into(),
                            detail: format!("subscriber_cursor db open: {e}"),
                        }
                    })?)
                };
                let store = crate::control::array_sync::SubscriberStore::open(cursor_db)?;
                std::sync::Arc::new(crate::control::array_sync::SubscriberMap::new(store))
            },
            array_merger_registry: std::sync::Arc::new(
                crate::control::array_sync::MergerRegistry::new(),
            ),
            mirror_link_registry: Arc::new(crate::control::mirror::MirrorLinkRegistry::new()),
            database_registry,
            surrogate_registry: surrogate_registry_handle,
            surrogate_assigner,
            block_cache: crate::control::planner::procedural::executor::ProcedureBlockCache::new(
                4096,
            ),
            stream_registry: Arc::clone(&stream_registry),
            cdc_router: Arc::new(
                crate::event::cdc::CdcRouter::new(stream_registry)
                    .with_metrics(Arc::clone(&system_metrics)),
            ),
            group_registry,
            offset_store: Arc::new(crate::event::cdc::OffsetStore::open(
                catalog_path.parent().unwrap_or(std::path::Path::new(".")),
            )?),
            retention_policy_registry,
            bitemporal_retention_registry: Arc::new(
                crate::engine::bitemporal::BitemporalRetentionRegistry::new(),
            ),
            alert_registry,
            alert_hysteresis,
            schedule_registry,
            synonym_registry,
            custom_type_registry,
            job_history: Arc::new(crate::event::scheduler::JobHistoryStore::open(
                catalog_path.parent().unwrap_or(std::path::Path::new(".")),
            )?),
            ep_topic_registry,
            webhook_manager: crate::event::webhook::WebhookManager::new(shutdown.raw_receiver()),
            mv_registry,
            consumer_assignments: crate::event::cdc::consumer_group::ConsumerAssignments::new(),
            watermark_tracker: Arc::new(crate::event::watermark_tracker::WatermarkTracker::new()),
            event_plane_budget: Arc::new(crate::event::budget::EventPlaneBudget::new()),
            cross_shard_dispatcher: None,
            cross_shard_dlq: None,
            cross_shard_metrics: None,
            hwm_store: None,
            kafka_manager: crate::event::kafka::KafkaManager::new(shutdown.raw_receiver()),
            definition_sync_fanout: std::sync::Arc::new(
                crate::control::server::sync::definition_fanout::DefinitionSyncFanout::new(),
            ),
            crdt_sync_delivery: Arc::new(crate::event::crdt_sync::CrdtSyncDelivery::new()),
            delta_packager: Arc::new(crate::event::crdt_sync::DeltaPackager::new()),
            mv_persistence: Arc::new(crate::event::streaming_mv::MvPersistence::open(
                catalog_path.parent().unwrap_or(std::path::Path::new(".")),
            )?),
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
            cluster_topology: None,
            cluster_routing: None,
            cluster_transport: None,
            node_id: 0,
            metadata_cache: Arc::new(std::sync::RwLock::new(nodedb_cluster::MetadataCache::new())),
            catalog_change_tx: tokio::sync::broadcast::channel(
                crate::control::cluster::metadata_applier::CATALOG_CHANNEL_CAPACITY,
            )
            .0,
            group_watchers: Arc::new(nodedb_cluster::GroupAppliedWatchers::new()),
            metadata_raft: std::sync::OnceLock::new(),
            propose_tracker: std::sync::OnceLock::new(),
            raft_proposer: std::sync::OnceLock::new(),
            async_raft_proposer: std::sync::OnceLock::new(),
            raft_status_fn: None,
            cluster_observer: std::sync::OnceLock::new(),
            loop_metrics_registry: nodedb_cluster::LoopMetricsRegistry::new(),
            per_vshard_metrics: crate::control::metrics::PerVShardMetricsRegistry::new(),
            health_monitor: std::sync::OnceLock::new(),
            trace_exporter: crate::control::trace_export::TraceExporter::disabled(),
            debug_endpoints_enabled: false,
            migration_tracker: None,
            rls: rls_store,
            blacklist,
            auth_users: crate::control::security::jit::auth_user::AuthUserStore::new(),
            orgs: crate::control::security::org::store::OrgStore::new(),
            scope_defs: crate::control::security::scope::store::ScopeStore::new(),
            scope_grants: crate::control::security::scope::grant::ScopeGrantStore::new(),
            rate_limiter: crate::control::security::ratelimit::limiter::RateLimiter::default(),
            session_handles:
                crate::control::security::session_handle::SessionHandleStore::from_config(
                    &auth_config.session,
                ),
            session_registry: prod_session_registry,
            escalation: crate::control::security::escalation::EscalationEngine::default(),
            usage_counter: Arc::new(
                crate::control::security::metering::counter::UsageCounter::new(),
            ),
            usage_store: Arc::new(crate::control::security::metering::store::UsageStore::default()),
            quota_manager: crate::control::security::metering::quota::QuotaManager::new(),
            auth_api_keys: crate::control::security::auth_apikey::AuthApiKeyStore::new(),
            impersonation: crate::control::security::impersonation::ImpersonationStore::default(),
            emergency: crate::control::security::emergency::EmergencyState::default(),
            auth_metrics: crate::control::security::observability::AuthMetrics::new(),
            ceilings: crate::control::security::ceiling::CeilingStore::new(),
            redaction: crate::control::security::redaction::RedactionStore::new(),
            risk_scorer: crate::control::security::risk::RiskScorer::default(),
            tls_policy: crate::control::security::tls_policy::TlsPolicy::default(),
            siem: crate::control::security::siem::SiemExporter::default(),
            jwks_registry: None,
            sync_dlq: Mutex::new(SyncDlq::new(DlqConfig::default())),
            audit_retention_days: auth_config.audit_retention_days,
            audit_max_entries: auth_config.audit_max_entries,
            idle_timeout_secs: auth_config.idle_timeout_secs,
            session_absolute_timeout_secs: auth_config.session_absolute_timeout_secs,
            ws_sessions: std::sync::RwLock::new(std::collections::HashMap::new()),
            topic_registry: crate::control::pubsub::TopicRegistry::new(10_000),
            shape_registry: Arc::new(crate::control::server::sync::shape::ShapeRegistry::new()),
            change_stream: crate::control::change_stream::ChangeStream::new(4096),
            notify_bus: crate::control::notify_bus::NotifyBus::default(),
            connections_rejected: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            raft_propose_leader_change_retries: AtomicU64::new(0),
            request_id_counter: AtomicU64::new(1),
            // Use the pre-created Arc so the CdcRouter (above) and this
            // metrics endpoint share the same SystemMetrics registry.
            system_metrics: Some(Arc::clone(&system_metrics)),
            database_metrics: Arc::new(crate::control::metrics::DatabaseMetricsRegistry::new()),
            quota_ceiling: Arc::new(std::sync::RwLock::new(
                crate::control::security::catalog::GlobalQuotaCeiling::default(),
            )),
            retention_settings: Arc::new(std::sync::RwLock::new(
                crate::config::server::RetentionSettings::default(),
            )),
            governor: None,
            maintenance_budget: Arc::new(
                crate::control::maintenance::MaintenanceBudgetTracker::new(),
            ),
            producer_registry,
            ts_partition_registries: Some(Mutex::new(std::collections::HashMap::new())),
            cold_storage: None,
            snapshot_storage: Arc::new(object_store::memory::InMemory::new()),
            quarantine_storage: Arc::new(object_store::memory::InMemory::new()),
            hlc_clock: Arc::new(nodedb_types::HlcClock::new()),
            tenant_write_hlc: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            lease_drain: Arc::new(crate::control::lease::DescriptorDrainTracker::new()),
            lease_refcount: Arc::new(crate::control::lease::LeaseRefCount::new()),
            sequencer_inbox: std::sync::OnceLock::new(),
            sequencer_metrics: std::sync::OnceLock::new(),
            calvin_completion_registry: std::sync::OnceLock::new(),
            ollp_orchestrator: std::sync::OnceLock::new(),
            limits: nodedb_types::protocol::Limits::default(),
            tuning,
            scheduler_config: crate::config::server::SchedulerConfig::default(),
            data_dir: std::path::PathBuf::new(),
            schema_version: crate::control::server::pgwire::handler::prepared::SchemaVersion::new(),
            sequence_registry,
            dml_counter:
                crate::control::server::pgwire::ddl::maintenance::auto_analyze::DmlCounter::new(),
            wal_catchup_lsn: AtomicU64::new(0),
            presence: Arc::new(tokio::sync::RwLock::new(
                crate::control::server::sync::presence::PresenceManager::new(
                    crate::control::server::sync::presence::PresenceConfig::default(),
                ),
            )),
            permission_cache: Arc::new(tokio::sync::RwLock::new(permission_cache)),
            gateway_invalidator: None,
            gateway: None,
            backup_kek: None,
            quarantine_registry: Arc::new(crate::storage::quarantine::QuarantineRegistry::new()),
            admission_registry: Arc::new(
                crate::control::server::admission::AdmissionRegistry::new(),
            ),
            audit_dml_cache: Arc::new(crate::control::state::audit_dml_cache::AuditDmlCache::new()),
            idle_timeout_cache: Arc::new(
                crate::control::state::idle_timeout_cache::IdleTimeoutCache::new(),
            ),
            collection_to_database: Arc::new(
                crate::control::state::collection_to_database::CollectionToDatabase::new(),
            ),
            lsn_ms_map: Arc::new(Mutex::new(nodedb_types::temporal::LsnMsMap::new())),
            materialize_freeze: crate::control::clone::MaterializeFreezeRegistry::new(),
            shutdown: Arc::clone(&shutdown),
            loop_registry: Arc::clone(&loop_registry),
            startup: Arc::clone(&startup_gate),
        });

        Self::wire_session_handle_audit(&state);

        // Populate per-database DML audit cache and collection→database reverse
        // map from the catalog so the Event Plane consumer has accurate data
        // from the first write after startup.
        if let Some(catalog) = state.credentials.catalog() {
            if let Err(e) = state.audit_dml_cache.load_from_catalog(catalog) {
                tracing::warn!(error = %e, "boot: failed to populate audit_dml_cache from catalog");
            }
            if let Err(e) = state.collection_to_database.load_from_catalog(catalog) {
                tracing::warn!(
                    error = %e,
                    "boot: failed to populate collection_to_database cache from catalog"
                );
            }
            if let Err(e) = state.idle_timeout_cache.load_from_catalog(catalog) {
                tracing::warn!(
                    error = %e,
                    "boot: failed to populate idle_timeout_cache from catalog"
                );
            }
        }

        // Spawn the array GC background task. The handle is stored by the caller
        // (main.rs) which has mutable access at that point via Arc::get_mut.
        // The task shuts itself down via ShutdownWatch, so dropping the handle
        // here is safe — the task keeps running until shutdown is signalled.
        let _gc_handle = crate::control::array_sync::spawn_gc_task(
            Arc::clone(&state.array_sync_op_log),
            Arc::clone(&state.array_snapshot_store),
            Arc::clone(&state.array_ack_registry),
            Arc::clone(&state.array_snapshot_hlcs),
            Arc::clone(&state.shutdown),
            crate::control::array_sync::gc_task::DEFAULT_GC_INTERVAL,
        );
        // `array_gc_handle` in SharedState stays None; main.rs may install the
        // handle via Arc::get_mut after open() returns (before cloning the Arc).

        Ok(state)
    }
}
