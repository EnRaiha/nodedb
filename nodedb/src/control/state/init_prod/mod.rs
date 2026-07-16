// SPDX-License-Identifier: BUSL-1.1

//! SharedState::open — production constructor loading from disk.

mod bootstrap;
mod post_init;

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use nodedb_types::config::TuningConfig;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
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
        let bootstrap::ProdBootstrap {
            credentials,
            producer_registry,
            api_keys,
            roles,
            permissions,
            blacklist,
            trigger_registry,
            stream_registry,
            group_registry,
            schedule_registry,
            synonym_registry,
            custom_type_registry,
            retention_policy_registry,
            alert_registry,
            alert_hysteresis,
            ep_topic_registry,
            mv_registry,
            sequence_registry,
            rls_store,
            shared_audit,
            database_registry,
            surrogate_registry_handle,
            surrogate_assigner,
            permission_cache,
            shutdown,
            loop_registry,
            startup_gate,
            system_metrics,
            prod_session_registry,
            si_bus,
            uc_bus,
            bus_consumer_handle,
        } = bootstrap::run(&wal, catalog_path, auth_config)?;

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
            raft_compactor: std::sync::OnceLock::new(),
            raft_status_fn: std::sync::OnceLock::new(),
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
            shuffle_id_counter: AtomicU64::new(1),
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
            reservation_inbox: std::sync::OnceLock::new(),
            sequencer_metrics: std::sync::OnceLock::new(),
            calvin_completion_registry: std::sync::OnceLock::new(),
            ollp_orchestrator: std::sync::OnceLock::new(),
            limits: nodedb_types::protocol::Limits::default(),
            tuning,
            scheduler_config: crate::config::server::SchedulerConfig::default(),
            data_dir: std::path::PathBuf::new(),
            // Production stores live under real on-disk paths, not a temp dir.
            _test_state_dir: None,
            schema_version: crate::control::server::shared::session::plan_cache::SchemaVersion::new(
            ),
            sequence_registry,
            dml_counter:
                crate::control::server::shared::ddl::neutral::maintenance::auto_analyze::DmlCounter::new(),
            wal_catchup_lsn: AtomicU64::new(0),
            last_applied_calvin_epoch: Arc::new(AtomicU64::new(0)),
            calvin_apply_results: Arc::new(Mutex::new(std::collections::HashMap::new())),
            calvin_lock_managers: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            hot_key_table: Arc::new(Mutex::new(
                crate::control::cluster::calvin::scheduler::lock::HotKeyTable::new(),
            )),
            calvin_promotion_senders: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            write_order_locks: Arc::new(
                crate::control::server::shared::write_admission::KeyedWriteOrderLock::new(),
            ),
            autocommit_lock_seq: std::sync::atomic::AtomicU32::new(0),
            calvin_counters: crate::control::state::CalvinCounters {
                write_versions_recorded: Arc::new(AtomicU64::new(0)),
                read_set_validation_failures: Arc::new(AtomicU64::new(0)),
                commits_flushed: Arc::new(AtomicU64::new(0)),
                commits_dropped: Arc::new(AtomicU64::new(0)),
            },
            presence: Arc::new(tokio::sync::RwLock::new(
                crate::control::server::sync::presence::PresenceManager::new(
                    crate::control::server::sync::presence::PresenceConfig::default(),
                ),
            )),
            permission_cache: Arc::new(tokio::sync::RwLock::new(permission_cache)),
            gateway_invalidator: std::sync::OnceLock::new(),
            gateway: std::sync::OnceLock::new(),
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
            shuffle_registry: Arc::new(
                crate::control::server::shuffle::ShuffleReceiverRegistry::new(
                    catalog_path
                        .parent()
                        .unwrap_or(std::path::Path::new("."))
                        .to_path_buf(),
                ),
            ),
            shutdown: Arc::clone(&shutdown),
            loop_registry: Arc::clone(&loop_registry),
            startup: Arc::clone(&startup_gate),
        });

        post_init::hydrate_caches(&state);
        post_init::spawn_array_gc(&state);

        Ok(state)
    }
}
