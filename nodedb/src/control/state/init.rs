// SPDX-License-Identifier: BUSL-1.1

//! SharedState constructors: new (test) and new_with_credentials (test+catalog).

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
    /// Monotonic counter for unique test temp dirs (prevents redb lock collisions).
    fn unique_test_id() -> u64 {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Create shared state with a pre-built credential store (for tests that need catalog).
    pub fn new_with_credentials(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        credentials: Arc<CredentialStore>,
    ) -> Arc<Self> {
        let wal_for_assigner = Arc::clone(&wal);
        let mut state = Self::new_inner(dispatcher, wal);
        if let Some(s) = Arc::get_mut(&mut state) {
            // Rebuild the surrogate assigner against the supplied
            // credential store. `new_inner` constructs the assigner
            // from a fresh in-memory `CredentialStore` whose
            // `catalog()` is `None`, which causes every `assign()`
            // call to short-circuit to `Surrogate::ZERO` — collapsing
            // every row in every test to the same substrate key.
            let registry = Arc::clone(&s.surrogate_registry);
            // Seed the registry's high-watermark AND applied-reserve cursor
            // from the catalog so restarts in a re-opened test fixture pick up
            // where the previous session left off — and so cluster-mode
            // metadata-log replay skips already-applied reservations rather
            // than double-counting `G`.
            if let Some(catalog) = credentials.catalog()
                && let Ok(hwm) = catalog.get_surrogate_hwm()
                && let Ok(reserve_index) = catalog.get_surrogate_reserve_index()
                && let Ok(mut reg) = registry.write()
            {
                *reg = crate::control::surrogate::SurrogateRegistry::from_persisted(
                    hwm,
                    reserve_index,
                );
            }
            let wal_appender: Arc<dyn crate::control::surrogate::SurrogateWalAppender> = Arc::new(
                crate::control::surrogate::WalSurrogateAppender::new(wal_for_assigner),
            );
            s.surrogate_assigner = Arc::new(crate::control::surrogate::SurrogateAssigner::new(
                Arc::clone(&registry),
                Arc::clone(&credentials),
                wal_appender,
            ));
            s.credentials = credentials;
        }
        state
    }

    /// Create shared state with in-memory credential store (for tests).
    pub fn new(dispatcher: Dispatcher, wal: Arc<WalManager>) -> Arc<Self> {
        Self::new_inner(dispatcher, wal)
    }

    fn new_inner(dispatcher: Dispatcher, wal: Arc<WalManager>) -> Arc<Self> {
        let shutdown = Arc::new(crate::control::shutdown::ShutdownWatch::new());
        let loop_registry = Arc::new(crate::control::shutdown::LoopRegistry::new());
        // Test helpers get a pre-fired gate so listeners start accepting
        // immediately. Production code (main.rs) replaces this with a real
        // StartupSequencer after calling `SharedState::open`.
        let startup_gate = crate::control::startup::StartupGate::pre_fired();
        let test_id = Self::unique_test_id();
        let test_credentials = Arc::new(CredentialStore::new());
        let test_surrogate_registry: crate::control::surrogate::SurrogateRegistryHandle = Arc::new(
            std::sync::RwLock::new(crate::control::surrogate::SurrogateRegistry::new()),
        );
        let test_surrogate_assigner = Arc::new(crate::control::surrogate::SurrogateAssigner::new(
            Arc::clone(&test_surrogate_registry),
            Arc::clone(&test_credentials),
            Arc::new(crate::control::surrogate::NoopWalAppender),
        ));
        let shared_audit = Arc::new(Mutex::new(AuditLog::new(10_000)));
        let test_session_registry =
            Arc::new(crate::control::security::sessions::SessionRegistry::new());
        let (si_bus, uc_bus, bus_consumer_task) = super::buses_init::init_security_buses(
            Arc::clone(&shared_audit),
            Arc::clone(&test_session_registry),
        );
        let bus_consumer_handle = Some(bus_consumer_task);
        // Wire buses into the credential store so test mutations publish events.
        test_credentials.set_buses(
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
            quiesce: crate::bridge::quiesce::CollectionQuiesce::new(),
            http_client: Arc::new(reqwest::Client::new()),
            credentials: Arc::clone(&test_credentials),
            audit: shared_audit,
            api_keys: ApiKeyStore::new(),
            roles: RoleStore::new(),
            permissions: PermissionStore::new(),
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
            rls: RlsPolicyStore::new(),
            blacklist: crate::control::security::blacklist::store::BlacklistStore::new(),
            auth_users: crate::control::security::jit::auth_user::AuthUserStore::new(),
            orgs: crate::control::security::org::store::OrgStore::new(),
            scope_defs: crate::control::security::scope::store::ScopeStore::new(),
            scope_grants: crate::control::security::scope::grant::ScopeGrantStore::new(),
            rate_limiter: crate::control::security::ratelimit::limiter::RateLimiter::default(),
            session_handles: crate::control::security::session_handle::SessionHandleStore::default(
            ),
            session_registry: test_session_registry,
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
            audit_retention_days: 0,
            audit_max_entries: 0,
            idle_timeout_secs: 0,
            session_absolute_timeout_secs: 0,
            ws_sessions: std::sync::RwLock::new(std::collections::HashMap::new()),
            topic_registry: crate::control::pubsub::TopicRegistry::new(10_000),
            shape_registry: Arc::new(crate::control::server::sync::shape::ShapeRegistry::new()),
            change_stream: crate::control::change_stream::ChangeStream::new(4096),
            notify_bus: crate::control::notify_bus::NotifyBus::default(),
            trigger_registry: crate::control::trigger::TriggerRegistry::new(),
            array_catalog: crate::control::array_catalog::ArrayCatalog::handle(),
            array_sync_op_log: {
                std::sync::Arc::new(
                    crate::control::array_sync::OriginOpLog::open_in_memory()
                        .expect("failed to open test array op-log"),
                )
            },
            array_ack_registry: {
                crate::control::array_sync::ArrayAckRegistry::open_in_memory()
                    .expect("failed to open test ack registry")
            },
            array_snapshot_store: {
                crate::control::array_sync::OriginSnapshotStore::open_in_memory()
                    .expect("failed to open test snapshot store")
            },
            array_snapshot_hlcs: std::sync::Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            array_gc_handle: None,
            session_invalidation_bus: si_bus,
            user_change_bus: uc_bus,
            bus_consumer_handle,
            array_sync_schemas: {
                let db = std::sync::Arc::new(
                    redb::Database::builder()
                        .create_with_backend(redb::backends::InMemoryBackend::new())
                        .expect("failed to create test schema_registry db"),
                );
                {
                    let txn = db.begin_write().expect("schema_registry init txn");
                    txn.open_table(redb::TableDefinition::<&[u8], &[u8]>::new(
                        "array_schema_docs",
                    ))
                    .expect("schema_registry init table");
                    txn.commit().expect("schema_registry init commit");
                }
                let replica_id = nodedb_array::sync::ReplicaId::new(0);
                let hlc_gen =
                    std::sync::Arc::new(nodedb_array::sync::HlcGenerator::new(replica_id));
                std::sync::Arc::new(
                    crate::control::array_sync::OriginSchemaRegistry::open(db, replica_id, hlc_gen)
                        .expect("failed to open test array schema registry"),
                )
            },
            array_delivery: std::sync::Arc::new(
                crate::control::array_sync::ArrayDeliveryRegistry::new(),
            ),
            array_subscriber_cursors: {
                let store = crate::control::array_sync::SubscriberStore::in_memory()
                    .expect("failed to open test subscriber store");
                std::sync::Arc::new(crate::control::array_sync::SubscriberMap::new(store))
            },
            array_merger_registry: std::sync::Arc::new(
                crate::control::array_sync::MergerRegistry::new(),
            ),
            mirror_link_registry: Arc::new(crate::control::mirror::MirrorLinkRegistry::new()),
            database_registry: crate::control::database::DatabaseRegistry::new(),
            surrogate_registry: Arc::clone(&test_surrogate_registry),
            surrogate_assigner: Arc::clone(&test_surrogate_assigner),
            block_cache: crate::control::planner::procedural::executor::ProcedureBlockCache::new(
                4096,
            ),
            stream_registry: Arc::new(crate::event::cdc::StreamRegistry::new()),
            cdc_router: Arc::new(crate::event::cdc::CdcRouter::new(Arc::new(
                crate::event::cdc::StreamRegistry::new(),
            ))),
            group_registry: crate::event::cdc::GroupRegistry::new(),
            offset_store: {
                let dir = std::env::temp_dir().join(format!(
                    "nodedb-test-offsets-{}-{test_id}",
                    std::process::id(),
                ));
                Arc::new(
                    crate::event::cdc::OffsetStore::open(&dir)
                        .expect("failed to open test offset store"),
                )
            },
            retention_policy_registry: Arc::new(
                crate::engine::timeseries::retention_policy::RetentionPolicyRegistry::new(),
            ),
            bitemporal_retention_registry: Arc::new(
                crate::engine::bitemporal::BitemporalRetentionRegistry::new(),
            ),
            alert_registry: Arc::new(crate::event::alert::AlertRegistry::new()),
            alert_hysteresis: Arc::new(crate::event::alert::hysteresis::HysteresisManager::new()),
            schedule_registry: Arc::new(crate::event::scheduler::ScheduleRegistry::new()),
            synonym_registry: Arc::new(crate::control::synonym::SynonymRegistry::new()),
            custom_type_registry: Arc::new(crate::control::custom_type::CustomTypeRegistry::new()),
            job_history: {
                let dir = std::env::temp_dir().join(format!(
                    "nodedb-test-history-{}-{test_id}",
                    std::process::id(),
                ));
                Arc::new(
                    crate::event::scheduler::JobHistoryStore::open(&dir)
                        .expect("failed to open test job history"),
                )
            },
            ep_topic_registry: crate::event::topic::EpTopicRegistry::new(),
            webhook_manager: crate::event::webhook::WebhookManager::new(shutdown.raw_receiver()),
            mv_registry: Arc::new(crate::event::streaming_mv::MvRegistry::new()),
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
            mv_persistence: {
                let dir = std::env::temp_dir().join(format!(
                    "nodedb-test-mvstate-{}-{test_id}",
                    std::process::id(),
                ));
                Arc::new(
                    crate::event::streaming_mv::MvPersistence::open(&dir)
                        .expect("failed to open test MV persistence"),
                )
            },
            connections_rejected: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            raft_propose_leader_change_retries: AtomicU64::new(0),
            request_id_counter: AtomicU64::new(1),
            shuffle_id_counter: AtomicU64::new(1),
            system_metrics: Some(Arc::new(crate::control::metrics::SystemMetrics::new())),
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
            producer_registry: None,
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
            tuning: TuningConfig::default(),
            scheduler_config: crate::config::server::SchedulerConfig::default(),
            data_dir: std::path::PathBuf::new(),
            schema_version: crate::control::server::shared::session::plan_cache::SchemaVersion::new(
            ),
            sequence_registry: Arc::new(crate::control::sequence::SequenceRegistry::new()),
            dml_counter:
                crate::control::server::pgwire::ddl::maintenance::auto_analyze::DmlCounter::new(),
            wal_catchup_lsn: AtomicU64::new(0),
            presence: Arc::new(tokio::sync::RwLock::new(
                crate::control::server::sync::presence::PresenceManager::new(
                    crate::control::server::sync::presence::PresenceConfig::default(),
                ),
            )),
            permission_cache: Arc::new(tokio::sync::RwLock::new(
                crate::control::security::permission_tree::PermissionCache::new(),
            )),
            gateway_invalidator: None,
            gateway: None,
            backup_kek: None,
            quarantine_registry: Arc::new(crate::storage::quarantine::QuarantineRegistry::new()),
            admission_registry: Arc::new(
                crate::control::server::admission::AdmissionRegistry::new(),
            ),
            lsn_ms_map: Arc::new(Mutex::new(nodedb_types::temporal::LsnMsMap::new())),
            audit_dml_cache: Arc::new(crate::control::state::audit_dml_cache::AuditDmlCache::new()),
            idle_timeout_cache: Arc::new(
                crate::control::state::idle_timeout_cache::IdleTimeoutCache::new(),
            ),
            collection_to_database: Arc::new(
                crate::control::state::collection_to_database::CollectionToDatabase::new(),
            ),
            materialize_freeze: crate::control::clone::MaterializeFreezeRegistry::new(),
            shuffle_registry: Arc::new(
                // Test path: no catalog data dir, so stage under a process- and
                // test-unique temp subdir to keep concurrent test inboxes
                // isolated.
                crate::control::server::shuffle::ShuffleReceiverRegistry::new(
                    std::env::temp_dir()
                        .join(format!("nodedb-shuffle-{}-{test_id}", std::process::id(),)),
                ),
            ),
            shutdown: Arc::clone(&shutdown),
            loop_registry: Arc::clone(&loop_registry),
            startup: Arc::clone(&startup_gate),
        });
        Self::wire_session_handle_audit(&state);
        state
    }

    /// Point the session-handle store's audit hook at this state's
    /// `AuditLog`, so `SessionHandleFingerprintMismatch` and
    /// `SessionHandleResolveMissSpike` are hash-chained with
    /// the rest of the auth-plane event stream. Captures the audit Arc
    /// directly — a `Weak<Self>` would block the cluster wire-up phase's
    /// `Arc::get_mut` on `SharedState`.
    pub(super) fn wire_session_handle_audit(state: &Arc<Self>) {
        let audit = Arc::clone(&state.audit);
        state.session_handles.set_audit_hook(move |event| {
            if let Ok(mut log) = audit.lock() {
                let _ = log.record(event, None, "session_handle", "");
            }
        });
    }
}
