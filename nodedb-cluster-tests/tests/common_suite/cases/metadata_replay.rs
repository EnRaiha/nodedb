// SPDX-License-Identifier: BUSL-1.1
//! Regression coverage for group-0 metadata replay damaging a later
//! incarnation of the same name.
//!
//! Each test drives a create/drop/recreate (or clone/move) sequence, then a
//! graceful WAL-only restart of a single-node metadata group, and asserts the
//! post-restart state still matches the pre-restart state, not the historical
//! entry replay reasserts.
//!
//! - `DeleteConsumerGroup` and `DeleteChangeStream` are fenced by the HLC
//!   incarnation stamp (`nodedb/src/control/catalog_entry/incarnation/`).
//! - `CloneDatabase` applies once: its lineage edge marks it applied.
//! - `MoveTenantCutover` writes only the snapshot it carries. Every later
//!   change to the rows it touches is a later log entry, so replay restores it.

use crate::common;

use common::cluster_harness::TestClusterNode;
use common::cluster_harness::shared_steps::{database_id, wait_for_single_node_ready};

use nodedb::event::cdc::CdcOffset;
use nodedb_types::DatabaseId;

const TENANT: u64 = 1;

/// Whether `name` is an active collection under `database_id` for
/// `tenant_id`, read through the local `SystemCatalog` redb.
fn has_collection(
    node: &TestClusterNode,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> bool {
    node.shared
        .credentials
        .catalog()
        .load_collections_for_tenant(database_id, tenant_id)
        .expect("load collections")
        .iter()
        .any(|c| c.name == name)
}

/// CREATE, DROP, and CREATE again on the same consumer-group name, each
/// incarnation committing its own offset, all before a graceful restart of
/// the single-node metadata group. `forget_offsets`
/// (`nodedb/src/control/catalog_entry/post_apply/consumer_group.rs`)
/// deletes the node-local `offset_store` row by stream+group name alone, so
/// replaying the first incarnation's `DeleteConsumerGroup` must not erase
/// the second incarnation's already-persisted offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replayed_consumer_group_drop_keeps_recreated_group_offsets() {
    const TOPIC: &str = "mrt_cg_topic";
    const GROUP: &str = "mrt_cg_group";
    const RECREATED_OFFSET: u64 = 42;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
    wait_for_single_node_ready(&node).await;

    node.client
        .simple_query(&format!(
            "CREATE TOPIC {TOPIC} WITH (RETENTION = '2 hours')"
        ))
        .await
        .expect("create topic");
    node.client
        .simple_query(&format!("CREATE CONSUMER GROUP {GROUP} ON {TOPIC}"))
        .await
        .expect("create first incarnation of the group");
    node.client
        .simple_query(&format!(
            "COMMIT OFFSET PARTITION 0 AT 0:5 ON {TOPIC} CONSUMER GROUP {GROUP}"
        ))
        .await
        .expect("commit an offset on the first incarnation");

    node.client
        .simple_query(&format!("DROP CONSUMER GROUP {GROUP} ON {TOPIC}"))
        .await
        .expect("drop the first incarnation");

    node.client
        .simple_query(&format!("CREATE CONSUMER GROUP {GROUP} ON {TOPIC}"))
        .await
        .expect("create the second incarnation of the group");
    node.client
        .simple_query(&format!(
            "COMMIT OFFSET PARTITION 0 AT 0:{RECREATED_OFFSET} ON {TOPIC} CONSUMER GROUP {GROUP}"
        ))
        .await
        .expect("commit an offset on the second incarnation");

    let stream = format!("topic:{TOPIC}");
    assert_eq!(
        node.shared
            .offset_store
            .get_offset(DatabaseId::DEFAULT, TENANT, &stream, GROUP, 0),
        CdcOffset::whole_index(RECREATED_OFFSET),
        "the second incarnation must hold its committed offset before restart"
    );

    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path)
        .await
        .expect("restart against the persisted metadata log");
    wait_for_single_node_ready(&node).await;

    assert_eq!(
        node.shared
            .offset_store
            .get_offset(DatabaseId::DEFAULT, TENANT, &stream, GROUP, 0),
        CdcOffset::whole_index(RECREATED_OFFSET),
        "replaying the first incarnation's DeleteConsumerGroup must not wipe the second \
         incarnation's committed offset"
    );

    node.shutdown().await;
}

/// CREATE, DROP, and CREATE again on the same change-stream name, the
/// second incarnation carrying its own consumer group and committed offset,
/// all before a graceful restart of the single-node metadata group.
/// `DeleteChangeStream`'s post-apply
/// (`nodedb/src/control/catalog_entry/post_apply/change_stream.rs`)
/// unregisters the stream by name and cascades offset deletion over every
/// group currently attached to that name, so replaying the first
/// incarnation's drop must not unregister the second incarnation or wipe
/// its group's offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replayed_change_stream_drop_keeps_recreated_stream() {
    const SRC: &str = "mrt_cs_src";
    const STREAM: &str = "mrt_cs_stream";
    const GROUP: &str = "mrt_cs_group";
    const OFFSET: u64 = 7;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
    wait_for_single_node_ready(&node).await;

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {SRC} (id TEXT PRIMARY KEY, val BIGINT) WITH (engine='document_strict')"
        ))
        .await
        .expect("create the collection the stream watches");
    node.client
        .simple_query(&format!("CREATE CHANGE STREAM {STREAM} ON {SRC}"))
        .await
        .expect("create the first incarnation of the stream");

    node.client
        .simple_query(&format!("DROP CHANGE STREAM {STREAM}"))
        .await
        .expect("drop the first incarnation");

    node.client
        .simple_query(&format!("CREATE CHANGE STREAM {STREAM} ON {SRC}"))
        .await
        .expect("create the second incarnation of the stream");
    node.client
        .simple_query(&format!("CREATE CONSUMER GROUP {GROUP} ON {STREAM}"))
        .await
        .expect("create a group on the second incarnation");
    // Stands in for consumption progress: the offset a faulty replay wipes lives in
    // the same node-local store regardless of how it was produced.
    node.client
        .simple_query(&format!(
            "COMMIT OFFSET PARTITION 0 AT 0:{OFFSET} ON {STREAM} CONSUMER GROUP {GROUP}"
        ))
        .await
        .expect("commit an offset on the second incarnation's group");

    assert!(
        node.has_change_stream(DatabaseId::DEFAULT, TENANT, STREAM),
        "the second incarnation must be registered before restart"
    );
    assert_eq!(
        node.shared
            .offset_store
            .get_offset(DatabaseId::DEFAULT, TENANT, STREAM, GROUP, 0),
        CdcOffset::whole_index(OFFSET),
        "the second incarnation's group must hold its committed offset before restart"
    );

    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path)
        .await
        .expect("restart against the persisted metadata log");
    wait_for_single_node_ready(&node).await;

    assert!(
        node.has_change_stream(DatabaseId::DEFAULT, TENANT, STREAM),
        "replaying the first incarnation's DeleteChangeStream must not unregister the \
         second incarnation's stream"
    );
    assert_eq!(
        node.shared
            .offset_store
            .get_offset(DatabaseId::DEFAULT, TENANT, STREAM, GROUP, 0),
        CdcOffset::whole_index(OFFSET),
        "replaying the first incarnation's DeleteChangeStream must not wipe the second \
         incarnation's group offset"
    );

    node.shutdown().await;
}

/// CLONE a database, drop one collection from the child, and create a new
/// collection in the source, all before a graceful restart of the
/// single-node metadata group. `clone_apply`
/// (`nodedb/src/control/catalog_entry/apply/database.rs`) enumerates the
/// source's currently active collections when it applies, so replay must not
/// re-apply it: that resurrects the dropped child collection and pull in
/// the source's later collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replayed_clone_keeps_child_changes() {
    const SRC_DB: &str = "mrt_clone_src";
    const CHILD_DB: &str = "mrt_clone_child";
    const DROPPED: &str = "mrt_will_drop";
    const STAYS: &str = "mrt_stays";
    const NEW_IN_SRC: &str = "mrt_new_in_src";

    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
    wait_for_single_node_ready(&node).await;

    node.client
        .simple_query(&format!("CREATE DATABASE {SRC_DB}"))
        .await
        .expect("create the source database");
    node.client
        .simple_query(&format!("USE DATABASE {SRC_DB}"))
        .await
        .expect("use the source database");
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {DROPPED} (id TEXT PRIMARY KEY, val TEXT)"
        ))
        .await
        .expect("create the collection the child will drop");
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {STAYS} (id TEXT PRIMARY KEY, val TEXT)"
        ))
        .await
        .expect("create the collection that stays in both databases");

    node.client
        .simple_query("USE DATABASE default")
        .await
        .expect("use the default database");
    node.client
        .simple_query(&format!("CLONE DATABASE {CHILD_DB} FROM {SRC_DB}"))
        .await
        .expect("clone the database");

    let child_db = database_id(&node, CHILD_DB);
    let src_db = database_id(&node, SRC_DB);
    assert!(
        has_collection(&node, child_db, TENANT, DROPPED),
        "the clone must carry the source's active collections at clone time"
    );

    node.client
        .simple_query(&format!("USE DATABASE {CHILD_DB}"))
        .await
        .expect("use the child database");
    node.client
        .simple_query(&format!("DROP COLLECTION {DROPPED}"))
        .await
        .expect("drop the collection from the child after cloning");

    node.client
        .simple_query(&format!("USE DATABASE {SRC_DB}"))
        .await
        .expect("use the source database");
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {NEW_IN_SRC} (id TEXT PRIMARY KEY, val TEXT)"
        ))
        .await
        .expect("create a collection in the source after cloning");

    node.client
        .simple_query("USE DATABASE default")
        .await
        .expect("use the default database");

    assert!(
        !has_collection(&node, child_db, TENANT, DROPPED),
        "the child must not see the dropped collection before restart"
    );
    assert!(
        !has_collection(&node, child_db, TENANT, NEW_IN_SRC),
        "the child must not see the source's later collection before restart"
    );
    assert!(has_collection(&node, src_db, TENANT, STAYS));

    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path)
        .await
        .expect("restart against the persisted metadata log");
    wait_for_single_node_ready(&node).await;

    assert!(
        !has_collection(&node, child_db, TENANT, DROPPED),
        "replaying CloneDatabase must not resurrect a collection dropped from the child"
    );
    assert!(
        !has_collection(&node, child_db, TENANT, NEW_IN_SRC),
        "replaying CloneDatabase must not pull in a collection created in the source \
         after the clone"
    );

    node.shutdown().await;
}

/// MOVE a tenant's database to a target database, ALTER the moved
/// collection's owner in the target, and create a same-name collection in
/// the source, all before a graceful restart of the single-node metadata
/// group. `move_cutover` (`nodedb/src/control/catalog_entry/apply/tenant.rs`)
/// rewrites the target from the collection snapshot carried at propose
/// time and deletes source rows by name, so replaying it must not revert
/// the target's later ALTER or delete the source's later same-name
/// collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replayed_move_tenant_keeps_later_changes() {
    const SRC_DB: &str = "mrt_move_src";
    const TGT_DB: &str = "mrt_move_tgt";
    const MOVED: &str = "mrt_moved";
    const NEW_OWNER: &str = "mrt_new_owner";

    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
    wait_for_single_node_ready(&node).await;

    node.client
        .simple_query(&format!(
            "CREATE USER {NEW_OWNER} WITH PASSWORD 'pw' ROLE READWRITE"
        ))
        .await
        .expect("create the user the target ALTER assigns");

    node.client
        .simple_query(&format!("CREATE DATABASE {SRC_DB}"))
        .await
        .expect("create the source database");
    node.client
        .simple_query(&format!("USE DATABASE {SRC_DB}"))
        .await
        .expect("use the source database");
    node.client
        .simple_query("CREATE TENANT mrt_tenant ID 50")
        .await
        .expect("create the tenant to move");
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {MOVED} (id STRING PRIMARY KEY, val STRING) WITH (engine='kv')"
        ))
        .await
        .expect("create the collection the tenant moves");
    node.client
        .simple_query(&format!(
            "INSERT INTO {MOVED} (id, val) VALUES ('k1', 'v1')"
        ))
        .await
        .expect("seed a row");

    node.client
        .simple_query("USE DATABASE default")
        .await
        .expect("use the default database");
    node.client
        .simple_query(&format!("CREATE DATABASE {TGT_DB}"))
        .await
        .expect("create the target database");
    node.client
        .simple_query(&format!("USE DATABASE {TGT_DB}"))
        .await
        .expect("use the target database");
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {MOVED} (id STRING PRIMARY KEY, val STRING) WITH (engine='kv')"
        ))
        .await
        .expect("pre-create the matching target collection MOVE TENANT requires");

    node.client
        .simple_query("USE DATABASE default")
        .await
        .expect("use the default database");
    node.client
        .simple_query(&format!("MOVE TENANT mrt_tenant FROM {SRC_DB} TO {TGT_DB}"))
        .await
        .expect("move the tenant");

    let target_db = database_id(&node, TGT_DB);
    let source_db = database_id(&node, SRC_DB);

    node.client
        .simple_query(&format!("USE DATABASE {TGT_DB}"))
        .await
        .expect("use the target database");
    node.client
        .simple_query(&format!("ALTER COLLECTION {MOVED} OWNER TO {NEW_OWNER}"))
        .await
        .expect("alter the moved collection's owner in the target");

    node.client
        .simple_query(&format!("USE DATABASE {SRC_DB}"))
        .await
        .expect("use the source database");
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {MOVED} (id STRING PRIMARY KEY, val STRING) WITH (engine='kv')"
        ))
        .await
        .expect("create a same-name collection in the source after the move");

    node.client
        .simple_query("USE DATABASE default")
        .await
        .expect("use the default database");

    assert_eq!(
        node.owner_of("collection", target_db.as_u64(), TENANT, MOVED)
            .as_deref(),
        Some(NEW_OWNER),
        "the target must carry the ALTER before restart"
    );
    assert!(
        has_collection(&node, source_db, TENANT, MOVED),
        "the source must carry its later same-name collection before restart"
    );

    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path)
        .await
        .expect("restart against the persisted metadata log");
    wait_for_single_node_ready(&node).await;

    assert_eq!(
        node.owner_of("collection", target_db.as_u64(), TENANT, MOVED)
            .as_deref(),
        Some(NEW_OWNER),
        "replaying MoveTenantCutover must not revert the target's later ALTER OWNER"
    );
    assert!(
        has_collection(&node, source_db, TENANT, MOVED),
        "replaying MoveTenantCutover must not delete the source's later same-name collection"
    );

    node.shutdown().await;
}
