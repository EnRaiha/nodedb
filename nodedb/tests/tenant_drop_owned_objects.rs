// SPDX-License-Identifier: BUSL-1.1

//! Tenant teardown must terminate ownership inside the tenant being removed.

mod common;

use common::pgwire_harness::TestServer;

async fn assert_tenant_drop_removes_admin_owned_collection(retained: bool) {
    let server = TestServer::start().await;
    let tenant = if retained {
        "retained_scope"
    } else {
        "active_scope"
    };
    let user = if retained {
        "retained_owner"
    } else {
        "active_owner"
    };
    let collection = if retained {
        "retained_tenant_notes"
    } else {
        "active_tenant_notes"
    };
    let admin = format!("{tenant}_admin");

    server
        .exec(&format!("CREATE TENANT {tenant}"))
        .await
        .expect("create tenant");
    let tenant_id = server
        .shared
        .credentials
        .catalog()
        .find_tenant_by_name(tenant)
        .expect("read tenant")
        .expect("tenant exists")
        .tenant_id;
    server
        .exec(&format!(
            "CREATE USER {user} PASSWORD 'probe-secret-99' TENANT {tenant}"
        ))
        .await
        .expect("create tenant user");
    server
        .exec(&format!("GRANT tenant_admin TO {user}"))
        .await
        .expect("grant tenant administration");

    let (client, connection) = server
        .connect_as(user, "probe-secret-99")
        .await
        .expect("connect as tenant owner");
    client
        .simple_query(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, body TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create tenant collection");
    if !retained {
        client
            .simple_query(&format!(
                "CREATE RLS POLICY tenant_guard ON {collection} FOR READ USING (id = id)"
            ))
            .await
            .expect("create collection-scoped RLS policy");
    }
    if retained {
        client
            .simple_query(&format!("DROP COLLECTION {collection}"))
            .await
            .expect("retain dropped collection tombstone");
    }
    drop(client);
    connection.abort();

    server
        .exec(&format!("DROP USER {user}"))
        .await
        .expect("drop ordinary tenant owner");
    assert!(
        server
            .shared
            .credentials
            .catalog()
            .load_all_owners()
            .expect("load owners")
            .iter()
            .any(|owner| {
                owner.tenant_id == tenant_id
                    && owner.object_name == collection
                    && owner.owner_username == admin
            }),
        "the lifecycle administrator must inherit collection ownership before teardown"
    );

    server
        .exec(&format!("DROP TENANT {tenant}"))
        .await
        .expect("tenant teardown must remove its terminal owner and owned objects");

    assert!(
        server
            .shared
            .credentials
            .catalog()
            .find_tenant_by_name(tenant)
            .expect("read dropped tenant")
            .is_none()
    );
    assert!(server.shared.credentials.get_user(&admin).is_none());
    assert!(
        server
            .shared
            .credentials
            .catalog()
            .load_all_rls_policies()
            .expect("load RLS policies after tenant drop")
            .iter()
            .all(|policy| policy.tenant_id != tenant_id),
        "tenant teardown must not leave collection policies behind"
    );
    assert!(
        server
            .shared
            .credentials
            .catalog()
            .load_all_owners()
            .expect("load owners after tenant drop")
            .iter()
            .all(|owner| owner.tenant_id != tenant_id),
        "tenant teardown must not leave owner rows referencing the removed principal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_tenant_removes_active_objects_owned_by_its_lifecycle_admin() {
    assert_tenant_drop_removes_admin_owned_collection(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_tenant_removes_retained_objects_owned_by_its_lifecycle_admin() {
    assert_tenant_drop_removes_admin_owned_collection(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_tenant_rejects_transactional_buffering_without_side_effects() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TENANT rollback_scope")
        .await
        .expect("create tenant");
    let error = server
        .exec("BEGIN; DROP TENANT rollback_scope")
        .await
        .expect_err("tenant teardown must not enter the generic DDL buffer");
    assert!(error.contains("0A000"), "unexpected error: {error}");
    server.exec("ROLLBACK").await.expect("rollback transaction");
    assert!(
        server
            .shared
            .credentials
            .catalog()
            .find_tenant_by_name("rollback_scope")
            .expect("read tenant")
            .is_some(),
        "rejected transactional teardown must leave tenant state untouched"
    );
}
