// SPDX-License-Identifier: BUSL-1.1

//! Regression: `DROP USER` must reassign EVERY owner-bearing object the
//! user owns — not just collections — and revoke every grant made to
//! the user, so no dangling `owner → user` or `permission.grantee →
//! user` reference survives into the next boot's catalog integrity
//! check.
//!
//! Before the fix, `drop_user` only reassigned collections it could
//! derive from the user's grant targets. A user that owned a SEQUENCE,
//! FUNCTION, (etc.) left those owner rows pointing at the deleted user.
//! On the next boot `verify_redb_integrity` flags each as a
//! `DanglingReference { from_kind: "owner" }`; the boot repair pass has
//! no case for `DanglingReference`, so it survives into
//! `VerifyReport.integrity_violations`, `is_acceptable()` returns
//! `false`, and startup aborts — a permanently unbootable data dir.
//!
//! This test plants a collection, a sequence, and a function (each with
//! its primary row + owner row) plus a grant, all owned by / granted to
//! a victim user, then issues a real `DROP USER` over pgwire and asserts
//! the boot integrity+repair pass is acceptable and every owner row was
//! reassigned to the tenant admin. It fails on the pre-fix tree because
//! the non-collection owner rows stay pointed at the deleted user.

mod catalog_integrity_helpers;
mod common;

use catalog_integrity_helpers::{TENANT, make_collection, make_function, make_sequence};
use common::pgwire_harness::TestServer;
use nodedb::control::cluster::recovery_check::divergence::DivergenceKind;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;
use nodedb::control::cluster::verify_and_repair;
use nodedb::control::security::catalog::SystemCatalog;
use nodedb::control::security::catalog::auth_types::{StoredOwner, StoredPermission};
use nodedb::control::security::identity::Role;
use nodedb::types::TenantId;

const VICTIM: &str = "victim_owner";
/// The tenant-admin convention `drop_user` reassigns to: `{tenant}_admin`.
const ADMIN_TARGET: &str = "1_admin";

fn plant_owner(catalog: &SystemCatalog, object_type: &str, name: &str, owner: &str) {
    catalog
        .put_owner(&StoredOwner {
            object_type: object_type.to_string(),
            object_name: name.to_string(),
            tenant_id: TENANT,
            owner_username: owner.to_string(),
        })
        .unwrap();
}

fn owner_of(catalog: &SystemCatalog, object_type: &str, name: &str) -> String {
    catalog
        .load_all_owners()
        .unwrap()
        .into_iter()
        .find(|o| o.object_type == object_type && o.object_name == name && o.tenant_id == TENANT)
        .unwrap_or_else(|| panic!("owner row for {object_type} '{name}' vanished"))
        .owner_username
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_user_reassigns_every_owner_bearing_kind_and_sweeps_grants() {
    let server = TestServer::start().await;
    let catalog = server.shared.credentials.catalog().clone();

    // The reassignment target `{tenant}_admin` must resolve to a real
    // StoredUser, else its owner rows would themselves dangle.
    server
        .shared
        .credentials
        .create_user(
            ADMIN_TARGET,
            "pw",
            TenantId::new(TENANT),
            vec![Role::TenantAdmin],
        )
        .expect("create tenant admin");
    server
        .shared
        .credentials
        .create_user(VICTIM, "pw", TenantId::new(TENANT), vec![Role::ReadWrite])
        .expect("create victim user");

    // Plant a representative spread of owner-bearing objects owned by the
    // victim: a collection (the only kind the pre-fix code handled), plus
    // a sequence and a function (kinds it did NOT handle). Each needs its
    // primary row AND its owner row.
    let mut coll = make_collection("victim_coll");
    coll.owner = VICTIM.to_string();
    catalog
        .put_collection(nodedb_types::DatabaseId::DEFAULT, &coll)
        .unwrap();
    plant_owner(&catalog, "collection", "victim_coll", VICTIM);

    let mut seq = make_sequence("victim_seq");
    seq.owner = VICTIM.to_string();
    catalog.put_sequence(&seq).unwrap();
    plant_owner(&catalog, "sequence", "victim_seq", VICTIM);

    let mut func = make_function("victim_fn");
    func.owner = VICTIM.to_string();
    catalog.put_function(&func).unwrap();
    plant_owner(&catalog, "function", "victim_fn", VICTIM);

    // A grant made TO the victim — its persistent row would dangle too
    // (integrity Check 3) unless drop_user sweeps it. Install into both
    // redb and the in-memory permission store the sweep reads from.
    let grant = StoredPermission {
        target: "collection:1:victim_coll".to_string(),
        grantee: format!("user:{VICTIM}"),
        permission: "read".to_string(),
        granted_by: "nodedb".to_string(),
        granted_at: 0,
    };
    catalog.put_permission(&grant).unwrap();
    server
        .shared
        .permissions
        .install_replicated_permission(&grant);

    // Sanity: pre-drop the catalog is internally consistent (every
    // primary has its owner row, every owner/grant resolves to a user).
    assert!(
        verify_redb_integrity(&catalog).is_empty(),
        "planted state should be integrity-clean before the drop: {:?}",
        verify_redb_integrity(&catalog)
    );

    // The operation under test: a real DROP USER over pgwire as the
    // bootstrap superuser.
    server
        .exec(&format!("DROP USER {VICTIM}"))
        .await
        .expect("DROP USER should succeed");

    // 1. No dangling owner/permission references remain — this is the
    //    exact condition the boot check aborts on.
    let violations = verify_redb_integrity(&catalog);
    let dangling: Vec<_> = violations
        .iter()
        .filter(|v| {
            matches!(
                &v.kind,
                DivergenceKind::DanglingReference { from_kind, .. }
                    if *from_kind == "owner" || *from_kind == "permission"
            )
        })
        .collect();
    assert!(
        dangling.is_empty(),
        "DROP USER must leave no dangling owner/permission references — \
         every owned object reassigned and every grant swept. Got: {dangling:?}"
    );

    // 2. Boot repair pass would accept this catalog (startup would not
    //    abort).
    let report = verify_and_repair(&server.shared)
        .await
        .expect("verify_and_repair");
    assert!(
        report.is_acceptable(),
        "boot catalog sanity check must accept the post-drop catalog; \
         integrity_violations: {:?}",
        report.integrity_violations
    );

    // 3. Every owned object is now owned by the tenant admin, not the
    //    deleted user.
    assert_eq!(
        owner_of(&catalog, "collection", "victim_coll"),
        ADMIN_TARGET
    );
    assert_eq!(owner_of(&catalog, "sequence", "victim_seq"), ADMIN_TARGET);
    assert_eq!(owner_of(&catalog, "function", "victim_fn"), ADMIN_TARGET);

    // 3b. The in-band owner on each primary row was rewritten in lockstep.
    assert_eq!(
        catalog
            .get_sequence(TENANT, "victim_seq")
            .unwrap()
            .unwrap()
            .owner,
        ADMIN_TARGET
    );
    assert_eq!(
        catalog
            .get_function(TENANT, "victim_fn")
            .unwrap()
            .unwrap()
            .owner,
        ADMIN_TARGET
    );

    // 4. The grant to the victim is gone (no dangling grantee).
    assert!(
        !catalog
            .load_all_permissions()
            .unwrap()
            .iter()
            .any(|p| p.grantee == format!("user:{VICTIM}")),
        "every grant made to the dropped user must be swept"
    );
}
