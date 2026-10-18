// SPDX-License-Identifier: BUSL-1.1

//! Synthetic pseudo-collection keys folding tenant permission-tree / RLS
//! policy versions into a [`super::GatewayVersionSet`].

/// Prefix for the synthetic entry carrying a tenant's permission-tree
/// version. `\0` makes it unrepresentable as a real collection name, so it
/// can never collide with one.
const PERMISSION_TREE_VERSION_KEY_PREFIX: &str = "\0__permission_tree_version::";

/// Prefix for the synthetic entry carrying a tenant's RLS policy version.
const RLS_VERSION_KEY_PREFIX: &str = "\0__rls_version::";

/// The synthetic pseudo-collection key a tenant's permission-tree version is
/// folded into the set under, so a revoked grant makes a cached gateway plan
/// unlookupable exactly like a bumped descriptor does.
pub fn permission_tree_version_key(tenant_id: u64) -> String {
    format!("{PERMISSION_TREE_VERSION_KEY_PREFIX}{tenant_id}")
}

/// The synthetic pseudo-collection key a tenant's RLS policy version is
/// folded into the set under.
pub fn rls_version_key(tenant_id: u64) -> String {
    format!("{RLS_VERSION_KEY_PREFIX}{tenant_id}")
}

/// Prefix for the synthetic entry carrying a continuous aggregate's version.
const CONTINUOUS_AGGREGATE_VERSION_KEY_PREFIX: &str = "\0__continuous_aggregate::";

/// The synthetic pseudo-collection key a continuous aggregate's catalog
/// version is folded into the set under, so a plan that reads the aggregate
/// as a relation stops matching once the aggregate is dropped or
/// re-created.
pub fn continuous_aggregate_version_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{CONTINUOUS_AGGREGATE_VERSION_KEY_PREFIX}{database_id}:{tenant_id}:{name}")
}

/// The aggregate name a [`continuous_aggregate_version_key`] names, when
/// `key` is one of `database_id` / `tenant_id`.
pub fn continuous_aggregate_of_key(key: &str, database_id: u64, tenant_id: u64) -> Option<&str> {
    key.strip_prefix(CONTINUOUS_AGGREGATE_VERSION_KEY_PREFIX)?
        .strip_prefix(&format!("{database_id}:{tenant_id}:"))
}

/// Prefix for the synthetic entry carrying an array's catalog version.
const ARRAY_VERSION_KEY_PREFIX: &str = "\0__array::";

/// The synthetic pseudo-collection key an array's catalog version is folded
/// into the set under, so a plan over the array stops matching once the
/// array is altered, dropped, or re-created.
pub fn array_version_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{ARRAY_VERSION_KEY_PREFIX}{database_id}:{tenant_id}:{name}")
}

/// The array name an [`array_version_key`] names, when `key` is one of
/// `database_id` / `tenant_id`.
pub fn array_of_key(key: &str, database_id: u64, tenant_id: u64) -> Option<&str> {
    key.strip_prefix(ARRAY_VERSION_KEY_PREFIX)?
        .strip_prefix(&format!("{database_id}:{tenant_id}:"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pseudo_keys_are_scoped_per_tenant() {
        assert_ne!(
            permission_tree_version_key(1),
            permission_tree_version_key(2)
        );
        assert_ne!(rls_version_key(1), rls_version_key(2));
        assert_ne!(permission_tree_version_key(1), rls_version_key(1));
    }

    #[test]
    fn array_key_round_trips_only_in_its_scope() {
        let key = array_version_key(7, 3, "scene");
        assert_eq!(array_of_key(&key, 7, 3), Some("scene"));
        assert_eq!(array_of_key(&key, 7, 4), None);
        assert_eq!(array_of_key(&key, 8, 3), None);
        assert_eq!(
            array_of_key(&continuous_aggregate_version_key(7, 3, "scene"), 7, 3),
            None
        );
    }
}
