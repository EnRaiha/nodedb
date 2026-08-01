// SPDX-License-Identifier: BUSL-1.1

//! Apply Function catalog entries to `SystemCatalog` redb.

use crate::control::planner::wasm::{self, WasmConfig};
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::function_types::{FunctionLanguage, StoredFunction};

/// Apply a function definition and its transient WASM module payload.
///
/// A malformed or incomplete WASM proposal is a replicated-state violation:
/// panic before installing metadata so a node cannot advertise a function
/// whose module it cannot execute.
pub fn put(stored: &StoredFunction, catalog: &SystemCatalog) {
    // Validation and local-payload recovery complete before the write
    // transaction starts. The transaction then commits metadata, module
    // installation, and old-module cleanup together.
    let module = validate_module(stored, catalog);
    catalog
        .put_function_with_wasm_module(stored, module.as_deref())
        .unwrap_or_else(|error| panic!("atomic function/WASM catalog put failed: {error}"));

    super::owner::put_parent_owner_in_database(
        object_type::FUNCTION,
        stored.database_id.as_u64(),
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) {
    catalog
        .delete_function_with_unreferenced_wasm(database_id, tenant_id, name)
        .unwrap_or_else(|error| panic!("atomic function/WASM catalog delete failed: {error}"));

    super::owner::delete_parent_owner_in_database(
        object_type::FUNCTION,
        database_id.as_u64(),
        tenant_id,
        name,
        catalog,
    );
}

fn validate_module(stored: &StoredFunction, catalog: &SystemCatalog) -> Option<Vec<u8>> {
    if stored.language == FunctionLanguage::Sql {
        assert!(
            stored.wasm_module.is_none(),
            "SQL function proposals must not carry a WASM module"
        );
        return None;
    }

    let hash = stored
        .wasm_hash
        .as_deref()
        .unwrap_or_else(|| panic!("WASM function proposal is missing wasm_hash"));
    let max_size = WasmConfig::default().max_binary_size;
    let bytes = match stored.wasm_module.as_deref() {
        Some(bytes) => bytes.to_vec(),
        None => wasm::store::load_verified_wasm_binary(catalog, hash, max_size).unwrap_or_else(
            |error| panic!("WASM function proposal lacks a valid local module: {error}"),
        ),
    };
    let computed_hash = wasm::store::validate_wasm_binary(&bytes, max_size)
        .unwrap_or_else(|error| panic!("invalid replicated WASM module: {error}"));
    assert_eq!(
        computed_hash.as_str(),
        hash,
        "replicated WASM module hash does not match wasm_hash"
    );
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::control::planner::wasm::store::{load_wasm_binary, validate_wasm_binary};
    use crate::control::security::catalog::{FunctionParam, FunctionSecurity, FunctionVolatility};

    fn catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn wasm_function(name: &str, bytes: Option<Vec<u8>>) -> StoredFunction {
        let hash = bytes.as_deref().map(|module| {
            validate_wasm_binary(module, WasmConfig::default().max_binary_size).unwrap()
        });
        StoredFunction {
            tenant_id: 1,
            database_id: nodedb_types::DatabaseId::DEFAULT,
            name: name.into(),
            parameters: vec![FunctionParam {
                name: "x".into(),
                data_type: "INT".into(),
            }],
            return_type: "INT".into(),
            body_sql: String::new(),
            compiled_body_sql: None,
            volatility: FunctionVolatility::Volatile,
            security: FunctionSecurity::Invoker,
            language: FunctionLanguage::Wasm,
            wasm_hash: hash,
            wasm_module: bytes,
            dependencies: vec![],
            wasm_fuel: 1_000_000,
            wasm_memory: 16 * 1024 * 1024,
            owner: "admin".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn follower_apply_installs_module_but_not_metadata_payload() {
        let catalog = catalog();
        let bytes = b"\0asmfollower".to_vec();
        let stored = wasm_function("f", Some(bytes.clone()));
        let hash = stored.wasm_hash.clone().unwrap();

        put(&stored, &catalog);

        assert_eq!(load_wasm_binary(&catalog, &hash).unwrap(), bytes);
        assert!(
            catalog
                .get_function(1, "f")
                .unwrap()
                .unwrap()
                .wasm_module
                .is_none()
        );
    }

    #[test]
    fn missing_or_mismatched_module_never_installs_metadata() {
        let catalog = catalog();
        let mut missing = wasm_function("missing", None);
        missing.wasm_hash = Some(
            validate_wasm_binary(b"\0asmmissing", WasmConfig::default().max_binary_size).unwrap(),
        );
        let missing_result = catch_unwind(AssertUnwindSafe(|| put(&missing, &catalog)));
        assert!(missing_result.is_err());
        assert!(catalog.get_function(1, "missing").unwrap().is_none());

        let mut mismatched = wasm_function("mismatched", Some(b"\0asmone".to_vec()));
        mismatched.wasm_hash = Some("not-the-module-hash".into());
        let mismatch_result = catch_unwind(AssertUnwindSafe(|| put(&mismatched, &catalog)));
        assert!(mismatch_result.is_err());
        assert!(catalog.get_function(1, "mismatched").unwrap().is_none());
    }

    #[test]
    fn deleting_one_shared_hash_keeps_the_blob() {
        let catalog = catalog();
        let bytes = b"\0asmshared".to_vec();
        let first = wasm_function("first", Some(bytes.clone()));
        let hash = first.wasm_hash.clone().unwrap();
        put(&first, &catalog);
        let mut second = wasm_function("second", None);
        second.wasm_hash = Some(hash.clone());
        put(&second, &catalog);

        delete(nodedb_types::DatabaseId::DEFAULT, 1, "first", &catalog);
        assert_eq!(load_wasm_binary(&catalog, &hash).unwrap(), bytes);

        delete(nodedb_types::DatabaseId::DEFAULT, 1, "second", &catalog);
        assert!(load_wasm_binary(&catalog, &hash).is_err());
    }

    #[test]
    fn failed_atomic_replacement_rolls_back_metadata_and_blobs() {
        let catalog = catalog();
        let first = wasm_function("replace", Some(b"\0asmold".to_vec()));
        let old_hash = first.wasm_hash.clone().unwrap();
        put(&first, &catalog);

        let replacement = wasm_function("replace", Some(b"\0asmnew".to_vec()));
        let new_hash = replacement.wasm_hash.clone().unwrap();
        catalog.fail_next_function_wasm_write_for_test();
        let result = catch_unwind(AssertUnwindSafe(|| put(&replacement, &catalog)));

        assert!(result.is_err());
        assert_eq!(
            catalog
                .get_function(1, "replace")
                .unwrap()
                .unwrap()
                .wasm_hash,
            Some(old_hash.clone())
        );
        assert_eq!(load_wasm_binary(&catalog, &old_hash).unwrap(), b"\0asmold");
        assert!(load_wasm_binary(&catalog, &new_hash).is_err());
    }

    #[test]
    fn replacement_removes_old_unreferenced_blob() {
        let catalog = catalog();
        let first = wasm_function("replace", Some(b"\0asmold".to_vec()));
        let old_hash = first.wasm_hash.clone().unwrap();
        put(&first, &catalog);
        let replacement = wasm_function("replace", Some(b"\0asmnew".to_vec()));
        let new_hash = replacement.wasm_hash.clone().unwrap();
        put(&replacement, &catalog);

        assert!(load_wasm_binary(&catalog, &old_hash).is_err());
        assert_eq!(
            load_wasm_binary(&catalog, &new_hash).unwrap(),
            b"\0asmnew".to_vec()
        );
    }
}
