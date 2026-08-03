// SPDX-License-Identifier: BUSL-1.1

//! [`ArrayAckRegistry`] — per-replica ack HLC tracking for array GC.
//!
//! Acknowledgements are persisted by `(database_id, tenant_id, array, replica_id)`.
//! The legacy table is read only as a `(DatabaseId::DEFAULT, 0)` fallback; all
//! new data is stored in the V2 table.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use nodedb_array::sync::ack::AckVector;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::replica_id::ReplicaId;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::warn;

use crate::types::DatabaseId;

/// Legacy redb table: `[name_len: u8][name][replica: u64 BE]` → HLC.
///
/// This table is retained solely to load pre-V2 DEFAULT-database rows.
const ACK_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_ack_hlcs");

/// V2 redb table: `[db: u64 BE][tenant: u64 BE][name_len: u16 BE][name][replica: u64 BE]` → HLC.
const ACK_TABLE_V2: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_ack_hlcs_v2");

type ArrayScope = (DatabaseId, u64, String);

/// Build a V2 key: `[db: u64 BE][tenant: u64 BE][name_len: u16 BE][name][replica: u64 BE]`.
fn ack_key(
    database_id: DatabaseId,
    tenant_id: u64,
    array: &str,
    replica_id: u64,
) -> Option<Vec<u8>> {
    let name_bytes = array.as_bytes();
    let name_len = u16::try_from(name_bytes.len()).ok()?;
    let mut key = Vec::with_capacity(8 + 8 + 2 + name_bytes.len() + 8);
    key.extend_from_slice(&database_id.as_u64().to_be_bytes());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name_bytes);
    key.extend_from_slice(&replica_id.to_be_bytes());
    Some(key)
}

/// Build a legacy key for removal after a DEFAULT-database V2 write.
fn legacy_ack_key(array: &str, replica_id: u64) -> Option<Vec<u8>> {
    let name_bytes = array.as_bytes();
    let name_len = u8::try_from(name_bytes.len()).ok()?;
    let mut key = Vec::with_capacity(1 + name_bytes.len() + 8);
    key.push(name_len);
    key.extend_from_slice(name_bytes);
    key.extend_from_slice(&replica_id.to_be_bytes());
    Some(key)
}

/// Parse a V2 key into its database, tenant, array name, and replica.
fn scope_from_key(key: &[u8]) -> Option<(DatabaseId, u64, String, u64)> {
    if key.len() < 26 {
        return None;
    }
    let database_id = DatabaseId::new(u64::from_be_bytes(key[..8].try_into().ok()?));
    let tenant_id = u64::from_be_bytes(key[8..16].try_into().ok()?);
    let name_len = u16::from_be_bytes(key[16..18].try_into().ok()?) as usize;
    let replica_start = 18 + name_len;
    if key.len() != replica_start + 8 {
        return None;
    }
    let array = std::str::from_utf8(&key[18..replica_start])
        .ok()?
        .to_owned();
    let replica_id = u64::from_be_bytes(key[replica_start..].try_into().ok()?);
    Some((database_id, tenant_id, array, replica_id))
}

/// Parse a legacy key into its array name and replica.
fn legacy_scope_from_key(key: &[u8]) -> Option<(String, u64)> {
    if key.len() < 9 {
        return None;
    }
    let name_len = key[0] as usize;
    let replica_start = 1 + name_len;
    if key.len() != replica_start + 8 {
        return None;
    }
    let array = std::str::from_utf8(&key[1..replica_start]).ok()?.to_owned();
    let replica_id = u64::from_be_bytes(key[replica_start..].try_into().ok()?);
    Some((array, replica_id))
}

/// Registry tracking the latest acknowledged HLC per `(database, array, replica)`.
pub struct ArrayAckRegistry {
    db: Arc<Database>,
    cache: std::sync::RwLock<HashMap<ArrayScope, AckVector>>,
}

impl ArrayAckRegistry {
    /// Open or create the ack registry database at `{data_dir}/array_sync/acks.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Arc<Self>> {
        let dir = data_dir.join("array_sync");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;
        let path = dir.join("acks.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("open acks db {}: {e}", path.display()),
        })?;
        Self::init_db(db)
    }

    /// In-memory-only registry for tests.
    pub fn open_in_memory() -> crate::Result<Arc<Self>> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("in-memory acks db: {e}"),
            })?;
        Self::init_db(db)
    }

    fn init_db(db: Database) -> crate::Result<Arc<Self>> {
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry init begin_write: {e}"),
            })?;
            txn.open_table(ACK_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry init open legacy table: {e}"),
                })?;
            txn.open_table(ACK_TABLE_V2)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry init open v2 table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry init commit: {e}"),
            })?;
        }

        let db = Arc::new(db);
        let cache = Self::load_cache(&db)?;
        Ok(Arc::new(Self {
            db,
            cache: std::sync::RwLock::new(cache),
        }))
    }

    fn load_cache(db: &Database) -> crate::Result<HashMap<ArrayScope, AckVector>> {
        let txn = db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry load begin_read: {e}"),
        })?;
        let v2 = txn
            .open_table(ACK_TABLE_V2)
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry load open v2 table: {e}"),
            })?;
        let legacy = txn
            .open_table(ACK_TABLE)
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry load open legacy table: {e}"),
            })?;
        let mut cache: HashMap<ArrayScope, AckVector> = HashMap::new();

        let v2_rows = v2.iter().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry load v2 iter: {e}"),
        })?;
        for entry in v2_rows {
            let (key, value) = entry.map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry load v2 entry: {e}"),
            })?;
            let Some((database_id, tenant_id, array, replica_raw)) = scope_from_key(key.value())
            else {
                warn!("ack_registry: malformed V2 key, skipping");
                continue;
            };
            let Ok(hlc_bytes) = <[u8; 18]>::try_from(value.value()) else {
                warn!(database = %database_id, array = %array, "ack_registry: V2 ack hlc wrong length, skipping");
                continue;
            };
            cache
                .entry((database_id, tenant_id, array))
                .or_default()
                .record(ReplicaId::new(replica_raw), Hlc::from_bytes(&hlc_bytes));
        }

        let legacy_rows = legacy.iter().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry load legacy iter: {e}"),
        })?;
        for entry in legacy_rows {
            let (key, value) = entry.map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry load legacy entry: {e}"),
            })?;
            let Some((array, replica_raw)) = legacy_scope_from_key(key.value()) else {
                warn!("ack_registry: malformed legacy key, skipping");
                continue;
            };
            let Ok(hlc_bytes) = <[u8; 18]>::try_from(value.value()) else {
                warn!(array = %array, "ack_registry: legacy ack hlc wrong length, skipping");
                continue;
            };
            let replica_id = ReplicaId::new(replica_raw);
            let vector = cache.entry((DatabaseId::DEFAULT, 0, array)).or_default();
            if vector.ack_for(replica_id).is_none() {
                vector.record(replica_id, Hlc::from_bytes(&hlc_bytes));
            }
        }

        Ok(cache)
    }

    /// Record an acknowledgement in an explicit database scope.
    ///
    /// The stored value advances monotonically. Every persisted write uses V2;
    /// a DEFAULT-database write also removes its corresponding legacy row.
    pub fn record_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
        replica_id: ReplicaId,
        ack_hlc: Hlc,
    ) {
        let mut cache = self.cache.write().unwrap_or_else(|p| p.into_inner());
        let vector = cache
            .entry((database_id, tenant_id, array.to_owned()))
            .or_default();
        if vector
            .ack_for(replica_id)
            .is_some_and(|current| current >= ack_hlc)
        {
            return;
        }
        vector.record(replica_id, ack_hlc);
        if let Err(error) = self.persist_row(database_id, tenant_id, array, replica_id, ack_hlc) {
            warn!(database = %database_id, array = %array, error = %error, "ack_registry: persist failed — in-memory ack advanced but disk not updated");
        }
    }

    fn persist_row(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
        replica_id: ReplicaId,
        hlc: Hlc,
    ) -> crate::Result<()> {
        let key = ack_key(database_id, tenant_id, array, replica_id.as_u64()).ok_or_else(|| {
            crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry: array name too long: '{array}'"),
            }
        })?;
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry persist begin_write: {e}"),
        })?;
        {
            let mut v2 = txn
                .open_table(ACK_TABLE_V2)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry persist open v2 table: {e}"),
                })?;
            let hlc_bytes = hlc.to_bytes();
            v2.insert(key.as_slice(), hlc_bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry persist insert: {e}"),
                })?;
        }
        if database_id == DatabaseId::DEFAULT
            && tenant_id == 0
            && let Some(legacy_key) = legacy_ack_key(array, replica_id.as_u64())
        {
            let mut legacy = txn
                .open_table(ACK_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry persist open legacy table: {e}"),
                })?;
            legacy
                .remove(legacy_key.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry persist remove legacy row: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry persist commit: {e}"),
        })
    }

    /// Record an acknowledgement in the DEFAULT database.
    pub fn record(&self, array: &str, replica_id: ReplicaId, ack_hlc: Hlc) {
        self.record_in_database(DatabaseId::DEFAULT, 0, array, replica_id, ack_hlc);
    }

    /// Return the scoped minimum acknowledgement HLC.
    pub fn min_ack_hlc_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> Option<Hlc> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache
            .get(&(database_id, tenant_id, array.to_owned()))?
            .min_ack_hlc()
    }

    /// Return the DEFAULT-database minimum acknowledgement HLC.
    pub fn min_ack_hlc(&self, array: &str) -> Option<Hlc> {
        self.min_ack_hlc_in_database(DatabaseId::DEFAULT, 0, array)
    }

    /// Return the scoped acknowledgement vector, or an empty vector when absent.
    pub fn ack_vector_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> AckVector {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache
            .get(&(database_id, tenant_id, array.to_owned()))
            .cloned()
            .unwrap_or_else(AckVector::new)
    }

    /// Return the DEFAULT-database acknowledgement vector.
    pub fn ack_vector(&self, array: &str) -> AckVector {
        self.ack_vector_in_database(DatabaseId::DEFAULT, 0, array)
    }

    /// Return all array names with acknowledgement state in one database.
    pub fn known_arrays_in_database(&self, database_id: DatabaseId, tenant_id: u64) -> Vec<String> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache
            .keys()
            .filter(|(id, tenant, _)| *id == database_id && *tenant == tenant_id)
            .map(|(_, _, array)| array.clone())
            .collect()
    }

    /// Return all database/array scopes with acknowledgement state.
    pub fn known_array_scopes(&self) -> Vec<(DatabaseId, u64, String)> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache.keys().cloned().collect()
    }

    /// Return all array names with acknowledgement state in the DEFAULT database.
    pub fn known_arrays(&self) -> Vec<String> {
        self.known_arrays_in_database(DatabaseId::DEFAULT, 0)
    }

    /// Return all replica IDs that have acked an array in one database.
    pub fn all_replicas_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> Vec<ReplicaId> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache
            .get(&(database_id, tenant_id, array.to_owned()))
            .map(|vector| vector.replicas().collect())
            .unwrap_or_default()
    }

    /// Return all replica IDs that have acked an array in the DEFAULT database.
    pub fn all_replicas(&self, array: &str) -> Vec<ReplicaId> {
        self.all_replicas_in_database(DatabaseId::DEFAULT, 0, array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).expect("valid test HLC")
    }

    fn r(n: u64) -> ReplicaId {
        ReplicaId::new(n)
    }

    fn registry() -> Arc<ArrayAckRegistry> {
        ArrayAckRegistry::open_in_memory().expect("in-memory registry")
    }

    #[test]
    fn default_wrappers_use_default_database() {
        let reg = registry();
        reg.record("arr", r(1), hlc(100));
        assert_eq!(reg.min_ack_hlc("arr"), Some(hlc(100)));
        assert_eq!(
            reg.min_ack_hlc_in_database(DatabaseId::DEFAULT, 0, "arr"),
            Some(hlc(100))
        );
        assert_eq!(reg.all_replicas("arr"), vec![r(1)]);
    }

    #[test]
    fn same_name_is_isolated_between_databases() {
        let reg = registry();
        let database_id = DatabaseId::new(1024);
        reg.record_in_database(database_id, 1, "arr", r(1), hlc(100));
        reg.record_in_database(database_id, 2, "arr", r(2), hlc(50));

        assert_eq!(
            reg.min_ack_hlc_in_database(database_id, 1, "arr"),
            Some(hlc(100))
        );
        assert_eq!(
            reg.min_ack_hlc_in_database(database_id, 2, "arr"),
            Some(hlc(50))
        );
        assert!(reg.min_ack_hlc_in_database(database_id, 3, "arr").is_none());
        assert_eq!(reg.known_arrays_in_database(database_id, 1), vec!["arr"]);
    }

    #[test]
    fn persistence_survives_reopen() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let database_id = DatabaseId::new(1024);
        {
            let reg = ArrayAckRegistry::open(dir.path()).expect("open registry");
            reg.record_in_database(database_id, 7, "arr", r(7), hlc(123));
        }
        let reloaded = ArrayAckRegistry::open(dir.path()).expect("reopen registry");
        assert_eq!(
            reloaded.min_ack_hlc_in_database(database_id, 7, "arr"),
            Some(hlc(123))
        );
    }

    #[test]
    fn v2_precedes_legacy_and_default_write_migrates_legacy_row() {
        let reg = registry();
        let legacy_key = legacy_ack_key("arr", r(1).as_u64()).expect("legacy key");
        let v2_key = ack_key(DatabaseId::DEFAULT, 0, "arr", r(1).as_u64()).expect("v2 key");
        let txn = reg.db.begin_write().expect("write transaction");
        {
            let mut legacy = txn.open_table(ACK_TABLE).expect("legacy table");
            legacy
                .insert(legacy_key.as_slice(), hlc(50).to_bytes().as_slice())
                .expect("legacy row");
            let mut v2 = txn.open_table(ACK_TABLE_V2).expect("v2 table");
            v2.insert(v2_key.as_slice(), hlc(100).to_bytes().as_slice())
                .expect("v2 row");
        }
        txn.commit().expect("commit test rows");

        let cache = ArrayAckRegistry::load_cache(&reg.db).expect("reload cache");
        assert_eq!(
            cache
                .get(&(DatabaseId::DEFAULT, 0, "arr".to_owned()))
                .and_then(|v| v.ack_for(r(1))),
            Some(hlc(100))
        );

        reg.record("arr", r(1), hlc(150));
        let txn = reg.db.begin_read().expect("read transaction");
        let legacy = txn.open_table(ACK_TABLE).expect("legacy table");
        assert!(
            legacy
                .get(legacy_key.as_slice())
                .expect("legacy get")
                .is_none()
        );
        let v2 = txn.open_table(ACK_TABLE_V2).expect("v2 table");
        let persisted = v2.get(v2_key.as_slice()).expect("v2 get").expect("v2 row");
        assert_eq!(
            Hlc::from_bytes(&persisted.value().try_into().expect("HLC bytes")),
            hlc(150)
        );
    }
}
