// SPDX-License-Identifier: BUSL-1.1

//! Auth user catalog operations (redb persistence for JIT-provisioned users).

use super::types::{AUTH_USERS, StoredAuthUser, SystemCatalog, catalog_err};
use redb::ReadableDatabase;

impl SystemCatalog {
    /// Insert or update a JIT-provisioned auth user.
    pub fn put_auth_user(&self, user: &StoredAuthUser) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(user).map_err(|e| catalog_err("serialize auth user", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("auth_users write txn", e))?;
        {
            let mut table = write_txn
                .open_table(AUTH_USERS)
                .map_err(|e| catalog_err("open auth_users", e))?;
            table
                .insert(user.id.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert auth user", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("auth_users commit", e))?;
        Ok(())
    }

    /// Get a single auth user by ID.
    pub fn get_auth_user(&self, id: &str) -> crate::Result<Option<StoredAuthUser>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("auth_users read txn", e))?;
        let table = read_txn
            .open_table(AUTH_USERS)
            .map_err(|e| catalog_err("open auth_users", e))?;
        match table.get(id).map_err(|e| catalog_err("get auth user", e))? {
            Some(val) => {
                let user: StoredAuthUser = zerompk::from_msgpack(val.value())
                    .map_err(|e| catalog_err("deserialize auth user", e))?;
                Ok(Some(user))
            }
            None => Ok(None),
        }
    }

    /// Delete a JIT-provisioned auth user by ID.
    pub fn delete_auth_user(&self, id: &str) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("auth_users write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(AUTH_USERS)
                .map_err(|e| catalog_err("open auth_users", e))?;
            table
                .remove(id)
                .map_err(|e| catalog_err("remove auth user", e))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| catalog_err("auth_users commit", e))?;
        Ok(removed)
    }

    /// Load all JIT-provisioned auth users.
    pub fn load_all_auth_users(&self) -> crate::Result<Vec<StoredAuthUser>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("auth_users read txn", e))?;
        let table = read_txn
            .open_table(AUTH_USERS)
            .map_err(|e| catalog_err("open auth_users", e))?;
        let mut users = Vec::new();
        let range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range auth_users", e))?;
        for item in range {
            let (_, value) = item.map_err(|e| catalog_err("read auth user", e))?;
            if let Ok(user) = zerompk::from_msgpack::<StoredAuthUser>(value.value()) {
                users.push(user);
            }
        }
        Ok(users)
    }

    /// Delete auth users inactive for longer than the given duration.
    /// Returns the number of purged records.
    pub fn purge_inactive_auth_users(&self, inactive_before_secs: u64) -> crate::Result<usize> {
        let all = self.load_all_auth_users()?;
        let mut purged = 0;

        for user in &all {
            if !user.is_active && user.last_seen < inactive_before_secs {
                self.delete_auth_user(&user.id)?;
                purged += 1;
            }
        }

        Ok(purged)
    }
}
