// SPDX-License-Identifier: BUSL-1.1

//! Per-session read-your-writes floor tracking on `SessionStore`.
//!
//! Records the highest committed write-version this session has observed for
//! each `(database, tenant, collection)` it has written, so a later
//! transaction's read-set capture can be floored at the session's OWN prior
//! committed writes. This extends the read-your-own-write exclusion already
//! applied to a transaction's buffered writes (see the Calvin static builder)
//! to the session's PRIOR committed autocommit writes: without it, a
//! cross-shard OCC validation would see the session's own committed
//! `coll_write_lsn` exceed a read that captured a stale floor and false-abort
//! with a serialization failure.
//!
//! Soundness: the floor is only ever RAISED by this session's own committed
//! writes to that exact `(database, tenant, collection)`. A concurrent
//! OTHER-session write yields a higher `coll_write_lsn` that still exceeds the
//! floor, so a genuine conflict still aborts — this only removes the self-abort,
//! never a real one.

use super::connection::SessionId;
use crate::types::{DatabaseId, Lsn, TenantId};

use super::store::SessionStore;

impl SessionStore {
    /// Record a committed write-version for `(database, tenant, collection)` on
    /// this session, keeping the maximum seen. `version` is the write's
    /// committed per-collection version (`coll_write_lsn`), sourced from the
    /// replicated-write response. A `Lsn::ZERO` version carries no floor and is
    /// ignored. Persists for the life of the session.
    pub fn note_own_write(
        &self,
        addr: impl Into<SessionId>,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        version: Lsn,
    ) {
        if version == Lsn::ZERO {
            return;
        }
        self.write_session(addr, |session| {
            let slot = session
                .own_write_versions
                .entry((database_id, tenant_id, collection.to_string()))
                .or_insert(Lsn::ZERO);
            if version > *slot {
                *slot = version;
            }
        });
    }

    /// Return the session's own highest committed write-version for
    /// `(database, tenant, collection)`, or `Lsn::ZERO` if the session never
    /// wrote that collection (no floor to apply).
    pub fn own_write_version(
        &self,
        addr: impl Into<SessionId>,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Lsn {
        self.read_session(addr, |session| {
            session
                .own_write_versions
                .get(&(database_id, tenant_id, collection.to_string()))
                .copied()
                .unwrap_or(Lsn::ZERO)
        })
        .unwrap_or(Lsn::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    fn addr() -> SocketAddr {
        "127.0.0.1:5601".parse().expect("test addr")
    }

    fn store_with_session() -> (SessionStore, SocketAddr) {
        let sessions = SessionStore::new();
        let a = addr();
        sessions.ensure_session(a);
        (sessions, a)
    }

    #[test]
    fn absent_collection_returns_zero() {
        let (sessions, a) = store_with_session();
        assert_eq!(
            sessions.own_write_version(a, DatabaseId::DEFAULT, TenantId::new(1), "bread"),
            Lsn::ZERO
        );
    }

    #[test]
    fn records_and_returns_own_write_version() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "bread",
            Lsn::new(2),
        );
        assert_eq!(
            sessions.own_write_version(a, DatabaseId::DEFAULT, TenantId::new(1), "bread"),
            Lsn::new(2)
        );
    }

    #[test]
    fn keeps_the_maximum_version() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "bread",
            Lsn::new(5),
        );
        // A lower version never lowers the floor.
        sessions.note_own_write(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "bread",
            Lsn::new(3),
        );
        assert_eq!(
            sessions.own_write_version(a, DatabaseId::DEFAULT, TenantId::new(1), "bread"),
            Lsn::new(5)
        );
    }

    #[test]
    fn zero_version_is_ignored() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write(a, DatabaseId::DEFAULT, TenantId::new(1), "bread", Lsn::ZERO);
        assert_eq!(
            sessions.own_write_version(a, DatabaseId::DEFAULT, TenantId::new(1), "bread"),
            Lsn::ZERO
        );
    }

    #[test]
    fn scoped_by_database_tenant_and_collection() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "bread",
            Lsn::new(7),
        );
        // A different collection, tenant, or database sees no floor.
        assert_eq!(
            sessions.own_write_version(a, DatabaseId::DEFAULT, TenantId::new(1), "milk"),
            Lsn::ZERO
        );
        assert_eq!(
            sessions.own_write_version(a, DatabaseId::DEFAULT, TenantId::new(2), "bread"),
            Lsn::ZERO
        );
    }
}
