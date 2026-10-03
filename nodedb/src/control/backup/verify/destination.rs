// SPDX-License-Identifier: BUSL-1.1

//! The destination side of restore verification.
//!
//! After the last re-issue, the restore captures every restored collection
//! from the whole cluster, with the capture a backup takes, and recomputes each
//! collection part's tally. A keyed part counts only the destination rows whose
//! key the envelope holds: a row the destination held before the restore, under
//! another key, is not the restore's. A keyless part, timeseries or columnar,
//! counts every row: its re-issue replaces the collection's contents.
//!
//! A KV row whose TTL has passed by the time of the capture counts on neither
//! side: the restore skips it, and the destination can drop it.

use std::collections::{BTreeMap, HashSet};

use nodedb_types::backup_envelope::{VerificationMismatch, VerificationPhase};

use crate::Error;
use crate::control::backup::capture::capture_collections;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantDataSnapshot};
use nodedb_wal::crypto::WalEncryptionKey;

use super::expect::{DatabaseExpectation, Expectation, compare};
use super::walk::{BindIndex, Tallies, Walk};

/// What the destination check verified.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Verified {
    /// Collection parts whose tally matched.
    pub collections: usize,
    /// Rows those parts hold.
    pub rows: u64,
    /// Rows per collection, keyed by (destination database id, collection).
    pub per_collection: BTreeMap<(u64, String), u64>,
}

/// Check that the destination holds every backed-up row. `dest_of` maps a
/// source database id to its destination database.
///
/// A mismatch fails with [`Error::RestoreVerificationFailed`] in `phase`,
/// naming every mismatched collection. Nothing is rolled back.
pub(crate) async fn verify_destination(
    state: &SharedState,
    tenant_id: u64,
    expectation: Expectation,
    dest_of: impl Fn(u64) -> Result<DatabaseId, Error>,
    phase: VerificationPhase,
) -> Result<Verified, Error> {
    let mut verified = Verified::default();
    let mut mismatches = Vec::new();
    for (source, database) in expectation.databases {
        let collections = database.collections();
        if collections.is_empty() {
            continue;
        }
        let dest = dest_of(source)?;
        let arrays = database
            .rows
            .tallies
            .keys()
            .any(|(_, part)| *part == nodedb_types::backup_envelope::VerifiedPart::Array);
        let snap = capture_collections(state, tenant_id, dest, &collections, arrays).await?;
        // Taken after the capture: a row whose TTL passed during it counts on
        // neither side.
        let cutoff_ms = now_ms();
        let scope = Scope {
            tenant_id,
            database_id: dest,
            kek: state.wal.encryption_key(),
        };
        let found = compare_destination(scope, database, &snap, cutoff_ms)?;
        verified.collections += found.verified.collections;
        verified.rows += found.verified.rows;
        for (key, rows) in found.verified.per_collection {
            *verified.per_collection.entry(key).or_default() += rows;
        }
        mismatches.extend(found.mismatches);
    }
    if mismatches.is_empty() {
        Ok(verified)
    } else {
        Err(Error::RestoreVerificationFailed { phase, mismatches })
    }
}

struct Compared {
    verified: Verified,
    mismatches: Vec<VerificationMismatch>,
}

/// The destination database a capture came from.
#[derive(Clone, Copy)]
struct Scope<'a> {
    tenant_id: u64,
    database_id: DatabaseId,
    kek: Option<&'a WalEncryptionKey>,
}

/// Compare `snap`, captured from the destination database of `scope`, with
/// `expected`. The capture carries the destination's own binds.
fn compare_destination(
    scope: Scope<'_>,
    mut expected: DatabaseExpectation,
    snap: &TenantDataSnapshot,
    cutoff_ms: u64,
) -> Result<Compared, Error> {
    let rows = &mut expected.rows;
    for row in &rows.expiring {
        if row.expire_at_ms > cutoff_ms {
            continue;
        }
        if let Some(tally) = rows.tallies.get_mut(&row.part) {
            tally.remove(&row.hash);
        }
        if let Some(keys) = rows.keys.get_mut(&row.part) {
            keys.remove(&row.key);
        }
    }
    rows.tallies.retain(|_, tally| tally.count != 0);
    let rows = &expected.rows;

    let binds = BindIndex::new(
        snap.surrogate_pk
            .iter()
            .filter(|b| b.tenant_id == scope.tenant_id)
            .map(|b| (b.collection.as_str(), b.surrogate, b.pk.as_slice())),
    );
    let walk = Walk {
        tenant_id: scope.tenant_id,
        database_id: scope.database_id,
        shapes: &expected.shapes,
        binds: &binds,
        kek: scope.kek,
    };
    let no_keys = HashSet::new();
    let mut found = Tallies::new();
    walk.snapshot(snap, &mut |row| {
        let part = (row.collection, row.part);
        if !rows.tallies.contains_key(&part) {
            return;
        }
        if let Some(key) = row.key
            && !rows.keys.get(&part).unwrap_or(&no_keys).contains(&key)
        {
            return;
        }
        found.entry(part).or_default().add(&row.hash);
    })?;

    let mismatches = compare(scope.database_id.as_u64(), &rows.tallies, &found);
    let mut per_collection = BTreeMap::new();
    for ((collection, _), tally) in &rows.tallies {
        *per_collection
            .entry((scope.database_id.as_u64(), collection.clone()))
            .or_default() += tally.count;
    }
    let verified = Verified {
        collections: rows.tallies.len().saturating_sub(mismatches.len()),
        rows: rows.tallies.values().map(|t| t.count).sum(),
        per_collection,
    };
    Ok(Compared {
        verified,
        mismatches,
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use nodedb_types::backup_envelope::VerifiedPart;

    use super::*;
    use crate::control::backup::verify::expect::expect_rows;
    use crate::control::backup::verify::walk::Shapes;

    const TENANT: u64 = 7;

    fn kv_snapshot(db: u64, rows: &[(&str, &str, u64)]) -> TenantDataSnapshot {
        let rows: Vec<crate::engine::kv::hash_table::KvSnapshotRow> = rows
            .iter()
            .zip(1u32..)
            .map(|((k, v, at), surrogate)| {
                (k.as_bytes().to_vec(), v.as_bytes().to_vec(), *at, surrogate)
            })
            .collect();
        TenantDataSnapshot {
            kv_tables: vec![(
                format!("{db}:{TENANT}:sessions"),
                zerompk::to_msgpack_vec(&rows).expect("encode"),
            )],
            ..Default::default()
        }
    }

    fn scope() -> Scope<'static> {
        Scope {
            tenant_id: TENANT,
            database_id: DatabaseId::DEFAULT,
            kek: None,
        }
    }

    /// The envelope-side expectation of `snap`.
    fn expect(snap: &TenantDataSnapshot) -> DatabaseExpectation {
        let (shapes, binds) = (Shapes::new(), BindIndex::default());
        let walk = Walk {
            tenant_id: TENANT,
            database_id: DatabaseId::DEFAULT,
            shapes: &shapes,
            binds: &binds,
            kek: None,
        };
        DatabaseExpectation {
            shapes: Shapes::new(),
            rows: expect_rows(&walk, snap).expect("walk"),
        }
    }

    fn check(expected: DatabaseExpectation, found: &TenantDataSnapshot, cutoff: u64) -> Compared {
        compare_destination(scope(), expected, found, cutoff).expect("compare")
    }

    #[test]
    fn a_faithful_destination_verifies() {
        let backup = kv_snapshot(0, &[("a", "1", 0), ("b", "2", 0)]);
        let dest = kv_snapshot(0, &[("b", "2", 0), ("a", "1", 0)]);
        let compared = check(expect(&backup), &dest, 1_000);
        assert!(compared.mismatches.is_empty());
        assert_eq!(
            compared.verified,
            Verified {
                collections: 1,
                rows: 2,
                per_collection: BTreeMap::from([((0, "sessions".to_string()), 2)]),
            }
        );
    }

    #[test]
    fn a_deleted_row_fails_and_names_the_collection() {
        let backup = kv_snapshot(0, &[("a", "1", 0), ("b", "2", 0)]);
        let dest = kv_snapshot(0, &[("a", "1", 0)]);
        let compared = check(expect(&backup), &dest, 1_000);
        assert_eq!(compared.mismatches.len(), 1);
        let mismatch = &compared.mismatches[0];
        assert_eq!(mismatch.collection, "sessions");
        assert_eq!(mismatch.part, VerifiedPart::KeyValue);
        assert_eq!((mismatch.expected.count, mismatch.found.count), (2, 1));
    }

    #[test]
    fn a_changed_value_fails() {
        let backup = kv_snapshot(0, &[("a", "1", 0)]);
        let dest = kv_snapshot(0, &[("a", "2", 0)]);
        assert_eq!(check(expect(&backup), &dest, 1_000).mismatches.len(), 1);
    }

    #[test]
    fn a_row_under_another_key_is_not_the_restores() {
        let backup = kv_snapshot(0, &[("a", "1", 0)]);
        let dest = kv_snapshot(0, &[("a", "1", 0), ("older", "x", 0)]);
        assert!(check(expect(&backup), &dest, 1_000).mismatches.is_empty());
    }

    #[test]
    fn an_expired_row_counts_on_neither_side() {
        let backup = kv_snapshot(0, &[("a", "1", 0), ("gone", "x", 500)]);
        let dest = kv_snapshot(0, &[("a", "1", 0)]);
        assert!(check(expect(&backup), &dest, 1_000).mismatches.is_empty());
        // Before the deadline the row must be there.
        let compared = check(expect(&backup), &dest, 100);
        assert_eq!(compared.mismatches.len(), 1);
    }
}
