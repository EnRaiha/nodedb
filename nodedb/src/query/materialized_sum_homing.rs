// SPDX-License-Identifier: BUSL-1.1

//! Where a materialized-sum TARGET collection lives relative to its SOURCE.
//!
//! A collection homes to one vShard, so a binding's source and target are
//! generally served by DIFFERENT cores — co-residency is the exception, not the
//! rule. Two decisions hang off that fact and they must never disagree:
//!
//! * the **Control Plane** decides, at plan time, whether the balance write can
//!   ride the source write's own task or needs a task of its own homed on the
//!   target's vShard;
//! * the **Data Plane** decides, inside the source write's transaction, whether
//!   to apply the delta itself or leave it to that separate task.
//!
//! If those two answered differently the balance would be applied twice or not
//! at all, so the answer is one pure function of `(database, source, target)`
//! that both planes call. It reads no catalog and touches no storage, which is
//! also what lets the Data Plane use it without holding Control-Plane state.

use crate::types::{DatabaseId, VShardId};

/// Qualify a catalog collection name into the db-scoped name every plan carries.
///
/// Homing hashes the name AS IT APPEARS ON THE PLAN, so a target named only by
/// the catalog has to be qualified the same way before it can be compared with a
/// source that already is.
pub fn db_qualified(database_id: DatabaseId, collection: &str) -> String {
    if database_id == DatabaseId::DEFAULT {
        collection.to_string()
    } else {
        format!("{}/{}", database_id.as_u64(), collection)
    }
}

/// The vShard a materialized-sum target collection homes to.
///
/// `target_collection` is the CATALOG name carried on the binding; it is
/// qualified here so the result matches how the target's own writes route.
pub fn sum_target_vshard(database_id: DatabaseId, target_collection: &str) -> VShardId {
    VShardId::from_collection_in_database(
        database_id,
        &db_qualified(database_id, target_collection),
    )
}

/// Whether the balance write may ride the source write's transaction.
///
/// `source_collection` is the source's name as it appears on the plan — the same
/// string its own task is homed on. `true` means one core owns both rows and the
/// derived write is atomic for free; `false` means the balance needs its own
/// task on the target's vShard, dual-homed with the source through Calvin.
pub fn sum_target_is_co_resident(
    database_id: DatabaseId,
    source_collection: &str,
    target_collection: &str,
) -> bool {
    VShardId::from_collection_in_database(database_id, source_collection)
        == sum_target_vshard(database_id, target_collection)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two planes' questions are the same question: a target that is
    /// co-resident homes to the source's own vShard, by construction.
    #[test]
    fn co_residency_agrees_with_the_target_home() {
        let db = DatabaseId::DEFAULT;
        for i in 0..256 {
            let source = format!("src_{i}");
            let target = format!("dst_{i}");
            let co_resident = sum_target_is_co_resident(db, &source, &target);
            let same_home = VShardId::from_collection_in_database(db, &source)
                == sum_target_vshard(db, &target);
            assert_eq!(co_resident, same_home);
        }
    }

    /// A collection is always co-resident with itself, whichever database it
    /// lives in — the qualification must not make a name differ from itself.
    #[test]
    fn a_collection_is_co_resident_with_itself() {
        for db in [DatabaseId::DEFAULT, DatabaseId::new(7)] {
            let qualified = db_qualified(db, "ledger");
            assert!(sum_target_is_co_resident(db, &qualified, "ledger"));
        }
    }

    /// Cross-shard is the common case, so the helper must actually report it —
    /// a function that always answered "co-resident" would silently restore the
    /// broken behaviour this exists to replace.
    #[test]
    fn distinct_collections_are_usually_cross_shard() {
        let db = DatabaseId::DEFAULT;
        let cross = (0..512)
            .filter(|i| !sum_target_is_co_resident(db, &format!("src_{i}"), &format!("dst_{i}")))
            .count();
        assert!(
            cross > 256,
            "co-residency must be the exception, not the rule: {cross}/512 cross-shard"
        );
    }
}
