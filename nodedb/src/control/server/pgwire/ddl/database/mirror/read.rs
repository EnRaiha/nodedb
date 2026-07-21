// SPDX-License-Identifier: BUSL-1.1

//! Read consistency enforcement for mirror databases.
//!
//! A mirror database is a continuously-updated read-only replica of a source
//! database. The three `ReadConsistency` levels behave as follows on a mirror:
//!
//! - `Strong`: Mirrors cannot serve strong-consistency reads because they are
//!   not the Raft leader for the source's log. A strong read on a mirror would
//!   require forwarding to the source cluster, which is a different process.
//!   This function returns `STALE_READ_NOT_LEADER` immediately with the source
//!   cluster endpoint as a hint so the client can redirect.
//!
//! - `BoundedStaleness(d)`: Reads `_system.mirror_lag.last_apply_ms` and
//!   compares to the current wall clock. If `now - last_apply_ms > d`, the
//!   mirror is too stale and the function returns `STALE_READ_NOT_LEADER` with
//!   the actual lag in the error detail. Otherwise the read is served locally.
//!
//! - `Eventual`: Reads are served from the local replica unconditionally.
//!   This is the lowest-latency option and is correct for use cases that
//!   accept CRDT-style monotonic convergence (e.g. mobile/edge workloads).

use nodedb_types::error::sqlstate;
use nodedb_types::{DatabaseId, MirrorOrigin, MirrorStatus};

use crate::control::security::catalog::SystemCatalog;
use crate::types::ReadConsistency;

/// Outcome of a mirror read consistency check.
///
/// Exhaustive matches are required — no `_ =>` arms.
#[derive(Debug)]
pub enum MirrorReadOutcome {
    /// Proceed: serve the read from the local mirror replica.
    ServeLocally,
    /// Reject: the consistency level cannot be satisfied by this mirror.
    /// Contains the SQLSTATE code and human-readable error message.
    Reject {
        sqlstate_code: &'static str,
        message: String,
    },
}

/// Check whether a read with the given `consistency` level can be served
/// from a mirror database identified by `mirror_db_id`.
///
/// `mirror_origin` must be `Some` for any database that reaches this check;
/// the caller is responsible for routing non-mirror databases past this gate.
///
/// `promoted` is true when the mirror has been promoted and the database
/// is now a writable primary — in that case all consistency levels are
/// served locally (no longer a mirror).
/// `now_ms` is the current wall-clock time (ms since epoch), supplied by the
/// caller rather than read internally so the staleness comparison is a pure
/// function of its inputs — this keeps `BoundedStaleness` boundary tests
/// deterministic instead of racing the wall clock between lag injection and
/// the check.
pub fn check_mirror_read_consistency(
    catalog: &SystemCatalog,
    mirror_db_id: DatabaseId,
    mirror_origin: &MirrorOrigin,
    consistency: ReadConsistency,
    now_ms: u64,
) -> MirrorReadOutcome {
    // A promoted mirror is a normal writable database — serve all reads locally.
    if matches!(mirror_origin.status, MirrorStatus::Promoted) {
        return MirrorReadOutcome::ServeLocally;
    }

    match consistency {
        ReadConsistency::Strong => {
            // Strong reads cannot be served by a mirror: the mirror is never
            // the Raft leader for the source's commit log. The client must
            // redirect to the source cluster for linearizable reads.
            MirrorReadOutcome::Reject {
                sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
                message: format!(
                    "database is a mirror and cannot serve strong-consistency reads; \
                     redirect to source cluster '{}'",
                    mirror_origin.source_cluster
                ),
            }
        }

        ReadConsistency::BoundedStaleness(max_lag) => {
            // Read the last-apply timestamp from the mirror_lag catalog table.
            // If unavailable (e.g. still bootstrapping), treat as maximally stale.
            let last_apply_ms = match catalog.get_mirror_lag(mirror_db_id) {
                Ok(Some(lag)) => lag.last_apply_ms,
                Ok(None) => {
                    // No lag record yet: mirror has not applied any entries.
                    return MirrorReadOutcome::Reject {
                        sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
                        message: format!(
                            "mirror database has not yet applied any log entries; \
                             lag is unbounded (bootstrapping). Source cluster: '{}'",
                            mirror_origin.source_cluster
                        ),
                    };
                }
                Err(_) => {
                    return MirrorReadOutcome::Reject {
                        sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
                        message: format!(
                            "mirror lag record unavailable; cannot verify staleness bound. \
                             Source cluster: '{}'",
                            mirror_origin.source_cluster
                        ),
                    };
                }
            };

            let actual_lag_ms = now_ms.saturating_sub(last_apply_ms);
            let max_lag_ms = max_lag.as_millis() as u64;

            if actual_lag_ms > max_lag_ms {
                MirrorReadOutcome::Reject {
                    sqlstate_code: sqlstate::STALE_READ_NOT_LEADER,
                    message: format!(
                        "mirror replication lag {actual_lag_ms} ms exceeds the \
                         requested bound {max_lag_ms} ms; source cluster: '{}'",
                        mirror_origin.source_cluster
                    ),
                }
            } else {
                MirrorReadOutcome::ServeLocally
            }
        }

        ReadConsistency::Eventual => {
            // Eventual reads are served unconditionally from the local replica.
            // The caller accepts CRDT-style monotonic convergence semantics.
            MirrorReadOutcome::ServeLocally
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nodedb_types::{DatabaseId, Lsn, MirrorLagRecord, MirrorMode, MirrorOrigin, MirrorStatus};
    use tempfile::TempDir;

    use super::*;
    use crate::control::security::catalog::SystemCatalog;

    fn open_tmp_catalog(tmp: &TempDir) -> SystemCatalog {
        let path: PathBuf = tmp.path().join("system.redb");
        SystemCatalog::open(&path).expect("open catalog")
    }

    fn sample_origin(status: MirrorStatus) -> MirrorOrigin {
        MirrorOrigin {
            source_cluster: "prod-us".to_string(),
            source_database: DatabaseId::new(0),
            mode: MirrorMode::Async,
            last_applied: Lsn::new(0),
            status,
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64
    }

    #[test]
    fn strong_on_mirror_returns_reject() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1024);
        let origin = sample_origin(MirrorStatus::Following);

        match check_mirror_read_consistency(
            &catalog,
            db_id,
            &origin,
            ReadConsistency::Strong,
            now_ms(),
        ) {
            MirrorReadOutcome::Reject {
                sqlstate_code,
                message,
            } => {
                assert_eq!(sqlstate_code, sqlstate::STALE_READ_NOT_LEADER);
                assert!(
                    message.contains("prod-us"),
                    "error message should mention source cluster: {message}"
                );
            }
            MirrorReadOutcome::ServeLocally => panic!("Strong should be rejected on a mirror"),
        }
    }

    #[test]
    fn eventual_on_mirror_serves_locally() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1025);
        let origin = sample_origin(MirrorStatus::Following);

        match check_mirror_read_consistency(
            &catalog,
            db_id,
            &origin,
            ReadConsistency::Eventual,
            now_ms(),
        ) {
            MirrorReadOutcome::ServeLocally => {}
            MirrorReadOutcome::Reject { message, .. } => {
                panic!("Eventual should serve locally, got reject: {message}")
            }
        }
    }

    #[test]
    fn bounded_staleness_fresh_mirror_serves_locally() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1026);
        let origin = sample_origin(MirrorStatus::Following);

        // Write a lag record with a very recent last_apply_ms.
        let recent_ms = now_ms();
        catalog
            .put_mirror_lag(
                db_id,
                &MirrorLagRecord {
                    last_applied_lsn: Lsn::new(10),
                    last_apply_ms: recent_ms,
                },
            )
            .unwrap();

        let bound = ReadConsistency::BoundedStaleness(Duration::from_secs(10));
        match check_mirror_read_consistency(&catalog, db_id, &origin, bound, now_ms()) {
            MirrorReadOutcome::ServeLocally => {}
            MirrorReadOutcome::Reject { message, .. } => {
                panic!("Fresh mirror should serve locally, got reject: {message}")
            }
        }
    }

    #[test]
    fn bounded_staleness_stale_mirror_returns_reject() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1027);
        let origin = sample_origin(MirrorStatus::Degraded { lag_ms: 60_000 });

        // Write a lag record with a very old last_apply_ms (60 seconds ago).
        let stale_ms = now_ms().saturating_sub(60_000);
        catalog
            .put_mirror_lag(
                db_id,
                &MirrorLagRecord {
                    last_applied_lsn: Lsn::new(1),
                    last_apply_ms: stale_ms,
                },
            )
            .unwrap();

        // Bound = 5 seconds; actual lag ≈ 60 seconds.
        let bound = ReadConsistency::BoundedStaleness(Duration::from_secs(5));
        match check_mirror_read_consistency(&catalog, db_id, &origin, bound, now_ms()) {
            MirrorReadOutcome::Reject {
                sqlstate_code,
                message,
            } => {
                assert_eq!(sqlstate_code, sqlstate::STALE_READ_NOT_LEADER);
                assert!(
                    message.contains("5000") || message.contains("ms"),
                    "error message should contain lag info: {message}"
                );
            }
            MirrorReadOutcome::ServeLocally => panic!("Stale mirror should be rejected"),
        }
    }

    #[test]
    fn promoted_mirror_always_serves_locally() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1028);
        let origin = sample_origin(MirrorStatus::Promoted);

        for consistency in [
            ReadConsistency::Strong,
            ReadConsistency::Eventual,
            ReadConsistency::BoundedStaleness(Duration::from_secs(1)),
        ] {
            match check_mirror_read_consistency(&catalog, db_id, &origin, consistency, now_ms()) {
                MirrorReadOutcome::ServeLocally => {}
                MirrorReadOutcome::Reject { message, .. } => {
                    panic!("Promoted mirror should always serve locally, got: {message}")
                }
            }
        }
    }
}
