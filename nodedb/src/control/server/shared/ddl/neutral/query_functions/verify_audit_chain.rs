// SPDX-License-Identifier: BUSL-1.1

//! `SELECT VERIFY_AUDIT_CHAIN(from_seq, to_seq)`
//!
//! Validates the SHA-256 hash chain integrity of audit entries over a sequence range.
//! Reads from the durable audit WAL (or in-memory cache as fallback).
//!
//! **Note:** For complete chain verification, pass `from_seq=1`. Partial ranges
//! validate chain integrity *within* the range but do not verify continuity from genesis.
//!
//! Superuser only, for the same reason `SHOW AUDIT LOG` is: the audit log is a
//! single node-wide record covering every tenant on the node.

use crate::control::security::audit::entry::hash_entry;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::{clean_arg, err, extract_function_args, single_result};

/// `SELECT VERIFY_AUDIT_CHAIN(from_seq, to_seq)`
///
/// Reads audit entries from the durable audit WAL (or in-memory cache),
/// verifies the hash chain over the specified sequence range, and returns
/// `{valid: true/false, entries: N, broken_at_seq: N}`.
///
/// **Superuser only.** `AuditEntry` does carry a `tenant_id`, but the chain
/// itself cannot be narrowed by it: entries from every tenant share one
/// monotonic `seq` and one `last_hash`, so each entry's `prev_hash` links to
/// whichever entry preceded it globally — commonly another tenant's. Dropping
/// the other tenants' entries would break the very links being checked and
/// report a false `valid: false` for any interleaved log. Since the answer is
/// unavoidably node-wide, it is gated the way the other node-wide audit
/// readers are (`SHOW AUDIT LOG`, `SHOW AUDIT WHERE`, `EXPORT AUDIT`): to a
/// superuser, whose remit already spans every tenant.
///
/// `database_id` is accepted for call-site uniformity with the other query
/// functions; the audit chain is not scoped per database either.
pub async fn verify_audit_chain(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err(
            "42501",
            "permission denied: only superuser can verify the audit chain",
        ));
    }

    let args = extract_function_args(sql, "VERIFY_AUDIT_CHAIN")?;

    let from_seq: u64 = if !args.is_empty() {
        clean_arg(args[0])
            .parse()
            .map_err(|_| err("22023", "from_seq must be an integer"))?
    } else {
        1
    };
    let to_seq: u64 = if args.len() >= 2 {
        clean_arg(args[1])
            .parse()
            .map_err(|_| err("22023", "to_seq must be an integer"))?
    } else {
        u64::MAX
    };

    // Try the durable audit WAL first.
    let audit_entries = state
        .wal
        .recover_audit_entries()
        .map_err(|e| err("XX000", &format!("audit WAL recovery failed: {e}")))?;

    let mut valid = true;
    let mut checked = 0u64;
    let mut broken_at: Option<u64> = None;
    let mut prev_hash = String::new();

    if !audit_entries.is_empty() {
        // Verify from durable WAL entries.
        for (_data_lsn, audit_bytes) in &audit_entries {
            let entry: crate::control::security::audit::AuditEntry =
                match zerompk::from_msgpack(audit_bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(error = %e, "skipping malformed audit WAL entry");
                        continue;
                    }
                };

            if entry.seq < from_seq {
                prev_hash = hash_entry(&entry);
                continue;
            }
            if entry.seq > to_seq {
                break;
            }

            if entry.prev_hash != prev_hash {
                valid = false;
                broken_at = Some(entry.seq);
                break;
            }

            prev_hash = hash_entry(&entry);
            checked += 1;
        }
    } else {
        // Fall back to in-memory audit log.
        // Recover from poisoned mutex — audit log corruption doesn't justify panic.
        let log = match state.audit.lock() {
            Ok(l) => l,
            Err(poisoned) => {
                tracing::warn!("audit log mutex poisoned, recovering");
                poisoned.into_inner()
            }
        };
        for entry in log.all() {
            if entry.seq < from_seq {
                prev_hash = hash_entry(entry);
                continue;
            }
            if entry.seq > to_seq {
                break;
            }

            if entry.prev_hash != prev_hash {
                valid = false;
                broken_at = Some(entry.seq);
                break;
            }

            prev_hash = hash_entry(entry);
            checked += 1;
        }
    }

    let result = serde_json::json!({
        "valid": valid,
        "entries_checked": checked,
        "from_seq": from_seq,
        "to_seq": if to_seq == u64::MAX { checked + from_seq - 1 } else { to_seq },
        "broken_at_seq": broken_at,
        "last_hash": prev_hash,
    });

    Ok(single_result(&result.to_string()))
}
