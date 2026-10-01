// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored KV rows.
//!
//! RESTORE re-issues each captured KV row as a `KvOp::Put`. Every replica of
//! the collection's group applies it, and its WAL makes it durable. The put
//! binds a destination surrogate. The captured surrogate belongs to the
//! source database, so RESTORE ignores it.

use nodedb_physical::physical_plan::KvOp;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::target::DatabaseTarget;

/// One restored KV table's rows, in the shape the KV snapshot captures. The
/// carried surrogate is the source database's identity. RESTORE ignores it
/// and binds each row in the destination catalog.
type KvRows = Vec<crate::engine::kv::hash_table::KvSnapshotRow>;

/// The TTL a restored row keeps at `now_ms`: `Some(0)` for no TTL, the time
/// left for a row that has not expired, `None` for a row already expired.
fn remaining_ttl_ms(expire_at_ms: u64, now_ms: u64) -> Option<u64> {
    match expire_at_ms {
        0 => Some(0),
        at if at > now_ms => Some(at - now_ms),
        _ => None,
    }
}

/// Decode and durably re-issue every restored KV table of one database.
/// Each table key is `"{db}:{tid}:{collection}"`, the collection named as the
/// source KV engine stored it. Returns the number of rows re-issued.
pub(in crate::control::backup::restore) async fn reissue_kv_tables(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    tables: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    let tenant = TenantId::new(tenant_id);
    let mut reissued = 0usize;
    for (table_key, bytes) in tables {
        let name = target.resolve_scoped(&table_key, tenant_id)?;
        let rows: KvRows = zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: deserialize KV table '{table_key}': {e}"),
        })?;
        super::durable::log_reissue_step(
            state,
            "kv",
            &name.bare,
            name.key(target.dest).vshard(),
            rows.len(),
        );
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let live: Vec<(Vec<u8>, Vec<u8>, u64)> = rows
            .into_iter()
            .filter_map(|(key, value, expire_at_ms, _source_surrogate)| {
                remaining_ttl_ms(expire_at_ms, now_ms).map(|ttl_ms| (key, value, ttl_ms))
            })
            .collect();
        // Every live row's surrogate in one batch at the table's home.
        let keys: Vec<&[u8]> = live.iter().map(|(key, _, _)| key.as_slice()).collect();
        let surrogates = crate::control::server::surrogate_exchange::assign_surrogates_routed(
            state,
            name.key(target.dest),
            tenant,
            &keys,
            crate::types::TraceId::ZERO,
        )
        .await?;
        for ((key, value, ttl_ms), surrogate) in live.into_iter().zip(surrogates) {
            let plan = PhysicalPlan::Kv(KvOp::Put {
                collection: name.stored.clone(),
                key,
                value,
                ttl_ms,
                surrogate,
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            });
            super::durable::reissue_plan_durably(state, tenant, target, &name.bare, plan).await?;
            reissued += 1;
        }
    }
    Ok(reissued)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expired_row_is_not_restored() {
        assert_eq!(remaining_ttl_ms(0, 500), Some(0));
        assert_eq!(remaining_ttl_ms(900, 500), Some(400));
        assert_eq!(remaining_ttl_ms(400, 500), None);
    }
}
