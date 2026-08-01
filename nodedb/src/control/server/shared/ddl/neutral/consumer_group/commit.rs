// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `COMMIT OFFSET` DDL handler.
//!
//! Ported from the pgwire `ddl::consumer_group::commit` handler. The two-form
//! token parsing, the group-existence checks, the per-partition tail-tracker
//! batch-commit path (NOT a full buffer scan — preserved
//! verbatim), and the `OffsetRegression` error mapping are preserved verbatim;
//! only the result construction changed from pgwire `Response` / `PgWireError`
//! to the protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Syntax:
//! - `COMMIT OFFSET PARTITION <p> AT <lsn>:<sequence> ON <stream> CONSUMER GROUP <name>`
//!   (a bare `<lsn>` is legacy compatibility and acknowledges the whole LSN)
//! - `COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>` (batch: commit all at latest)

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::status;
use super::identity::canonical_stream_name;

fn authorize_offset_commit(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    stream_name: &str,
) -> Result<(), DdlError> {
    let resource = if let Some(topic_name) = stream_name.strip_prefix("topic:") {
        format!("topic:{topic_name}")
    } else if let Some(stream_def) =
        state
            .stream_registry
            .get(database_id, identity.tenant_id.as_u64(), stream_name)
    {
        stream_def.collection
    } else {
        return Ok(());
    };
    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        &resource,
        Permission::Read,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)
    .map_err(|error| DdlError {
        sqlstate: "42501".to_string(),
        message: error.to_string(),
    })
}

fn migrate_legacy_group(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    stream_name: &str,
    group_name: &str,
) -> Result<(), DdlError> {
    super::identity::migrate_legacy_topic_group(
        state,
        database_id,
        tenant_id,
        stream_name,
        group_name,
    )
    .map(|_| ())
    .map_err(|error| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("consumer-group migration: {error}"),
    })
}

/// Handle `COMMIT OFFSET PARTITION <p> AT <lsn>:<sequence> ON <stream> CONSUMER GROUP <name>`.
pub async fn commit_offset(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    // Single partition: COMMIT OFFSET PARTITION <p> AT <lsn>:<sequence> ON <stream> CONSUMER GROUP <name>
    // parts: [COMMIT, OFFSET, PARTITION, <p>, AT, <offset>, ON, <stream>, CONSUMER, GROUP, <name>]
    // indices:  0       1       2         3   4    5     6     7        8         9      10
    if parts.len() >= 11
        && parts[2].eq_ignore_ascii_case("PARTITION")
        && parts[4].eq_ignore_ascii_case("AT")
        && parts[6].eq_ignore_ascii_case("ON")
        && parts[8].eq_ignore_ascii_case("CONSUMER")
        && parts[9].eq_ignore_ascii_case("GROUP")
    {
        let partition_id: u32 = parts[3].parse().map_err(|_| DdlError {
            sqlstate: "42601".to_string(),
            message: format!("invalid partition: '{}'", parts[3]),
        })?;
        let offset: CdcOffset =
            parts[5]
                .parse()
                .map_err(
                    |error: crate::event::cdc::offset::ParseCdcOffsetError| DdlError {
                        sqlstate: "42601".to_string(),
                        message: error.to_string(),
                    },
                )?;
        let requested_stream = parts[7];
        let mut stream_name =
            canonical_stream_name(state, database_id, tenant_id, requested_stream);
        let topic_lock = stream_name.strip_prefix("topic:").map(|topic| {
            state
                .ep_topic_registry
                .lifecycle_lock(database_id, tenant_id, topic)
        });
        let _topic_guard = match topic_lock {
            Some(lock) => Some(lock.lock_owned().await),
            None => None,
        };
        stream_name = canonical_stream_name(state, database_id, tenant_id, requested_stream);
        let group_name = parts[10].to_lowercase();
        authorize_offset_commit(state, identity, database_id, &stream_name)?;
        let lifecycle_lock =
            state
                .group_registry
                .lifecycle_lock(database_id, tenant_id, &stream_name, &group_name);
        let _group_guard = lifecycle_lock.lock().await;
        let legacy_group_lock = stream_name.strip_prefix("topic:").map(|legacy_stream| {
            state
                .group_registry
                .lifecycle_lock(database_id, tenant_id, legacy_stream, &group_name)
        });
        let _legacy_group_guard = match legacy_group_lock {
            Some(lock) => Some(lock.lock_owned().await),
            None => None,
        };
        migrate_legacy_group(state, database_id, tenant_id, &stream_name, &group_name)?;

        // Verify group exists.
        if state
            .group_registry
            .get(database_id, tenant_id, &stream_name, &group_name)
            .is_none()
        {
            return Err(DdlError {
                sqlstate: "42704".to_string(),
                message: format!(
                    "consumer group '{group_name}' does not exist on stream '{stream_name}'"
                ),
            });
        }

        state
            .offset_store
            .commit_offset(
                database_id,
                tenant_id,
                &stream_name,
                &group_name,
                partition_id,
                offset,
            )
            .map_err(|e| match e {
                crate::Error::OffsetRegression { .. } => DdlError {
                    sqlstate: "22023".to_string(),
                    message: e.to_string(),
                },
                _ => DdlError {
                    sqlstate: "XX000".to_string(),
                    message: format!("offset commit: {e}"),
                },
            })?;

        return Ok(status("COMMIT OFFSET"));
    }

    // Batch: COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>
    // parts: [COMMIT, OFFSETS, ON, <stream>, CONSUMER, GROUP, <name>]
    // indices:  0       1      2     3        4         5      6
    if parts.len() >= 7
        && parts[1].eq_ignore_ascii_case("OFFSETS")
        && parts[2].eq_ignore_ascii_case("ON")
        && parts[4].eq_ignore_ascii_case("CONSUMER")
        && parts[5].eq_ignore_ascii_case("GROUP")
    {
        let requested_stream = parts[3];
        let mut stream_name =
            canonical_stream_name(state, database_id, tenant_id, requested_stream);
        let topic_lock = stream_name.strip_prefix("topic:").map(|topic| {
            state
                .ep_topic_registry
                .lifecycle_lock(database_id, tenant_id, topic)
        });
        let _topic_guard = match topic_lock {
            Some(lock) => Some(lock.lock_owned().await),
            None => None,
        };
        stream_name = canonical_stream_name(state, database_id, tenant_id, requested_stream);
        let group_name = parts[6].to_lowercase();
        authorize_offset_commit(state, identity, database_id, &stream_name)?;
        let lifecycle_lock =
            state
                .group_registry
                .lifecycle_lock(database_id, tenant_id, &stream_name, &group_name);
        let _group_guard = lifecycle_lock.lock().await;
        let legacy_group_lock = stream_name.strip_prefix("topic:").map(|legacy_stream| {
            state
                .group_registry
                .lifecycle_lock(database_id, tenant_id, legacy_stream, &group_name)
        });
        let _legacy_group_guard = match legacy_group_lock {
            Some(lock) => Some(lock.lock_owned().await),
            None => None,
        };
        migrate_legacy_group(state, database_id, tenant_id, &stream_name, &group_name)?;

        if state
            .group_registry
            .get(database_id, tenant_id, &stream_name, &group_name)
            .is_none()
        {
            return Err(DdlError {
                sqlstate: "42704".to_string(),
                message: format!(
                    "consumer group '{group_name}' does not exist on stream '{stream_name}'"
                ),
            });
        }

        // Use the buffer's per-partition tail tracker — NOT a full
        // buffer scan. A scan is O(N) and silently
        // misses partitions whose events have been evicted by retention.
        if let Some(buffer) = state
            .cdc_router
            .get_buffer(database_id, tenant_id, &stream_name)
        {
            for (partition_id, offset) in buffer.partition_tails() {
                // Skip partitions whose committed offset already meets
                // or exceeds the current tail — commit_offset rejects
                // regressions and we want idempotent auto-commit.
                let current = state.offset_store.get_offset(
                    database_id,
                    tenant_id,
                    &stream_name,
                    &group_name,
                    partition_id,
                );
                if offset <= current {
                    continue;
                }
                state
                    .offset_store
                    .commit_offset(
                        database_id,
                        tenant_id,
                        &stream_name,
                        &group_name,
                        partition_id,
                        offset,
                    )
                    .map_err(|e| DdlError {
                        sqlstate: "XX000".to_string(),
                        message: format!("offset commit: {e}"),
                    })?;
            }
        }

        return Ok(status("COMMIT OFFSETS"));
    }

    Err(DdlError {
        sqlstate: "42601".to_string(),
        message:
            "expected COMMIT OFFSET PARTITION <p> AT <lsn>:<sequence> ON <stream> CONSUMER GROUP <name>, \
         or COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>; bare <lsn> is legacy whole-LSN acknowledgement"
                .to_string(),
    })
}
