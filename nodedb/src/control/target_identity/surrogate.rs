// SPDX-License-Identifier: BUSL-1.1

//! Assign a fresh, catalog-registered surrogate for each row written into a
//! target collection on behalf of another operation.

use nodedb_types::{CollectionKey, Surrogate, TenantId, extract_pk_value};

use super::pk::TargetPk;
use crate::control::state::SharedState;

/// The surrogates of `count` written rows that name no key on the TARGET's
/// primary key. `target` is the target collection's canonical key. Each row
/// takes a fresh surrogate bound on this node under its own identity, all in
/// one catalog transaction. A row keyless on a declared primary key is
/// refused.
async fn keyless_target_surrogates(
    state: &SharedState,
    target: CollectionKey<'_>,
    tenant_id: TenantId,
    target_pk: &TargetPk,
    count: usize,
) -> crate::Result<Vec<Surrogate>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    // No usable key value on a DDL-declared PRIMARY KEY: NOT NULL is implied,
    // so refuse rather than mint a surrogate for a row that plain INSERT
    // already rejects.
    if let TargetPk::Field {
        name,
        declared: true,
    } = target_pk
    {
        return Err(crate::Error::RejectedConstraint {
            collection: target.name().to_string(),
            constraint: "not_null".to_string(),
            detail: format!("primary key '{name}' cannot be NULL or omitted"),
        });
    }
    // An auto-`_rowid` row, or an undeclared `id`-by-convention field with no
    // value: a fresh unique surrogate, never one shared binding for every
    // keyless row. The row's identity is its document storage key, so the
    // allocator binds that form.
    let drawn = state
        .surrogate_assigner
        .assign_fresh_many(target, tenant_id, count)
        .await?;
    Ok(drawn.into_iter().map(|(surrogate, _)| surrogate).collect())
}

/// A fresh, registered surrogate for every written row of one target, in
/// `bodies` order, on the TARGET's primary key. Every row that names its key
/// resolves in one batch at the target's collection home, through the async
/// exchange. The empty string is a key like any other. Keyless rows follow
/// [`keyless_target_surrogates`].
pub(crate) async fn assign_target_surrogates(
    state: &SharedState,
    target: CollectionKey<'_>,
    tenant_id: TenantId,
    target_pk: &TargetPk,
    bodies: &[&[u8]],
) -> crate::Result<Vec<Surrogate>> {
    let keys: Vec<Option<String>> = bodies
        .iter()
        .map(|body| match target_pk {
            TargetPk::Field { name, .. } => extract_pk_value(body, name),
            TargetPk::AutoRowId => None,
        })
        .collect();
    let keyed: Vec<&[u8]> = keys.iter().flatten().map(|key| key.as_bytes()).collect();
    let mut bound = crate::control::server::surrogate_exchange::assign_surrogates_routed(
        state,
        target,
        tenant_id,
        &keyed,
        crate::types::TraceId::ZERO,
    )
    .await?
    .into_iter();
    let keyless_count = keys.iter().filter(|key| key.is_none()).count();
    let mut keyless = keyless_target_surrogates(state, target, tenant_id, target_pk, keyless_count)
        .await?
        .into_iter();
    let mut surrogates = Vec::with_capacity(bodies.len());
    for key in &keys {
        let next = match key {
            Some(_) => bound.next(),
            None => keyless.next(),
        };
        surrogates.push(next.ok_or_else(|| crate::Error::Internal {
            detail: format!("a row of '{}' got no surrogate", target.name()),
        })?);
    }
    Ok(surrogates)
}
