// SPDX-License-Identifier: BUSL-1.1

//! The policy-store and identity inputs every injection arm keys on.
//!
//! Each engine module receives an [`RlsCtx`] and resolves one outcome per plan
//! variant. A read either injects the policy into a filter slot, refuses
//! because its result cannot carry a row filter, or no-ops because the op reads
//! no user rows. A write either admits its post-image against the write policy
//! when the plan carries that image, ships the compiled predicate along for the
//! Data Plane to evaluate against the row bytes it is about to persist, or
//! refuses because neither is possible — never a silent no-op, which is
//! indistinguishable from having no policy at all.

use crate::control::security::auth_context::AuthContext;
use crate::control::security::rls::{PolicyType, RlsPolicyStore};

use super::filters::{get_rls, get_rls_write, merge_filters};

/// Policy registry plus the requester's tenant and authenticated identity.
///
/// Superuser bypass and fail-closed handling of unresolved `$auth.*`
/// references both live inside `combined_read_predicate_with_auth`, which
/// every method here reaches through [`get_rls`] — so no arm has to restate
/// either rule.
pub(super) struct RlsCtx<'a> {
    pub(super) store: &'a RlsPolicyStore,
    pub(super) tenant_id: u64,
    pub(super) auth: &'a AuthContext,
}

impl RlsCtx<'_> {
    /// The concrete read filters for `collection`; empty when no policy
    /// restricts this identity.
    pub(super) fn read_filters(&self, collection: &str) -> crate::Result<Vec<u8>> {
        get_rls(self.store, self.tenant_id, collection, self.auth)
    }

    /// AND the collection's read policy into a scan-style pushdown slot.
    pub(super) fn merge_into(&self, collection: &str, filters: &mut Vec<u8>) -> crate::Result<()> {
        let rls = self.read_filters(collection)?;
        if !rls.is_empty() {
            merge_filters(filters, &rls)?;
        }
        Ok(())
    }

    /// Store the collection's read policy in a dedicated post-fetch slot.
    pub(super) fn set_post_filters(
        &self,
        collection: &str,
        rls_filters: &mut Vec<u8>,
    ) -> crate::Result<()> {
        let rls = self.read_filters(collection)?;
        if !rls.is_empty() {
            *rls_filters = rls;
        }
        Ok(())
    }

    /// Refuse the plan while a read policy restricts this identity on
    /// `collection`.
    ///
    /// `why` completes the sentence "…is not supported with this operation:
    /// {why}", so it must state what the result carries instead of rows and
    /// why the filter cannot be evaluated against it.
    pub(super) fn refuse_if_policy(&self, collection: &str, why: &str) -> crate::Result<()> {
        if collection.is_empty() || self.read_filters(collection)?.is_empty() {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "RLS policy on '{collection}' is not supported with this operation: {why}"
            ),
        })
    }

    /// Refuse when this identity holds any read policy anywhere in the tenant.
    ///
    /// Used only where the plan does not name the collection it reads, so the
    /// narrow per-collection question cannot be asked and the plan cannot be
    /// shown to avoid a protected collection. Mirrors the redaction pass's
    /// tenant-wide fallback for an unscoped MATCH.
    pub(super) fn refuse_if_any_policy(&self, why: &str) -> crate::Result<()> {
        if self.auth.is_superuser() || !self.identity_has_any_read_policy() {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "RLS is not supported with this operation while a read policy applies to this \
                 identity and the plan names no collection: {why}"
            ),
        })
    }

    /// Admit a write whose post-image the plan already carries.
    ///
    /// `image` is the MessagePack row body the statement is about to persist,
    /// so the predicate decides the row that will exist after the write —
    /// matching the CRDT admission path, which evaluates the same policies
    /// against the merged post-image.
    ///
    /// A row the policy rejects fails the whole statement rather than being
    /// skipped: a silently dropped row would report a write that never
    /// happened.
    pub(super) fn admit_write_image(&self, collection: &str, image: &[u8]) -> crate::Result<()> {
        let check = get_rls_write(self.store, self.tenant_id, collection, self.auth)?;
        crate::control::security::rls::admit_compiled_write_image(
            &check,
            image,
            self.tenant_id,
            collection,
        )
    }

    /// Admit a write whose post-image the plan carries as a JSON object.
    ///
    /// A graph edge stores the `PROPERTIES` clause as JSON text rather than
    /// MessagePack, so the image is transcoded here and then decided by the
    /// same [`admit_compiled_write_image`] every other engine goes through —
    /// one compiled predicate cannot mean one thing for a document row and
    /// another for an edge's property object.
    ///
    /// Bytes that are not a JSON object — including the empty clause an edge
    /// written without `PROPERTIES` carries — hold no field the predicate can
    /// test, so they are denied rather than admitted by omission. That is the
    /// same fail-closed direction as a row missing the governed column.
    ///
    /// [`admit_compiled_write_image`]: crate::control::security::rls::admit_compiled_write_image
    pub(super) fn admit_write_json_image(
        &self,
        collection: &str,
        image: &[u8],
    ) -> crate::Result<()> {
        let check = get_rls_write(self.store, self.tenant_id, collection, self.auth)?;
        if check.is_empty() {
            return Ok(());
        }
        let decoded = sonic_rs::from_slice::<serde_json::Value>(image).ok();
        let Some(object @ serde_json::Value::Object(_)) = decoded else {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(self.tenant_id),
                resource: format!(
                    "RLS write policy on '{collection}': the write carries no decodable property \
                     object, so the policy could not be evaluated against it"
                ),
            });
        };
        crate::control::security::rls::admit_compiled_write_image(
            &check,
            &nodedb_types::json_to_msgpack_or_empty(&object),
            self.tenant_id,
            collection,
        )
    }

    /// Admit a write whose post-image the plan carries as a zerompk-encoded
    /// `HashMap<String, Value>`.
    ///
    /// [`nodedb_types::Value`]'s zerompk representation is TAGGED: a string
    /// field is written as the two-element array `[4, "…"]`, not as a bare
    /// MessagePack string. Handing those bytes to the row evaluator directly
    /// would compare every predicate against a tag array and reject rows the
    /// policy permits — a gate that denies everything, which is as wrong as one
    /// that admits everything. So the map is decoded and rewritten as the
    /// standard MessagePack body every other engine's image already is, and one
    /// compiled predicate decides them all identically.
    ///
    /// Field names are lower-cased because that is what the collection will
    /// store: a policy on `owner` must not depend on whether the statement
    /// spelled the column `owner` or `Owner`.
    ///
    /// Bytes that do not decode as that map carry no field the predicate can
    /// test, so they deny rather than being admitted by omission.
    pub(super) fn admit_write_value_map_image(
        &self,
        collection: &str,
        payload: &[u8],
    ) -> crate::Result<()> {
        let check = get_rls_write(self.store, self.tenant_id, collection, self.auth)?;
        if check.is_empty() {
            return Ok(());
        }
        let decoded = zerompk::from_msgpack::<std::collections::HashMap<String, nodedb_types::Value>>(
            payload,
        );
        let Ok(fields) = decoded else {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(self.tenant_id),
                resource: format!(
                    "RLS write policy on '{collection}': the write carries no decodable field \
                     image, so the policy could not be evaluated against it"
                ),
            });
        };
        let image = nodedb_types::Value::Object(
            fields
                .into_iter()
                .map(|(field, value)| (field.to_ascii_lowercase(), value))
                .collect(),
        );
        let bytes =
            nodedb_types::value_to_msgpack(&image).map_err(|error| crate::Error::PlanError {
                detail: format!("RLS write admission could not re-encode the row image: {error}"),
            })?;
        crate::control::security::rls::admit_compiled_write_image(
            &check,
            &bytes,
            self.tenant_id,
            collection,
        )
    }

    /// Admit every row of a MessagePack row batch the plan carries in full.
    ///
    /// The columnar family ships a statement's rows as one MessagePack array of
    /// per-row objects rather than as separate op fields, so the batch is split
    /// here and each row decided on its own. The first violation fails the whole
    /// statement before any dispatch, which is what keeps a partially applied
    /// batch impossible: nothing has been written yet when the refusal happens.
    ///
    /// A payload that is neither an array of rows nor a single row object is
    /// refused rather than admitted — an image the policy could not be
    /// evaluated against is not an image the policy admitted.
    pub(super) fn admit_write_batch(&self, collection: &str, payload: &[u8]) -> crate::Result<()> {
        let check = get_rls_write(self.store, self.tenant_id, collection, self.auth)?;
        if check.is_empty() {
            return Ok(());
        }
        let rows = match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows,
            Ok(row @ nodedb_types::Value::Object(_)) => vec![row],
            _ => {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: crate::types::TenantId::new(self.tenant_id),
                    resource: format!(
                        "RLS write policy on '{collection}': the row batch did not decode, so the \
                         policy could not be evaluated against it"
                    ),
                });
            }
        };
        for row in &rows {
            let image =
                nodedb_types::value_to_msgpack(row).map_err(|error| crate::Error::PlanError {
                    detail: format!("RLS write admission could not re-encode a row: {error}"),
                })?;
            crate::control::security::rls::admit_compiled_write_image(
                &check,
                &image,
                self.tenant_id,
                collection,
            )?;
        }
        Ok(())
    }

    /// Compile the collection's write policy into a plan's write-gate slot.
    ///
    /// For a write whose row image is produced where it is persisted: an
    /// update's post-image exists only after the stored row is read and the
    /// statement's changes are applied, and a delete's image only after the row
    /// being removed is read. The predicate therefore travels with the plan and
    /// the Data Plane evaluates it against the actual row bytes before
    /// persisting, rather than the plan being refused outright.
    ///
    /// Resolved through the same [`get_rls_write`] as [`Self::admit_write_image`],
    /// so superuser bypass and the fail-closed deny on an unresolvable
    /// `$auth.*` reference behave identically on both paths. Empty bytes mean
    /// no write policy restricts this identity here.
    pub(super) fn set_write_check(
        &self,
        collection: &str,
        rls_write_check: &mut Vec<u8>,
    ) -> crate::Result<()> {
        *rls_write_check = get_rls_write(self.store, self.tenant_id, collection, self.auth)?;
        Ok(())
    }

    /// Refuse the write while a write policy restricts this identity on
    /// `collection`.
    ///
    /// `why` completes the sentence "…cannot be enforced for this operation:
    /// {why}", so it must state why the row image the policy decides is not
    /// available where the write happens.
    pub(super) fn refuse_if_write_policy(&self, collection: &str, why: &str) -> crate::Result<()> {
        if collection.is_empty()
            || get_rls_write(self.store, self.tenant_id, collection, self.auth)?.is_empty()
        {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "RLS write policy on '{collection}' cannot be enforced for this operation: {why}"
            ),
        })
    }

    /// Refuse while this identity holds any write policy anywhere in the tenant.
    ///
    /// Used only where the write does not name the collection it mutates, so
    /// the narrow per-collection question cannot be asked and the write cannot
    /// be shown to avoid a protected collection.
    pub(super) fn refuse_if_any_write_policy(&self, why: &str) -> crate::Result<()> {
        if self.auth.is_superuser() || !self.store.tenant_has_write_policy(self.tenant_id) {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "an RLS write policy applies to this identity and the plan names no collection, so \
                 it cannot be enforced for this operation: {why}"
            ),
        })
    }

    /// Whether any enabled, non-vacuous read policy exists in this tenant.
    ///
    /// A policy with no compiled predicate filters nothing, so it is ignored
    /// here exactly as `combined_read_predicate_with_auth` ignores it.
    fn identity_has_any_read_policy(&self) -> bool {
        self.store
            .all_policies_for_tenant(self.tenant_id)
            .iter()
            .any(|policy| {
                policy.enabled
                    && policy.compiled_predicate.is_some()
                    && matches!(policy.policy_type, PolicyType::Read | PolicyType::All)
            })
    }
}
