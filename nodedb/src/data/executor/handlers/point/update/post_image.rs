// SPDX-License-Identifier: BUSL-1.1

//! Producing the post-update row image from the current one.
//!
//! Its own concern because this is pure value computation: it reads the stored
//! body, applies the assignments, recomputes generated columns, and re-encodes
//! in the collection's storage mode — touching no store, no index, and no
//! transaction. That is what lets the two encodings of the same update live
//! side by side and stay comparable: the binary field merge that never
//! materializes a document, and the decode → mutate → re-encode path that
//! every strict, generated-column, or expression assignment forces. Nothing
//! here is allowed to have a side effect, so a failure means the statement
//! aborts before anything has been written.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::strict_format;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::UpdateValue;

/// Inputs to [`CoreLoop::build_point_update_image`].
pub(in crate::data::executor) struct PointUpdateImage<'a> {
    pub(in crate::data::executor) config_key: &'a (DatabaseId, TenantId, String),
    /// The row as currently stored (Binary Tuple for a strict collection,
    /// MessagePack/JSON for a schemaless one).
    pub(in crate::data::executor) current_bytes: &'a [u8],
    pub(in crate::data::executor) updates: &'a [(String, UpdateValue)],
    pub(in crate::data::executor) is_strict: bool,
    pub(in crate::data::executor) has_generated: bool,
    pub(in crate::data::executor) has_expr: bool,
    pub(in crate::data::executor) bitemporal: bool,
    /// System time stamped into a bitemporal strict tuple; `0` otherwise.
    pub(in crate::data::executor) sys_from_ms: i64,
}

impl CoreLoop {
    /// Build the bytes this update will store, in the collection's storage mode.
    pub(in crate::data::executor) fn build_point_update_image(
        &self,
        params: PointUpdateImage<'_>,
    ) -> Result<Vec<u8>, ErrorCode> {
        let PointUpdateImage {
            config_key,
            current_bytes,
            updates,
            is_strict,
            has_generated,
            has_expr,
            bitemporal,
            sys_from_ms,
        } = params;

        // Fast path: non-strict, no generated columns, all literal — merge at binary level.
        if !is_strict && !has_generated && !has_expr {
            let base_mp = doc_format::json_to_msgpack(current_bytes);
            let update_pairs: Vec<(&str, &[u8])> = updates
                .iter()
                .filter_map(|(field, v)| match v {
                    UpdateValue::Literal(bytes) => Some((field.as_str(), bytes.as_slice())),
                    UpdateValue::Expr(_) => None,
                })
                .collect();
            return Ok(nodedb_query::msgpack_scan::merge_fields(
                &base_mp,
                &update_pairs,
            ));
        }

        // Strict, generated, or expression RHS: decode → mutate → re-encode.
        let mut doc = if is_strict {
            if let Some(config) = self.doc_configs.get(config_key)
                && let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
            {
                match strict_format::binary_tuple_to_json(current_bytes, schema) {
                    Some(v) => v,
                    None => {
                        return Err(ErrorCode::Internal {
                            detail: "failed to decode Binary Tuple for update".into(),
                        });
                    }
                }
            } else {
                return Err(ErrorCode::Internal {
                    detail: "strict config missing during update".into(),
                });
            }
        } else {
            match doc_format::decode_document(current_bytes) {
                Ok(v) => v,
                Err(e) => {
                    return Err(ErrorCode::Internal {
                        detail: format!("failed to parse document for update: {e}"),
                    });
                }
            }
        };

        // Apply field-level updates. Expressions are evaluated
        // against the current-row snapshot, so a later assignment
        // observing a column updated earlier in the same statement
        // still sees the pre-update value — matches PostgreSQL.
        let eval_doc: nodedb_types::Value = doc.clone().into();
        if let Some(obj) = doc.as_object_mut() {
            for (field, update_val) in updates {
                let val = match update_val {
                    UpdateValue::Literal(bytes) => match nodedb_types::json_from_msgpack(bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(ErrorCode::Internal {
                                detail: format!("update field '{field}': msgpack decode: {e}"),
                            });
                        }
                    },
                    UpdateValue::Expr(expr) => {
                        let result: nodedb_types::Value = match expr.eval(&eval_doc) {
                            Ok(v) => v,
                            // Division/modulo by zero fails the
                            // statement, same as the
                            // literal-decode-failure arm above.
                            Err(_e) => return Err(ErrorCode::DivisionByZero),
                        };
                        // Convert nodedb_types::Value → serde_json::Value so the
                        // downstream re-encode path (strict or msgpack) can proceed
                        // through its existing json-based branches unchanged.
                        let json: serde_json::Value = result.into();
                        json
                    }
                };
                obj.insert(field.clone(), val);
            }
        }

        // Recompute generated columns.
        if has_generated
            && let Some(config) = self.doc_configs.get(config_key)
            && let Err(e) = crate::data::executor::handlers::generated::evaluate_generated_columns(
                &mut doc,
                &config.enforcement.generated_columns,
            )
        {
            return Err(e);
        }

        // Re-encode.
        if is_strict {
            if let Some(config) = self.doc_configs.get(config_key)
                && let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
            {
                let ndb_val: nodedb_types::Value = doc.clone().into();
                let result = if bitemporal && schema.bitemporal {
                    strict_format::value_to_binary_tuple_bitemporal(
                        &ndb_val,
                        schema,
                        sys_from_ms,
                        i64::MIN,
                        i64::MAX,
                    )
                } else {
                    strict_format::value_to_binary_tuple(&ndb_val, schema)
                };
                match result {
                    Ok(bytes) => Ok(bytes),
                    Err(e) => Err(ErrorCode::Internal {
                        detail: format!("strict re-encode: {e}"),
                    }),
                }
            } else {
                Err(ErrorCode::Internal {
                    detail: "strict config missing during re-encode".into(),
                })
            }
        } else {
            Ok(doc_format::encode_to_msgpack(&doc))
        }
    }
}
