// SPDX-License-Identifier: BUSL-1.1

//! Turning an incoming document body into the bytes storage will hold.
//!
//! Its own concern because two callers need the same answer from the same
//! inputs: [`CoreLoop::apply_point_put`](super::core), which writes them, and
//! the governed-write RESOLVE pass, which reports what a write WOULD store and
//! projects `RETURNING` from it. Deriving the stored image twice is how the two
//! start disagreeing about which row the policy admitted.
//!
//! Pure value computation: no store, no index, no transaction.

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::generated;
use crate::data::executor::{doc_format, strict_format};
use crate::types::{DatabaseId, TenantId};

/// Inputs to [`CoreLoop::build_stored_body`].
pub(in crate::data::executor) struct StoredBodyInput<'a> {
    pub config_key: &'a (DatabaseId, TenantId, String),
    pub surrogate: Surrogate,
    /// The incoming MessagePack body, for both storage modes.
    pub value: &'a [u8],
    pub bitemporal: bool,
    pub sys_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

/// The two forms of one row this write lands.
pub(in crate::data::executor) struct StoredBody {
    /// Canonical MessagePack body — generated columns evaluated and `_rowid`
    /// injected. Downstream indexing reads THIS, so it sees the injected
    /// `_rowid` alongside the user's fields.
    pub value: Vec<u8>,
    /// The exact bytes handed to storage: a Binary Tuple on a strict
    /// collection, the canonical MessagePack otherwise.
    pub stored: Vec<u8>,
}

impl CoreLoop {
    /// Build the canonical body and the stored image for one incoming write.
    ///
    /// # `value` is an incoming body, so failing to read it as a document is an answer
    ///
    /// The generated-column step guards on a successful decode rather than
    /// propagating: a body with no readable fields has no column for a
    /// generated expression to read or write, so it is stored as supplied. The
    /// strict encode below still rejects a body it cannot read.
    pub(in crate::data::executor) fn build_stored_body(
        &self,
        input: StoredBodyInput<'_>,
    ) -> crate::Result<StoredBody> {
        let StoredBodyInput {
            config_key,
            surrogate,
            value,
            bitemporal,
            sys_from_ms,
            valid_from_ms,
            valid_until_ms,
        } = input;

        // Evaluate generated columns before encoding.
        let value = if let Some(config) = self.doc_configs.get(config_key)
            && !config.enforcement.generated_columns.is_empty()
        {
            if let Ok(mut doc) = doc_format::decode_document(value) {
                if let Err(e) = generated::evaluate_generated_columns(
                    &mut doc,
                    &config.enforcement.generated_columns,
                ) {
                    return Err(crate::Error::Storage {
                        engine: "generated".into(),
                        detail: format!("generated column evaluation failed: {e:?}"),
                    });
                }
                doc_format::encode_to_msgpack(&doc)
            } else {
                value.to_vec()
            }
        } else {
            doc_format::canonicalize_document_for_storage(value)
        };

        // Strict (Binary Tuple) encoding pipeline. Runs in two steps under a
        // single doc-config lookup:
        //   (1) When the schema has an auto-generated `_rowid` primary key
        //       (injected by `build_strict_schema` when no explicit PK is
        //       declared), the client INSERT payload won't contain it. Inject it
        //       from the surrogate before encoding so the NOT NULL constraint is
        //       satisfied.
        //   (2) Encode the (possibly-injected) MessagePack into Binary Tuple.
        let Some(config) = self.doc_configs.get(config_key) else {
            return Ok(StoredBody {
                stored: value.clone(),
                value,
            });
        };
        let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
            config.storage_mode
        else {
            return Ok(StoredBody {
                stored: value.clone(),
                value,
            });
        };

        let value_with_rowid: Option<Vec<u8>> = if schema
            .columns
            .first()
            .is_some_and(|c| c.name == "_rowid" && !c.nullable)
            && let Ok(mut decoded) = nodedb_types::json_from_msgpack(&value)
            && let serde_json::Value::Object(ref mut obj) = decoded
            && !obj.contains_key("_rowid")
        {
            obj.insert(
                "_rowid".to_string(),
                serde_json::Value::Number((surrogate.0 as i64).into()),
            );
            Some(nodedb_types::json_to_msgpack(&decoded).unwrap_or_else(|_| value.clone()))
        } else {
            None
        };
        let value = value_with_rowid.unwrap_or(value);

        let stored = if bitemporal && schema.bitemporal {
            strict_format::bytes_to_binary_tuple_bitemporal(
                &value,
                schema,
                sys_from_ms,
                valid_from_ms,
                valid_until_ms,
            )
        } else {
            strict_format::bytes_to_binary_tuple(&value, schema)
        }
        .map_err(|e| crate::Error::Serialization {
            format: "binary_tuple".into(),
            detail: e.to_string(),
        })?;

        Ok(StoredBody { value, stored })
    }
}
