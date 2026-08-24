// SPDX-License-Identifier: BUSL-1.1

//! MessagePack encoding for persisted deferred-action records.
//!
//! Kept apart from the record definition so the durable shape has one place to
//! change, and apart from the store so the encoding is testable without redb.

use super::record::{ActionKey, FailedAction};

/// Encode an action for storage.
pub fn encode_action(action: &FailedAction) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(action).map_err(|e| serialization_error(format!("encode action: {e}")))
}

/// Decode a stored action.
pub fn decode_action(bytes: &[u8]) -> crate::Result<FailedAction> {
    zerompk::from_msgpack::<FailedAction>(bytes)
        .map_err(|e| serialization_error(format!("decode action: {e}")))
}

/// Encode an action key for use as a storage key.
pub fn encode_key(key: &ActionKey) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(key).map_err(|e| serialization_error(format!("encode action key: {e}")))
}

fn serialization_error(detail: String) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::record::{ActionContext, ActionId, ActionPayload};
    use super::*;
    use nodedb_types::{DatabaseId, Value};

    fn sample() -> FailedAction {
        FailedAction {
            key: ActionKey {
                source_lsn: 42,
                source_sequence: 7,
                source_vshard: 3,
                action: ActionId::TriggerRow {
                    trigger_name: "audit_orders".into(),
                },
            },
            payload: ActionPayload::TriggerRow {
                operation: "INSERT".into(),
                new_fields: Some(HashMap::from([(
                    "id".to_owned(),
                    Value::String("order-1".into()),
                )])),
                old_fields: None,
            },
            context: ActionContext {
                database_id: DatabaseId::DEFAULT,
                tenant_id: 9,
                collection: "orders".into(),
                row_id: "order-1".into(),
                cascade_depth: 2,
            },
            attempts: 3,
            last_error: "shard unavailable".into(),
        }
    }

    #[test]
    fn action_round_trips() {
        let action = sample();
        let decoded = decode_action(&encode_action(&action).expect("encode")).expect("decode");
        assert_eq!(decoded.key, action.key);
        assert_eq!(decoded.attempts, 3);
        assert_eq!(decoded.context.cascade_depth, 2);
        assert_eq!(decoded.last_error, "shard unavailable");
    }

    #[test]
    fn equal_keys_encode_identically() {
        let a = encode_key(&sample().key).expect("encode");
        let b = encode_key(&sample().key).expect("encode");
        assert_eq!(a, b, "a stable key is what makes the store deduplicate");
    }

    #[test]
    fn event_action_payload_round_trips() {
        let mut action = sample();
        action.key.action = ActionId::EventAction {
            event_name: "on_order".into(),
            index: 1,
        };
        action.payload = ActionPayload::EventAction {
            sql: "INSERT INTO audit VALUES ('x')".into(),
        };
        let decoded = decode_action(&encode_action(&action).expect("encode")).expect("decode");
        assert_eq!(decoded.owner(), "on_order");
        match &decoded.payload {
            ActionPayload::EventAction { sql } => {
                assert_eq!(sql, "INSERT INTO audit VALUES ('x')");
            }
            other => panic!("expected EventAction, got {other:?}"),
        }
    }
}
