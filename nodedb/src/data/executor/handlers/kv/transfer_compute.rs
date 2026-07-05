// SPDX-License-Identifier: BUSL-1.1

//! Pure value computation for the atomic `Transfer` op (fungible balance
//! move), shared by the autocommit handler (`transfer.rs`) and the
//! in-transaction staging handler (`stage_kv_transfer.rs`) so a staged
//! value and its COMMIT-time durable replay are always computed by the
//! exact same code — mirrors the `engine_atomic_compute` / `stage_kv_atomic`
//! split for `Incr`/`Cas`/etc.

/// Failure modes of [`compute_transfer`], translated to `ErrorCode` at each
/// call site (the live handler and the staging handler render slightly
/// different `ErrorCode` variants around the same detail message).
#[derive(Debug)]
pub(in crate::data::executor) enum TransferError {
    TypeMismatch(String),
    InsufficientBalance { have: f64, need: f64 },
}

/// The two updated document bodies and post-transfer balances for a
/// `Transfer` op, computed from BASE ∪ OVERLAY current values.
pub(in crate::data::executor) struct TransferComputation {
    pub new_source: Vec<u8>,
    pub new_dest: Vec<u8>,
    pub source_balance_after: f64,
    pub dest_balance_after: f64,
}

/// Compute the read-validate-write outcome of an atomic fungible transfer.
///
/// `dest_bytes` is `None` when the destination key does not exist under
/// BASE ∪ OVERLAY -- a fresh document is created holding just `field`.
pub(in crate::data::executor) fn compute_transfer(
    source_bytes: &[u8],
    dest_bytes: Option<&[u8]>,
    field: &str,
    amount: f64,
) -> Result<TransferComputation, TransferError> {
    let source_balance = extract_numeric_field(source_bytes, field).ok_or_else(|| {
        TransferError::TypeMismatch(format!("field '{field}' is not numeric or missing"))
    })?;

    if source_balance < amount {
        return Err(TransferError::InsufficientBalance {
            have: source_balance,
            need: amount,
        });
    }

    let dest_balance = dest_bytes
        .and_then(|b| extract_numeric_field(b, field))
        .unwrap_or(0.0);

    let new_source = update_numeric_field(source_bytes, field, source_balance - amount)
        .map_err(TransferError::TypeMismatch)?;

    let new_dest = match dest_bytes.filter(|b| !b.is_empty()) {
        None => {
            let doc = serde_json::json!({ field: dest_balance + amount });
            nodedb_types::json_to_msgpack(&doc)
                .map_err(|e| TransferError::TypeMismatch(format!("serialize destination: {e}")))?
        }
        Some(bytes) => update_numeric_field(bytes, field, dest_balance + amount)
            .map_err(TransferError::TypeMismatch)?,
    };

    Ok(TransferComputation {
        new_source,
        new_dest,
        source_balance_after: source_balance - amount,
        dest_balance_after: dest_balance + amount,
    })
}

/// Extract a numeric field from a MessagePack-encoded KV value.
pub(in crate::data::executor) fn extract_numeric_field(value: &[u8], field: &str) -> Option<f64> {
    let doc: serde_json::Value = nodedb_types::json_from_msgpack(value).ok()?;
    let v = doc.get(field)?;
    v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
}

/// Update a numeric field in a MessagePack-encoded KV value, preserving other
/// fields.
pub(in crate::data::executor) fn update_numeric_field(
    value: &[u8],
    field: &str,
    new_value: f64,
) -> Result<Vec<u8>, String> {
    let mut doc: serde_json::Value =
        nodedb_types::json_from_msgpack(value).map_err(|e| format!("deserialize value: {e}"))?;
    if let Some(obj) = doc.as_object_mut() {
        if new_value.fract() == 0.0 && new_value >= i64::MIN as f64 && new_value <= i64::MAX as f64
        {
            obj.insert(field.to_string(), serde_json::json!(new_value as i64));
        } else {
            obj.insert(field.to_string(), serde_json::json!(new_value));
        }
    }
    nodedb_types::json_to_msgpack(&doc).map_err(|e| format!("serialize value: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(field: &str, value: f64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ field: value })).unwrap()
    }

    #[test]
    fn transfer_moves_balance_between_existing_docs() {
        let source = doc("balance", 100.0);
        let dest = doc("balance", 10.0);
        let result = compute_transfer(&source, Some(&dest), "balance", 30.0).unwrap();
        assert_eq!(result.source_balance_after, 70.0);
        assert_eq!(result.dest_balance_after, 40.0);
    }

    #[test]
    fn transfer_creates_dest_when_absent() {
        let source = doc("balance", 100.0);
        let result = compute_transfer(&source, None, "balance", 30.0).unwrap();
        assert_eq!(result.dest_balance_after, 30.0);
        assert_eq!(
            extract_numeric_field(&result.new_dest, "balance"),
            Some(30.0)
        );
    }

    #[test]
    fn transfer_rejects_insufficient_balance() {
        let source = doc("balance", 10.0);
        let err = compute_transfer(&source, None, "balance", 30.0);
        assert!(matches!(
            err,
            Err(TransferError::InsufficientBalance { .. })
        ));
    }

    #[test]
    fn transfer_rejects_non_numeric_field() {
        let source = nodedb_types::json_to_msgpack(&serde_json::json!({"balance": "abc"})).unwrap();
        let err = compute_transfer(&source, None, "balance", 30.0);
        assert!(matches!(err, Err(TransferError::TypeMismatch(_))));
    }
}
