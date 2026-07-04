// SPDX-License-Identifier: BUSL-1.1

// ── Re-export shared types from nodedb-types ──
pub use nodedb_types::id::{DatabaseId, DocumentId, RequestId, TenantId, TxnId, VShardId};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_display() {
        let t = TenantId::new(42);
        assert_eq!(t.to_string(), "tenant:42");
        assert_eq!(t.as_u64(), 42);
    }

    #[test]
    fn vshard_from_key_deterministic() {
        let a = VShardId::from_key(b"user:alice");
        let b = VShardId::from_key(b"user:alice");
        assert_eq!(a, b);
        assert!(a.as_u32() < VShardId::COUNT);
    }

    #[test]
    fn vshard_from_key_distributes() {
        let mut seen = std::collections::HashSet::new();
        for i in 0u32..1000 {
            let key = format!("tenant:{i}");
            seen.insert(VShardId::from_key(key.as_bytes()).as_u32());
        }
        assert!(
            seen.len() > 100,
            "poor distribution: only {} vShards hit",
            seen.len()
        );
    }

    #[test]
    fn request_id_roundtrip() {
        let r = RequestId::new(123456789);
        assert_eq!(r.as_u64(), 123456789);
        assert_eq!(r.to_string(), "req:123456789");
    }

    #[test]
    fn document_id_str() {
        let d = DocumentId::try_new("doc-abc-123").expect("test fixture");
        assert_eq!(d.as_str(), "doc-abc-123");
    }
}
