// SPDX-License-Identifier: Apache-2.0

//! Domain-bound CRDT frontier digests for preview/apply fencing.

use sha2::{Digest, Sha256};

use super::core::CrdtState;

const FRONTIER_DOMAIN: &[u8] = b"nodedb-crdt-frontier-v1\0";

/// Compute the domain-bound digest used to fence a previewed CRDT apply.
///
/// The empty-state case is represented without allocating or inserting a
/// collection. The raw state frontier is scoped by tenant, database, and exact
/// UTF-8 collection bytes so a digest cannot be replayed across domains.
pub fn domain_frontier_digest(
    tenant_id: u64,
    database_id: u64,
    collection: &str,
    state: Option<&CrdtState>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(FRONTIER_DOMAIN);
    hasher.update(tenant_id.to_be_bytes());
    hasher.update(database_id.to_be_bytes());
    hasher.update((collection.len() as u64).to_be_bytes());
    hasher.update(collection.as_bytes());
    hasher.update(
        state
            .map(CrdtState::frontier_digest)
            .unwrap_or_else(|| Sha256::digest(b"").into()),
    );
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_bound_to_every_domain_component() {
        let state = CrdtState::new(7).expect("state");
        let baseline = domain_frontier_digest(1, 2, "docs", Some(&state));
        assert_ne!(baseline, domain_frontier_digest(2, 2, "docs", Some(&state)));
        assert_ne!(baseline, domain_frontier_digest(1, 3, "docs", Some(&state)));
        assert_ne!(
            baseline,
            domain_frontier_digest(1, 2, "other", Some(&state))
        );
        assert_eq!(baseline, domain_frontier_digest(1, 2, "docs", None));
    }
}
